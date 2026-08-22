#pragma once
// =============================================================================
//  qmm/core/affinity.hpp
// -----------------------------------------------------------------------------
//  Windows CPU affinity, topology discovery, and NUMA-aware allocation.
//
//  WHY WE PIN THREADS
//  ------------------
//  By default the OS scheduler is free to move a thread from core to core.
//  Every migration is a latency disaster for us:
//    * The thread's data is hot in the OLD core's L1/L2 cache; after migration
//      it must be re-fetched into the new core's cache (cold-cache stalls).
//    * Migration itself is a context switch (~microseconds of jitter).
//  For a latency-critical pipeline we instead PIN each stage to a fixed physical
//  core so its working set stays hot and it is never migrated.
//
//  PHYSICAL vs LOGICAL CORES (SMT / hyper-threading)
//  -------------------------------------------------
//  With SMT, one physical core exposes TWO logical CPUs that share the core's
//  execution units, L1 and L2. Two busy latency-critical threads on the SAME
//  physical core will fight over those units. So for the hot path we prefer to
//  pin each stage to a *distinct physical core*. This header discovers which
//  logical CPUs are SMT siblings so callers can make that choice deliberately
//  (and so the benchmark can measure the SMT penalty on purpose).
//
//  NUMA
//  ----
//  On multi-socket / multi-node machines, RAM attached to the local node is
//  faster to access than RAM on a remote node. We expose VirtualAllocExNuma so a
//  consumer thread can allocate its buffers on its own NUMA node ("first-touch"
//  done explicitly), avoiding slow remote memory traffic on the hot path.
// =============================================================================
#include <cstddef>
#include <cstdint>
#include <vector>

#if defined(_WIN32)
  #define WIN32_LEAN_AND_MEAN
  #ifndef NOMINMAX
    #define NOMINMAX
  #endif
  #include <windows.h>
#endif

namespace qmm::core {

// Number of logical processors visible to this process.
inline unsigned hardware_threads() {
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return static_cast<unsigned>(si.dwNumberOfProcessors);
}

// -----------------------------------------------------------------------------
// pin_current_thread_to_cpu
// -----------------------------------------------------------------------------
// Pin the CALLING thread to a single logical CPU index (0-based).
//
// NOTE on processor groups: Windows groups logical CPUs into "processor groups"
// of at most 64. SetThreadAffinityMask operates within the thread's current
// group. Our target machine has 16 logical CPUs (one group), so a simple mask is
// correct here. For >64 CPUs one would use SetThreadGroupAffinity instead.
inline bool pin_current_thread_to_cpu(unsigned cpu) {
    const DWORD_PTR mask = static_cast<DWORD_PTR>(1) << cpu;
    // Returns the previous affinity mask on success, 0 on failure.
    return SetThreadAffinityMask(GetCurrentThread(), mask) != 0;
}

// Give the calling thread the highest scheduling priority. Combined with pinning
// this minimises the chance of being pre-empted on its dedicated core.
inline void boost_current_thread_priority() {
    SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
}

// -----------------------------------------------------------------------------
// Topology discovery
// -----------------------------------------------------------------------------
// Describes one physical core and the logical-CPU indices that live on it.
// If SMT is on, `logical_cpus` will contain two entries (the SMT siblings).
struct PhysicalCore {
    std::vector<unsigned> logical_cpus; // logical CPU indices on this core
};

struct Topology {
    std::vector<PhysicalCore> cores;    // one entry per physical core
    unsigned numa_nodes = 1;            // number of NUMA nodes

    bool smt_enabled() const {
        for (const auto& c : cores)
            if (c.logical_cpus.size() > 1) return true;
        return false;
    }
};

// Enumerate physical cores and their SMT siblings via GetLogicalProcessor
// InformationEx(RelationProcessorCore). Each RelationProcessorCore record has a
// bitmask of the logical CPUs belonging to that one physical core.
inline Topology discover_topology() {
    Topology topo;

    // First call with a null buffer to learn the required size.
    DWORD len = 0;
    GetLogicalProcessorInformationEx(RelationProcessorCore, nullptr, &len);
    std::vector<std::uint8_t> buffer(len);
    if (!GetLogicalProcessorInformationEx(
            RelationProcessorCore,
            reinterpret_cast<PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>(buffer.data()),
            &len)) {
        return topo; // best-effort; return whatever we have
    }

    // Walk the variable-length records packed into the buffer.
    std::uint8_t* ptr = buffer.data();
    std::uint8_t* end = buffer.data() + len;
    while (ptr < end) {
        auto* info = reinterpret_cast<PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>(ptr);
        if (info->Relationship == RelationProcessorCore) {
            PhysicalCore core;
            // A processor core normally has exactly one GROUP_AFFINITY group.
            const GROUP_AFFINITY& ga = info->Processor.GroupMask[0];
            for (unsigned bit = 0; bit < sizeof(KAFFINITY) * 8; ++bit) {
                if (ga.Mask & (static_cast<KAFFINITY>(1) << bit))
                    core.logical_cpus.push_back(bit);
            }
            topo.cores.push_back(std::move(core));
        }
        ptr += info->Size; // records are variable-length; advance by Size
    }

    // NUMA node count.
    ULONG highest = 0;
    if (GetNumaHighestNodeNumber(&highest))
        topo.numa_nodes = static_cast<unsigned>(highest) + 1;

    return topo;
}

// -----------------------------------------------------------------------------
// NUMA-aware allocation
// -----------------------------------------------------------------------------
// Reserve+commit `bytes` of memory bound to NUMA `node`. On single-node systems
// this behaves like a normal VirtualAlloc. Free with numa_free().
inline void* numa_alloc_onnode(std::size_t bytes, unsigned node) {
    return VirtualAllocExNuma(GetCurrentProcess(), nullptr, bytes,
                              MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE,
                              static_cast<DWORD>(node));
}

inline void numa_free(void* p, std::size_t /*bytes*/) {
    if (p) VirtualFree(p, 0, MEM_RELEASE);
}

} // namespace qmm::core
