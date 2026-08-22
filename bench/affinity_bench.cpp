// =============================================================================
//  bench/affinity_bench.cpp
// -----------------------------------------------------------------------------
//  Three experiments that make CPU-microarchitecture effects VISIBLE and
//  measurable -- exactly the systems knowledge a low-latency trading desk cares
//  about:
//
//    EXPERIMENT 1 -- THREAD PINNING (affinity) on vs off.
//        Run the same SPSC ping-pong with the two threads PINNED to fixed cores,
//        then again UNPINNED (the OS may migrate them). Pinning keeps each
//        thread's working set hot in one core's cache and avoids migration
//        jitter, so it should show a lower and tighter latency tail.
//
//    EXPERIMENT 2 -- SMT SIBLINGS vs DISTINCT PHYSICAL CORES.
//        Place producer & consumer on two logical CPUs that are SMT siblings of
//        the SAME physical core, then on two DIFFERENT physical cores. Siblings
//        share L1/L2 and the core's execution units, so two busy hot-path
//        threads contend; distinct cores run truly in parallel.
//
//    EXPERIMENT 3 -- FALSE SHARING.
//        Two threads hammer two independent counters. First the counters sit on
//        the SAME cache line (adjacent in a struct); then we pad them onto
//        SEPARATE lines with CachePadded<>. Logically identical work, but the
//        shared-line version "ping-pongs" the line between cores and runs far
//        slower -- the classic false-sharing tax.
//
//  Each experiment prints numbers plus a one-line interpretation.
// =============================================================================
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <thread>

#include "qmm/core/affinity.hpp"
#include "qmm/core/cache.hpp"
#include "qmm/core/latency_histogram.hpp"
#include "qmm/core/spsc_ring.hpp"
#include "qmm/core/timing.hpp"

using namespace qmm::core;

namespace {

struct Msg { std::uint64_t seq = 0; std::uint64_t tsc = 0; };

constexpr int kPingPong = 200'000;
constexpr int kWarmup   = 20'000;

// -----------------------------------------------------------------------------
// Shared ping-pong latency helper. `pin` selects whether we bind the threads to
// (a_cpu, b_cpu) or leave them free for the OS to schedule (pass pin=false).
// Returns the populated histogram (moved out for the caller to summarise).
// -----------------------------------------------------------------------------
LatencyHistogram pingpong(unsigned a_cpu, unsigned b_cpu, bool pin,
                          const TscClock& clk) {
    SpscRing<Msg, 1024> a2b, b2a;
    LatencyHistogram hist(kPingPong);

    std::thread responder([&] {
        if (pin) pin_current_thread_to_cpu(b_cpu);
        for (int i = 0; i < kPingPong + kWarmup; ++i) {
            Msg m;
            while (!a2b.try_pop(m)) { /* spin */ }
            while (!b2a.try_push(m)) { /* spin */ }
        }
    });

    if (pin) pin_current_thread_to_cpu(a_cpu);
    for (int i = 0; i < kPingPong + kWarmup; ++i) {
        const std::uint64_t t0 = rdtsc();
        while (!a2b.try_push(Msg{ (std::uint64_t)i, t0 })) { /* spin */ }
        Msg echo;
        while (!b2a.try_pop(echo)) { /* spin */ }
        const std::uint64_t rt = rdtsc() - t0;
        if (i >= kWarmup)
            hist.record((std::uint64_t)(clk.cycles_to_ns(rt) / 2.0));
    }
    responder.join();
    return hist;
}

// -----------------------------------------------------------------------------
// Experiment 3 support: two counter layouts exercised by two threads.
// -----------------------------------------------------------------------------
constexpr long long kBumps = 50'000'000;   // increments per thread

// SHARED LINE: two atomics packed next to each other -> same 64B cache line.
struct SharedLine {
    std::atomic<long long> a{0};
    std::atomic<long long> b{0};
};

// PADDED: each atomic forced onto its own cache line -> no false sharing.
struct PaddedLine {
    CachePadded<std::atomic<long long>> a;
    CachePadded<std::atomic<long long>> b;
};

// Run two threads, each incrementing its own counter kBumps times. Returns the
// elapsed seconds. Takes the two atomics by reference so the SAME routine drives
// both the shared-line and the padded layouts (a fair, apples-to-apples test).
double hammer(std::atomic<long long>& x, std::atomic<long long>& y,
              unsigned cpu_x, unsigned cpu_y) {
    const std::uint64_t t0 = now_ns();
    std::thread tx([&] {
        pin_current_thread_to_cpu(cpu_x);
        for (long long i = 0; i < kBumps; ++i)
            x.fetch_add(1, std::memory_order_relaxed);
    });
    std::thread ty([&] {
        pin_current_thread_to_cpu(cpu_y);
        for (long long i = 0; i < kBumps; ++i)
            y.fetch_add(1, std::memory_order_relaxed);
    });
    tx.join(); ty.join();
    return (now_ns() - t0) / 1e9;
}

} // namespace

int main() {
    const Topology topo = discover_topology();
    const TscClock clk = TscClock::calibrate();

    std::printf("=== affinity_bench : pinning, SMT & false sharing ===\n");
    std::printf("physical_cores=%zu smt=%d numa=%u\n\n",
                topo.cores.size(), topo.smt_enabled() ? 1 : 0, topo.numa_nodes);

    // Two distinct physical cores (first logical CPU of core 0 and core 1).
    unsigned p0 = 0, p1 = 1;
    if (topo.cores.size() >= 2) {
        p0 = topo.cores[0].logical_cpus.front();
        p1 = topo.cores[1].logical_cpus.front();
    }

    // -------------------------------------------------------------------------
    // Experiment 1: pinned vs unpinned.
    // -------------------------------------------------------------------------
    std::printf("--- Experiment 1: thread pinning on vs off (SPSC ping-pong) ---\n");
    LatencyHistogram pinned   = pingpong(p0, p1, /*pin=*/true,  clk);
    LatencyHistogram unpinned = pingpong(p0, p1, /*pin=*/false, clk);
    pinned.print_summary("pinned (2 cores)");
    unpinned.print_summary("unpinned (OS free)");
    std::printf("  -> On an idle machine the gap is modest (few migrations occur), but under\n"
                "     load pinning keeps caches hot and avoids migration jitter, tightening\n"
                "     the tail where it matters most.\n\n");

    // -------------------------------------------------------------------------
    // Experiment 2: SMT siblings vs distinct cores (only if SMT is present).
    // -------------------------------------------------------------------------
    std::printf("--- Experiment 2: SMT siblings vs distinct physical cores ---\n");
    bool have_siblings = false;
    unsigned s0 = 0, s1 = 0;
    for (const auto& c : topo.cores) {
        if (c.logical_cpus.size() >= 2) {   // this physical core has two threads
            s0 = c.logical_cpus[0];
            s1 = c.logical_cpus[1];
            have_siblings = true;
            break;
        }
    }
    if (have_siblings) {
        LatencyHistogram siblings = pingpong(s0, s1, /*pin=*/true, clk);
        LatencyHistogram distinct = pingpong(p0, p1, /*pin=*/true, clk);
        std::printf("  (SMT siblings = logical CPUs %u & %u on ONE physical core)\n", s0, s1);
        siblings.print_summary("SMT siblings");
        distinct.print_summary("distinct cores");
        std::printf("  -> Subtle: for this PURE hand-off (no compute), SMT siblings can WIN\n"
                    "     because they share L1/L2, so the cache line never leaves the core.\n"
                    "     But two COMPUTE-heavy hot threads would contend for the shared\n"
                    "     execution units -- there, distinct physical cores win. Measure, don't\n"
                    "     assume: the right placement depends on whether you are latency- or\n"
                    "     throughput-bound.\n\n");
    } else {
        std::printf("  SMT not detected on this machine; skipping.\n\n");
    }

    // -------------------------------------------------------------------------
    // Experiment 3: false sharing.
    // -------------------------------------------------------------------------
    std::printf("--- Experiment 3: false sharing (2 threads, %lld bumps each) ---\n", kBumps);
    SharedLine shared;
    PaddedLine padded;
    const double t_shared = hammer(shared.a, shared.b, p0, p1);
    const double t_padded = hammer(padded.a.value, padded.b.value, p0, p1);
    std::printf("  same cache line (false sharing) : %.3f s\n", t_shared);
    std::printf("  padded to separate lines        : %.3f s\n", t_padded);
    if (t_padded > 0.0)
        std::printf("  -> Padding is %.2fx faster: the shared line ping-pongs between cores.\n",
                    t_shared / t_padded);
    return 0;
}
