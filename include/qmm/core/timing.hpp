#pragma once
// =============================================================================
//  qmm/core/timing.hpp
// -----------------------------------------------------------------------------
//  High-resolution timing utilities for latency measurement.
//
//  We expose TWO clocks, because they answer two different questions:
//
//   1. QueryPerformanceCounter (QPC)  -> wall-clock nanoseconds.
//        * Monotonic, reliable, converts cleanly to real time units.
//        * Best for reporting human-meaningful latencies (p50/p99 in ns).
//
//   2. __rdtsc / __rdtscp             -> raw CPU cycle counter (TSC).
//        * Lowest possible measurement overhead (a single instruction).
//        * Great for measuring very short hot-path sections where even QPC's
//          call overhead would pollute the result.
//        * We calibrate it against QPC once at startup to convert cycles->ns.
//
//  __rdtscp additionally serialises (it waits for prior instructions to retire)
//  which makes it more accurate for tight measurements than plain __rdtsc.
// =============================================================================
#include <cstdint>

#if defined(_WIN32)
  #define WIN32_LEAN_AND_MEAN
  // NOMINMAX stops <windows.h> from defining min()/max() as macros, which would
  // otherwise clobber std::min / std::max and the LatencyHistogram::min()/max()
  // members, producing a cascade of bizarre compiler errors.
  #ifndef NOMINMAX
    #define NOMINMAX
  #endif
  #include <windows.h>   // QueryPerformanceCounter / QueryPerformanceFrequency
  #include <intrin.h>    // __rdtsc / __rdtscp
#endif

namespace qmm::core {

// ---- Raw TSC reads ----------------------------------------------------------
// Read the CPU timestamp counter. NOT serialising: cheap but the CPU may reorder
// surrounding instructions across it. Use for coarse timing.
inline std::uint64_t rdtsc() noexcept {
    return __rdtsc();
}

// Serialising TSC read: guarantees all prior instructions have retired before
// the counter is sampled. Slightly more expensive, but accurate for tiny spans.
inline std::uint64_t rdtscp() noexcept {
    unsigned int aux;          // receives the CPU/socket id; we don't use it.
    return __rdtscp(&aux);
}

// ---- Wall-clock nanoseconds via QPC ----------------------------------------
// Returns a monotonically increasing timestamp in nanoseconds.
inline std::uint64_t now_ns() noexcept {
    LARGE_INTEGER freq, counter;
    QueryPerformanceFrequency(&freq);  // ticks per second (constant per boot)
    QueryPerformanceCounter(&counter); // current tick count
    // Convert ticks -> ns as (counter * 1e9) / freq, done in 128-bit-safe order
    // to avoid overflow: multiply the fractional part carefully.
    const long double ns = (static_cast<long double>(counter.QuadPart) * 1e9L)
                           / static_cast<long double>(freq.QuadPart);
    return static_cast<std::uint64_t>(ns);
}

// -----------------------------------------------------------------------------
// TscClock : calibrated TSC -> nanoseconds conversion.
// -----------------------------------------------------------------------------
// The TSC ticks at a fixed rate on modern CPUs ("invariant TSC"), but that rate
// is not the CPU's clock speed and is not documented at runtime. So we measure
// it once: sample TSC and QPC, busy-wait a short interval, sample again, and
// compute how many TSC cycles elapsed per nanosecond.
class TscClock {
public:
    // Calibrate over ~10 ms. Call once at program start, ideally on a pinned
    // thread so the measurement isn't disturbed by migration.
    static TscClock calibrate() {
        const std::uint64_t qpc_start = now_ns();
        const std::uint64_t tsc_start = rdtscp();
        // Busy-wait ~10 ms of wall-clock time.
        while (now_ns() - qpc_start < 10'000'000ULL) { /* spin */ }
        const std::uint64_t tsc_end = rdtscp();
        const std::uint64_t qpc_end = now_ns();

        const double elapsed_ns  = static_cast<double>(qpc_end - qpc_start);
        const double elapsed_tsc = static_cast<double>(tsc_end - tsc_start);
        TscClock c;
        c.cycles_per_ns_ = elapsed_tsc / elapsed_ns;
        return c;
    }

    // Convert a TSC-cycle delta into nanoseconds using the calibrated ratio.
    double cycles_to_ns(std::uint64_t cycles) const noexcept {
        return static_cast<double>(cycles) / cycles_per_ns_;
    }

    double cycles_per_ns() const noexcept { return cycles_per_ns_; }

private:
    double cycles_per_ns_ = 1.0;
};

} // namespace qmm::core
