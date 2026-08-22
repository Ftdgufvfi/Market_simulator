#pragma once
// =============================================================================
//  qmm/core/latency_histogram.hpp
// -----------------------------------------------------------------------------
//  Recording and summarising latency measurements.
//
//  WHY PERCENTILES, NOT AVERAGES
//  -----------------------------
//  In trading, the *average* latency is almost useless. What kills you is the
//  tail: the p99, p99.9 ("three nines") and max. A system that is fast on
//  average but occasionally stalls for 500 us will miss trades exactly when the
//  market moves. So we always report p50 / p90 / p99 / p99.9 / max.
//
//  DESIGN
//  ------
//  The simplest *accurate* approach is to record every raw sample into a
//  pre-allocated vector (so we never allocate on the hot path), then sort once
//  at the end and index into it for percentiles. This is exact (no bucketing
//  error) and perfectly adequate for offline analysis of a benchmark run.
//
//  The only hot-path cost of record() is a bounds check + a store into
//  contiguous memory, which is negligible. We reserve capacity up front.
// =============================================================================
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <numeric>
#include <string>
#include <vector>

namespace qmm::core {

class LatencyHistogram {
public:
    // Reserve space for `expected_samples` up front so record() never allocates.
    explicit LatencyHistogram(std::size_t expected_samples = 1 << 20) {
        samples_.reserve(expected_samples);
    }

    // Record one latency sample (in nanoseconds). Hot-path safe: just a push.
    inline void record(std::uint64_t ns) {
        samples_.push_back(ns);
        sorted_ = false; // invalidate any previous sort
    }

    std::size_t count() const noexcept { return samples_.size(); }

    // Return the latency at percentile p (0..100). e.g. percentile(99.9).
    std::uint64_t percentile(double p) {
        if (samples_.empty()) return 0;
        ensure_sorted();
        // Nearest-rank method: index = ceil(p/100 * N) - 1, clamped.
        double rank = (p / 100.0) * static_cast<double>(samples_.size());
        std::size_t idx = static_cast<std::size_t>(rank);
        if (idx >= samples_.size()) idx = samples_.size() - 1;
        return samples_[idx];
    }

    std::uint64_t min()  { ensure_sorted(); return samples_.empty() ? 0 : samples_.front(); }
    std::uint64_t max()  { ensure_sorted(); return samples_.empty() ? 0 : samples_.back(); }

    double mean() const {
        if (samples_.empty()) return 0.0;
        // Sum in a wide accumulator to avoid overflow on large runs.
        const long double sum =
            std::accumulate(samples_.begin(), samples_.end(), (long double)0);
        return static_cast<double>(sum / samples_.size());
    }

    // Pretty-print a one-line-per-metric summary with a label.
    void print_summary(const std::string& label) {
        std::printf("  %-22s  n=%-9zu  min=%-7llu  p50=%-7llu  p90=%-7llu  "
                    "p99=%-7llu  p99.9=%-8llu  max=%-9llu  mean=%.1f  (ns)\n",
                    label.c_str(), count(),
                    (unsigned long long)min(),
                    (unsigned long long)percentile(50.0),
                    (unsigned long long)percentile(90.0),
                    (unsigned long long)percentile(99.0),
                    (unsigned long long)percentile(99.9),
                    (unsigned long long)max(),
                    mean());
    }

private:
    void ensure_sorted() {
        if (!sorted_) {
            std::sort(samples_.begin(), samples_.end());
            sorted_ = true;
        }
    }

    std::vector<std::uint64_t> samples_;
    bool sorted_ = false;
};

} // namespace qmm::core
