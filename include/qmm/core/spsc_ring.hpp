#pragma once
// =============================================================================
//  qmm/core/spsc_ring.hpp
// -----------------------------------------------------------------------------
//  A lock-free Single-Producer / Single-Consumer (SPSC) ring buffer.
//
//  This is the single most important data structure in the whole simulator: it
//  is how one pipeline stage hands work to the next WITHOUT ever taking a lock.
//  Locks are poison for low latency because a blocked thread gets de-scheduled
//  by the OS, and waking it back up costs microseconds of unpredictable jitter.
//  Here, neither thread ever blocks: they coordinate purely through two atomic
//  indices and the C++ memory model.
//
//  CONTRACT
//  --------
//    * Exactly ONE thread may call try_push (the "producer").
//    * Exactly ONE (other) thread may call try_pop (the "consumer").
//    * Capacity must be a power of two (so we can use a cheap bit-mask instead
//      of a modulo to wrap indices).
//    * One slot is always left empty to distinguish "full" from "empty", so a
//      ring of Capacity holds up to Capacity-1 elements.
//
//  HOW THE ATOMICS WORK (memory ordering)
//  --------------------------------------
//    head_ : index of the next slot the CONSUMER will read.  (consumer writes it)
//    tail_ : index of the next slot the PRODUCER will write. (producer writes it)
//
//    Producer publishing an item:
//        buf_[t] = value;                         // (1) write the data
//        tail_.store(next, memory_order_release); // (2) publish the new tail
//    The RELEASE store guarantees that write (1) is visible to any thread that
//    later does an ACQUIRE load of tail_ and sees this new value. So when the
//    consumer's ACQUIRE load of tail_ observes `next`, it is also guaranteed to
//    see the data written in (1). This release/acquire pair is what makes the
//    hand-off correct without a lock.
//
//  THE CACHING TRICK (avoiding cross-core cache-line traffic)
//  ----------------------------------------------------------
//    Every time the producer reads head_ (written by the consumer's core), it
//    pulls that cache line across the interconnect -- expensive. But the
//    producer only needs head_ to check "is the ring full?". Most of the time it
//    is NOT full, so we keep a private, non-atomic CACHE of the last head_ value
//    the producer saw (cached_head_). We only re-load the real atomic head_ when
//    our cache says we're full. Symmetrically the consumer caches tail_. This
//    slashes cross-core traffic and is a standard HFT ring-buffer optimisation.
//
//  FALSE-SHARING AVOIDANCE
//  -----------------------
//    head_ and tail_ are written by different threads, so we place them on
//    separate cache lines (alignas 64). If they shared a line, the producer's
//    store to tail_ would invalidate the consumer's cached copy of the whole
//    line (including head_) and vice-versa -- the classic false-sharing stall.
// =============================================================================
#include <array>
#include <atomic>
#include <cstddef>
#include <type_traits>

#include "qmm/core/cache.hpp"

namespace qmm::core {

template <typename T, std::size_t Capacity>
class SpscRing {
    // Capacity must be a power of two so `index & kMask` == `index % Capacity`.
    static_assert(Capacity >= 2, "Capacity must be at least 2");
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two");
    static_assert(std::is_nothrow_copy_assignable_v<T> ||
                  std::is_nothrow_move_assignable_v<T>,
                  "T should be cheaply/nothrow assignable for a hot-path queue");

    static constexpr std::size_t kMask = Capacity - 1;

public:
    SpscRing() = default;
    SpscRing(const SpscRing&) = delete;            // not copyable: it owns state
    SpscRing& operator=(const SpscRing&) = delete;

    // ---- Producer side ------------------------------------------------------
    // Try to enqueue `value`. Returns false (without blocking) if the ring is
    // full. Only the single producer thread may call this.
    bool try_push(const T& value) noexcept {
        const std::size_t t    = tail_.value.load(std::memory_order_relaxed);
        const std::size_t next = (t + 1) & kMask;

        // Fast full-check against our private cache of head_ (no atomic load).
        if (next == cached_head_) {
            // Cache says full -- refresh from the real atomic head_ (acquire so
            // we correctly observe the consumer's progress) and re-check.
            cached_head_ = head_.value.load(std::memory_order_acquire);
            if (next == cached_head_)
                return false;                      // genuinely full
        }

        buf_[t] = value;                           // (1) write payload
        tail_.value.store(next, std::memory_order_release); // (2) publish
        return true;
    }

    // ---- Consumer side ------------------------------------------------------
    // Try to dequeue into `out`. Returns false (without blocking) if the ring is
    // empty. Only the single consumer thread may call this.
    bool try_pop(T& out) noexcept {
        const std::size_t h = head_.value.load(std::memory_order_relaxed);

        // Fast empty-check against our private cache of tail_ (no atomic load).
        if (h == cached_tail_) {
            // Cache says empty -- refresh from the real atomic tail_ (acquire so
            // we also observe the payload the producer wrote before publishing).
            cached_tail_ = tail_.value.load(std::memory_order_acquire);
            if (h == cached_tail_)
                return false;                      // genuinely empty
        }

        out = buf_[h];                             // read payload
        head_.value.store((h + 1) & kMask, std::memory_order_release);
        return true;
    }

    // Approximate number of elements currently queued. For diagnostics only:
    // both indices may move under our feet, so treat this as a snapshot hint.
    std::size_t size_approx() const noexcept {
        const std::size_t t = tail_.value.load(std::memory_order_acquire);
        const std::size_t h = head_.value.load(std::memory_order_acquire);
        return (t - h) & kMask;
    }

    static constexpr std::size_t capacity() noexcept { return Capacity - 1; }

private:
    // --- Producer's line ---
    // tail_ is written by the producer; cached_head_ is the producer's private
    // (non-atomic) snapshot of head_. Both are touched only by the producer, so
    // they may happily share a line with each other -- but NOT with head_.
    CachePadded<std::atomic<std::size_t>> tail_{};
    std::size_t cached_head_ = 0;

    // --- Consumer's line ---
    // head_ is written by the consumer; cached_tail_ is the consumer's snapshot
    // of tail_. Separated from the producer's data above by cache-line padding.
    CachePadded<std::atomic<std::size_t>> head_{};
    std::size_t cached_tail_ = 0;

    // --- The storage itself ---
    // A fixed, pre-allocated array: zero runtime allocation, contiguous memory,
    // cache-friendly. alignas keeps it off the index cache lines.
    alignas(kCacheLineSize) std::array<T, Capacity> buf_{};
};

} // namespace qmm::core
