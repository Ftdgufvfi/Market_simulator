#pragma once
// =============================================================================
//  qmm/core/spinlock.hpp
// -----------------------------------------------------------------------------
//  A tiny test-and-test-and-set (TTAS) spinlock.
//
//  This exists mainly so our benchmark can compare THREE styles of cross-thread
//  coordination on the SAME workload:
//     1. std::mutex        -> may block/sleep (scheduler involved)  ~1-3 us
//     2. Spinlock (atomic) -> busy-waits, never sleeps              ~100-300 ns
//     3. lock-free ring    -> no mutual exclusion at all            ~20-80 ns
//
//  WHY "TEST-AND-TEST-AND-SET"?
//  ---------------------------
//  A naive spinlock hammers exchange() in a loop. Every exchange is a WRITE, so
//  every spinning core keeps invalidating the lock's cache line on every other
//  core -- a storm of coherency traffic. TTAS instead spins on a cheap READ
//  (load) and only attempts the expensive atomic exchange once the lock LOOKS
//  free. Spinning readers can then share the line, drastically cutting traffic.
//
//  We also emit a CPU "pause" hint (_mm_pause) while spinning: it reduces power
//  use and, on SMT cores, yields pipeline resources to the sibling thread that
//  may actually be holding the lock.
// =============================================================================
#include <atomic>

#if defined(_MSC_VER)
  #include <intrin.h>   // _mm_pause
#endif

namespace qmm::core {

class Spinlock {
public:
    void lock() noexcept {
        for (;;) {
            // 1) Fast path: try to grab the lock with a single atomic exchange.
            //    acquire ordering: nothing after lock() may be reordered before it.
            if (!flag_.exchange(true, std::memory_order_acquire))
                return; // we got it (previous value was false == unlocked)

            // 2) Contended: spin on a cheap RELAXED load until the lock looks
            //    free again, THEN retry the exchange. This is the "test-and-
            //    test-and-set" part -- readers don't fight over the cache line.
            while (flag_.load(std::memory_order_relaxed)) {
                cpu_pause();
            }
        }
    }

    // Non-blocking attempt; returns true if the lock was acquired.
    bool try_lock() noexcept {
        return !flag_.exchange(true, std::memory_order_acquire);
    }

    void unlock() noexcept {
        // release ordering: everything the critical section wrote becomes visible
        // to the next thread that acquires the lock.
        flag_.store(false, std::memory_order_release);
    }

private:
    static void cpu_pause() noexcept {
#if defined(_MSC_VER)
        _mm_pause();
#endif
    }

    // Kept on its own cache line so a contended lock doesn't false-share with
    // whatever the owning object stores next to it.
    alignas(64) std::atomic<bool> flag_{false};
};

// RAII guard so callers can write:  { std::lock_guard<Spinlock> g(sl); ... }
// (std::lock_guard works with any type exposing lock()/unlock().)

} // namespace qmm::core
