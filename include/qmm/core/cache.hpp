#pragma once
// =============================================================================
//  qmm/core/cache.hpp
// -----------------------------------------------------------------------------
//  Cache-line awareness utilities.
//
//  WHY THIS MATTERS
//  ----------------
//  Modern CPUs move memory around in fixed-size chunks called *cache lines*
//  (64 bytes on x86-64). The cache-coherency protocol (MESI) works at cache-line
//  granularity, NOT at variable granularity. That has a nasty consequence called
//  "FALSE SHARING":
//
//      Thread A writes variable X.
//      Thread B writes variable Y.
//      X and Y are *different* variables, so logically there is no conflict...
//      ...but if X and Y happen to sit on the SAME 64-byte cache line, every
//      write by A invalidates B's copy of the line and vice-versa. The line
//      "ping-pongs" between the two cores' caches, costing ~100+ cycles each
//      time. Your lock-free code becomes slower than a mutex.
//
//  The fix is to force hot, independently-written variables onto their own cache
//  lines using alignment/padding. This header provides the constants and helpers
//  to do that clearly.
// =============================================================================
#include <cstddef>

namespace qmm::core {

// The size of a cache line in bytes on x86-64 (Intel & AMD). We hard-code 64
// rather than using std::hardware_destructive_interference_size because MSVC
// emits a warning about that constant's ABI stability, and 64 is correct for
// every x86-64 CPU we target (including this AMD EPYC).
inline constexpr std::size_t kCacheLineSize = 64;

// -----------------------------------------------------------------------------
// CachePadded<T>
// -----------------------------------------------------------------------------
// Wraps a value T and forces it to occupy its own cache line(s), so that it can
// never false-share with whatever variable is declared next to it.
//
//   alignas(kCacheLineSize) does two things for us:
//     1. Aligns the object's start to a 64-byte boundary.
//     2. Rounds the object's SIZE up to a multiple of 64. That means in an
//        array of CachePadded<T>, consecutive elements never share a line.
//
// Usage:
//     CachePadded<std::atomic<size_t>> head;   // head lives alone on its line
//     CachePadded<std::atomic<size_t>> tail;   // tail lives alone on its line
//     head.value.store(...);                   // access via .value
template <typename T>
struct alignas(kCacheLineSize) CachePadded {
    T value{};

    // Convenience accessors so call sites can treat it almost like a T.
    T&       operator*()        noexcept { return value; }
    const T& operator*()  const noexcept { return value; }
    T*       operator->()       noexcept { return &value; }
    const T* operator->() const noexcept { return &value; }
};

} // namespace qmm::core
