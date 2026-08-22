#pragma once
// =============================================================================
//  qmm/core/mpmc_ring.hpp
// -----------------------------------------------------------------------------
//  A bounded, lock-free Multi-Producer / Multi-Consumer (MPMC) queue.
//
//  This is Dmitry Vyukov's well-known bounded MPMC queue. Unlike the SPSC ring
//  (which assumes exactly one producer and one consumer), this queue lets MANY
//  threads push and MANY threads pop concurrently, still without any locks.
//
//  THE KEY IDEA: A PER-SLOT SEQUENCE NUMBER
//  ----------------------------------------
//  Each slot carries its own atomic `sequence` counter that acts like a tiny
//  traffic light telling threads whether the slot is ready to be written or
//  read, and by which "turn":
//
//     * A producer may WRITE slot i when   sequence == ticket
//       (the slot is empty and waiting for this exact enqueue turn).
//     * A consumer may READ slot i when    sequence == ticket + 1
//       (the slot has been filled and is waiting to be dequeued).
//
//  Producers hand out enqueue tickets by atomically incrementing `enqueue_pos_`;
//  consumers hand out dequeue tickets via `dequeue_pos_`. Each thread then does
//  a compare-and-swap (CAS) race to claim its ticket; losers simply retry with
//  the next ticket. After acting on a slot, a thread advances that slot's
//  sequence so the *other* role can proceed. No thread ever blocks.
//
//  Because it needs a CAS per operation and touches per-slot atomics, MPMC is
//  necessarily a bit heavier than the SPSC ring -- which is exactly why we use
//  SPSC on the hottest 1-to-1 hops and reserve MPMC for genuine many-to-many
//  fan-in/fan-out.
// =============================================================================
#include <atomic>
#include <cstddef>
#include <vector>

#include "qmm/core/cache.hpp"

namespace qmm::core {

template <typename T>
class MpmcRing {
public:
    // Capacity must be a power of two (for cheap bit-mask wrapping).
    explicit MpmcRing(std::size_t capacity)
        : buffer_(capacity), mask_(capacity - 1) {
        // Validate power-of-two at runtime (constructor, not hot path).
        // (capacity & mask) == 0 iff capacity is a power of two.
        // Initialise each slot's sequence to its index: slot i is ready for the
        // producer whose enqueue ticket == i.
        for (std::size_t i = 0; i < capacity; ++i)
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        enqueue_pos_.value.store(0, std::memory_order_relaxed);
        dequeue_pos_.value.store(0, std::memory_order_relaxed);
    }

    // ---- Producer side (any number of threads) ------------------------------
    bool try_push(const T& value) {
        Cell* cell;
        // Read our prospective enqueue ticket without committing yet.
        std::size_t pos = enqueue_pos_.value.load(std::memory_order_relaxed);
        for (;;) {
            cell = &buffer_[pos & mask_];
            const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            // Difference tells us the slot's state relative to our ticket.
            const std::intptr_t diff =
                static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);

            if (diff == 0) {
                // Slot is empty and it's exactly our turn: try to claim the
                // ticket by bumping enqueue_pos_. If we win the CAS, the slot is
                // ours to write.
                if (enqueue_pos_.value.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed))
                    break; // claimed; `pos` is our slot ticket
                // Lost the race: `pos` was updated to the new value; retry.
            } else if (diff < 0) {
                // Slot still holds un-consumed data (producer lapped consumers):
                // the queue is full.
                return false;
            } else {
                // Another producer already advanced past us; reload and retry.
                pos = enqueue_pos_.value.load(std::memory_order_relaxed);
            }
        }

        cell->value = value;
        // Publish: mark the slot readable by a consumer whose dequeue ticket is
        // `pos` (it waits for sequence == pos + 1). release pairs with the
        // consumer's acquire load below.
        cell->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    // ---- Consumer side (any number of threads) ------------------------------
    bool try_pop(T& out) {
        Cell* cell;
        std::size_t pos = dequeue_pos_.value.load(std::memory_order_relaxed);
        for (;;) {
            cell = &buffer_[pos & mask_];
            const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            // A filled slot has sequence == ticket + 1.
            const std::intptr_t diff =
                static_cast<std::intptr_t>(seq) -
                static_cast<std::intptr_t>(pos + 1);

            if (diff == 0) {
                if (dequeue_pos_.value.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed))
                    break; // claimed this slot for reading
            } else if (diff < 0) {
                // Slot not yet filled: the queue is empty.
                return false;
            } else {
                pos = dequeue_pos_.value.load(std::memory_order_relaxed);
            }
        }

        out = cell->value;
        // Recycle the slot: set its sequence forward by one lap so the next
        // producer whose ticket == pos + capacity can reuse it.
        cell->sequence.store(pos + mask_ + 1, std::memory_order_release);
        return true;
    }

private:
    // One ring slot: the payload plus its atomic traffic-light sequence.
    struct Cell {
        std::atomic<std::size_t> sequence;
        T value;
    };

    std::vector<Cell> buffer_;
    std::size_t mask_;

    // enqueue_pos_ and dequeue_pos_ are hammered by different sets of threads,
    // so keep them on separate cache lines to avoid false sharing.
    CachePadded<std::atomic<std::size_t>> enqueue_pos_{};
    CachePadded<std::atomic<std::size_t>> dequeue_pos_{};
};

} // namespace qmm::core
