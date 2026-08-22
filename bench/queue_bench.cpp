// =============================================================================
//  bench/queue_bench.cpp
// -----------------------------------------------------------------------------
//  Head-to-head micro-benchmark of THREE cross-thread hand-off mechanisms, on
//  identical single-producer / single-consumer workloads:
//
//     1. std::mutex + std::queue   -- the textbook "just lock it" approach.
//     2. Spinlock  + std::queue    -- busy-wait TTAS lock, never sleeps.
//     3. Lock-free SPSC ring       -- no mutual exclusion at all.
//
//  We measure TWO very different things, because they answer different
//  questions and require different harnesses:
//
//  (A) HAND-OFF LATENCY  -- measured with a PING-PONG.
//      Thread A sends one message and then waits for thread B to echo it back;
//      A records the round-trip time and we halve it for the one-way latency.
//      The ping-pong is essential: if we instead let a producer stream ahead of
//      the consumer, the "latency" we'd measure is just how long items sit in a
//      backlog, not the true cost of moving ONE message core-to-core. Because
//      only one message is ever in flight, this isolates the pure hand-off cost.
//
//  (B) THROUGHPUT  -- measured by STREAMING.
//      One producer pushes N items as fast as it can while one consumer drains
//      them; we report items/second. This is the saturated-pipe number.
//
//  In trading the TAIL latency (p99/p99.9) matters far more than the average, so
//  we always print the full percentile spread.
//
//  METHODOLOGY
//  -----------
//   * Both threads are PINNED to two DISTINCT physical cores, so results reflect
//     core-to-core interconnect cost rather than scheduler noise or SMT sharing.
//   * The TSC->ns ratio is calibrated once up front (see TscClock).
//   * A warm-up phase (not measured) pages memory in and warms caches.
// =============================================================================
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <queue>
#include <thread>

#include "qmm/core/affinity.hpp"
#include "qmm/core/latency_histogram.hpp"
#include "qmm/core/spinlock.hpp"
#include "qmm/core/spsc_ring.hpp"
#include "qmm/core/timing.hpp"

using namespace qmm::core;

namespace {

// One message: a sequence number plus a TSC stamp. 16 bytes, trivially copyable.
struct Msg {
    std::uint64_t seq = 0;
    std::uint64_t tsc = 0;
};

constexpr int kPingPong = 200'000;   // measured round-trips for the latency test
constexpr int kStream   = 2'000'000; // measured items for the throughput test
constexpr int kWarmup   = 20'000;    // discarded warm-up iterations

// Pick two DISTINCT physical cores so we measure true cross-core hand-off and
// never accidentally place both threads on two SMT siblings of one core.
struct CorePair { unsigned a_cpu; unsigned b_cpu; };

CorePair choose_cores(const Topology& topo) {
    if (topo.cores.size() >= 2 &&
        !topo.cores[0].logical_cpus.empty() &&
        !topo.cores[1].logical_cpus.empty()) {
        return { topo.cores[0].logical_cpus[0], topo.cores[1].logical_cpus[0] };
    }
    return { 0, 1 };   // fallback if topology discovery came up short
}

// -----------------------------------------------------------------------------
// A one-directional channel backed by a std::queue guarded by a lockable
// (std::mutex or our Spinlock). Blocking-free: callers spin on try_pop.
// Templated on the lock so mutex and spinlock share ONE code path (fair test).
// -----------------------------------------------------------------------------
template <typename Lock>
struct LockedChannel {
    std::queue<Msg> q;
    Lock            lock;

    void push(const Msg& m) {
        std::lock_guard<Lock> g(lock);
        q.push(m);
    }
    bool try_pop(Msg& out) {
        std::lock_guard<Lock> g(lock);
        if (q.empty()) return false;
        out = q.front();
        q.pop();
        return true;
    }
};

// ---- PING-PONG latency for a pair of LockedChannels -------------------------
template <typename Lock>
void pingpong_locked(const char* label, const CorePair& cores, const TscClock& clk) {
    LockedChannel<Lock> a2b;   // A -> B
    LockedChannel<Lock> b2a;   // B -> A (the echo)
    LatencyHistogram hist(kPingPong);

    // Responder: echo every request straight back.
    std::thread responder([&] {
        pin_current_thread_to_cpu(cores.b_cpu);
        for (int i = 0; i < kPingPong + kWarmup; ++i) {
            Msg m;
            while (!a2b.try_pop(m)) { /* spin */ }
            b2a.push(m);
        }
    });

    // Initiator: send, wait for the echo, record the round-trip.
    pin_current_thread_to_cpu(cores.a_cpu);
    for (int i = 0; i < kPingPong + kWarmup; ++i) {
        const std::uint64_t t0 = rdtsc();
        a2b.push(Msg{ (std::uint64_t)i, t0 });
        Msg echo;
        while (!b2a.try_pop(echo)) { /* spin */ }
        const std::uint64_t rt = rdtsc() - t0;
        if (i >= kWarmup)
            hist.record((std::uint64_t)(clk.cycles_to_ns(rt) / 2.0));  // one-way
    }
    responder.join();
    hist.print_summary(label);
}

// ---- PING-PONG latency for a pair of lock-free SPSC rings -------------------
void pingpong_spsc(const char* label, const CorePair& cores, const TscClock& clk) {
    SpscRing<Msg, 1024> a2b;
    SpscRing<Msg, 1024> b2a;
    LatencyHistogram hist(kPingPong);

    std::thread responder([&] {
        pin_current_thread_to_cpu(cores.b_cpu);
        for (int i = 0; i < kPingPong + kWarmup; ++i) {
            Msg m;
            while (!a2b.try_pop(m)) { /* spin */ }
            while (!b2a.try_push(m)) { /* spin */ }
        }
    });

    pin_current_thread_to_cpu(cores.a_cpu);
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
    hist.print_summary(label);
}

// -----------------------------------------------------------------------------
// STREAMING throughput: producer floods, consumer drains, report items/second.
// -----------------------------------------------------------------------------
template <typename Lock>
double stream_locked(const CorePair& cores) {
    LockedChannel<Lock> ch;
    std::thread consumer([&] {
        pin_current_thread_to_cpu(cores.b_cpu);
        int got = 0;
        while (got < kStream) { Msg m; if (ch.try_pop(m)) ++got; }
    });
    const std::uint64_t t0 = now_ns();
    pin_current_thread_to_cpu(cores.a_cpu);
    for (int i = 0; i < kStream; ++i) ch.push(Msg{ (std::uint64_t)i, 0 });
    consumer.join();
    return kStream / ((now_ns() - t0) / 1e9) / 1e6;   // M items/s
}

double stream_spsc(const CorePair& cores) {
    SpscRing<Msg, 1024> ring;
    std::thread consumer([&] {
        pin_current_thread_to_cpu(cores.b_cpu);
        int got = 0;
        while (got < kStream) { Msg m; if (ring.try_pop(m)) ++got; }
    });
    const std::uint64_t t0 = now_ns();
    pin_current_thread_to_cpu(cores.a_cpu);
    for (int i = 0; i < kStream; ++i)
        while (!ring.try_push(Msg{ (std::uint64_t)i, 0 })) { /* spin */ }
    consumer.join();
    return kStream / ((now_ns() - t0) / 1e9) / 1e6;
}

} // namespace

int main() {
    const Topology topo = discover_topology();
    const CorePair cores = choose_cores(topo);
    const TscClock clk = TscClock::calibrate();

    std::printf("=== queue_bench : mutex vs spinlock vs lock-free SPSC ===\n");
    std::printf("physical_cores=%zu smt=%d | thread_A_cpu=%u thread_B_cpu=%u\n",
                topo.cores.size(), topo.smt_enabled() ? 1 : 0,
                cores.a_cpu, cores.b_cpu);
    std::printf("tsc calibrated at %.3f cycles/ns\n\n", clk.cycles_per_ns());

    // (A) One-way hand-off latency (ping-pong, only one message in flight).
    std::printf("--- one-way hand-off latency (ping-pong, %d samples) ---\n", kPingPong);
    pingpong_locked<std::mutex>("std::mutex + queue", cores, clk);
    pingpong_locked<Spinlock>  ("spinlock + queue",   cores, clk);
    pingpong_spsc             ("lock-free SPSC ring", cores, clk);

    // (B) Saturated throughput (streaming producer -> consumer).
    std::printf("\n--- streaming throughput (%d items) ---\n", kStream);
    std::printf("  %-22s  %.2f M items/s\n", "std::mutex + queue", stream_locked<std::mutex>(cores));
    std::printf("  %-22s  %.2f M items/s\n", "spinlock + queue",   stream_locked<Spinlock>(cores));
    std::printf("  %-22s  %.2f M items/s\n", "lock-free SPSC ring", stream_spsc(cores));

    std::printf("\nTakeaway: the lock-free ring should show BOTH the lowest p50\n"
                "and (crucially) the tightest tail, because neither thread ever\n"
                "blocks or enters the kernel. The std::mutex variant pays a\n"
                "syscall whenever it must sleep/wake a thread, which inflates its\n"
                "p99/p99.9 tail even when its average looks acceptable.\n");
    return 0;
}
