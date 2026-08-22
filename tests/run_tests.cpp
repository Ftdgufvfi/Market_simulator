// =============================================================================
//  tests/run_tests.cpp
// -----------------------------------------------------------------------------
//  A tiny, dependency-free unit-test harness for the simulator's core modules.
//
//  WHY A HAND-ROLLED HARNESS INSTEAD OF GTEST/CATCH2?
//  --------------------------------------------------
//  The whole project is intentionally build-from-source with zero external
//  dependencies (just MSVC + the STL), so pulling in a test framework would work
//  against that story. A ~30-line CHECK macro gives us everything we need:
//    * clear pass/fail reporting with file:line and the failing expression,
//    * a non-zero process exit code on any failure (so `ctest` / CI goes red).
//
//  Each test focuses on a single invariant that would be easy to break during a
//  refactor: ring correctness, order-book matching & accounting, the post-only
//  cross check, risk limits/kill-switch, and P&L bookkeeping.
// =============================================================================
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "qmm/core/spsc_ring.hpp"
#include "qmm/core/mpmc_ring.hpp"
#include "qmm/core/spinlock.hpp"
#include "qmm/core/latency_histogram.hpp"
#include "qmm/book/order_book.hpp"
#include "qmm/risk/risk_engine.hpp"
#include "qmm/analytics/pnl.hpp"

// -----------------------------------------------------------------------------
// Minimal assertion plumbing.
// -----------------------------------------------------------------------------
namespace {
int g_failures = 0;   // process-wide failure counter -> becomes the exit code
int g_checks   = 0;

// Record one check. On failure, print where and what went wrong but KEEP GOING
// so a single run surfaces every broken invariant, not just the first.
void report(bool ok, const char* expr, const char* file, int line) {
    ++g_checks;
    if (!ok) {
        ++g_failures;
        std::printf("  [FAIL] %s:%d   CHECK(%s)\n", file, line, expr);
    }
}
} // namespace

// CHECK(cond): assert a boolean. CHECK_EQ(a,b): assert equality (a,b printed on
// failure would need streams; we keep it simple and just show the expression).
#define CHECK(cond)      report((cond), #cond, __FILE__, __LINE__)
#define CHECK_EQ(a, b)   report((a) == (b), #a " == " #b, __FILE__, __LINE__)

// =============================================================================
//  Core: SPSC ring buffer
// =============================================================================
static void test_spsc_basic() {
    qmm::core::SpscRing<int, 4> ring;   // holds up to 3 (one slot kept empty)

    int out = -1;
    CHECK(!ring.try_pop(out));          // empty ring pops nothing

    CHECK(ring.try_push(10));
    CHECK(ring.try_push(20));
    CHECK(ring.try_push(30));
    CHECK(!ring.try_push(40));          // now full (capacity() == 3)

    CHECK(ring.try_pop(out)); CHECK_EQ(out, 10);   // FIFO order preserved
    CHECK(ring.try_pop(out)); CHECK_EQ(out, 20);
    CHECK(ring.try_push(40));           // space again after popping
    CHECK(ring.try_pop(out)); CHECK_EQ(out, 30);
    CHECK(ring.try_pop(out)); CHECK_EQ(out, 40);
    CHECK(!ring.try_pop(out));          // empty again
}

// Concurrency: one producer + one consumer must transfer every item exactly
// once, in order, with no loss or duplication.
static void test_spsc_threaded() {
    constexpr int kN = 200000;
    qmm::core::SpscRing<int, 1024> ring;

    std::thread producer([&] {
        for (int i = 0; i < kN; ++i)
            while (!ring.try_push(i)) { /* spin until space */ }
    });

    long long sum = 0;
    int received  = 0;
    int expected  = 0;
    bool in_order = true;
    while (received < kN) {
        int v;
        if (ring.try_pop(v)) {
            if (v != expected) in_order = false;   // SPSC must preserve FIFO
            ++expected;
            sum += v;
            ++received;
        }
    }
    producer.join();

    CHECK(in_order);
    CHECK_EQ(received, kN);
    // Sum of 0..kN-1 verifies nothing was dropped or duplicated.
    CHECK_EQ(sum, static_cast<long long>(kN) * (kN - 1) / 2);
}

// =============================================================================
//  Core: MPMC ring buffer (many producers, many consumers)
// =============================================================================
static void test_mpmc_threaded() {
    constexpr int kProducers = 4;
    constexpr int kConsumers = 4;
    constexpr int kPerProducer = 25000;
    constexpr int kTotal = kProducers * kPerProducer;

    qmm::core::MpmcRing<int> ring(1024);
    std::atomic<int> produced{0};
    std::atomic<long long> consumed_sum{0};
    std::atomic<int> consumed_count{0};

    std::vector<std::thread> threads;
    for (int p = 0; p < kProducers; ++p) {
        threads.emplace_back([&] {
            for (int i = 0; i < kPerProducer; ++i) {
                const int v = 1;   // push 1's so the total sum == kTotal
                while (!ring.try_push(v)) { /* spin */ }
                produced.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (int c = 0; c < kConsumers; ++c) {
        threads.emplace_back([&] {
            for (;;) {
                int v;
                if (ring.try_pop(v)) {
                    consumed_sum.fetch_add(v, std::memory_order_relaxed);
                    if (consumed_count.fetch_add(1, std::memory_order_relaxed) + 1 >= kTotal)
                        return;
                } else if (produced.load(std::memory_order_relaxed) >= kTotal &&
                           consumed_count.load(std::memory_order_relaxed) >= kTotal) {
                    return;
                }
            }
        });
    }
    for (auto& t : threads) t.join();

    // Every pushed item was popped exactly once (checksum is exact).
    CHECK_EQ(consumed_sum.load(), static_cast<long long>(kTotal));
    CHECK(consumed_count.load() >= kTotal);
}

// =============================================================================
//  Core: spinlock provides mutual exclusion
// =============================================================================
static void test_spinlock_mutual_exclusion() {
    qmm::core::Spinlock sl;
    long long counter = 0;         // guarded, non-atomic -> races if lock broken
    constexpr int kThreads = 8;
    constexpr int kIters   = 50000;

    std::vector<std::thread> ts;
    for (int i = 0; i < kThreads; ++i) {
        ts.emplace_back([&] {
            for (int k = 0; k < kIters; ++k) {
                sl.lock();
                ++counter;         // critical section
                sl.unlock();
            }
        });
    }
    for (auto& t : ts) t.join();
    CHECK_EQ(counter, static_cast<long long>(kThreads) * kIters);
}

// =============================================================================
//  Book: matching, accounting, and the post-only cross check
// =============================================================================
using namespace qmm::md;

static OrderMsg limit(OrderId id, Side s, Price px, Qty q, bool ours = false) {
    OrderMsg m{};
    m.id = id; m.type = MsgType::Limit; m.side = s;
    m.price = px; m.qty = q; m.is_ours = ours;
    return m;
}

static void test_order_book_match() {
    qmm::book::OrderBook ob;
    std::vector<Trade> trades;
    auto sink = [&](const Trade& t) { trades.push_back(t); };

    // Rest a bid @100 size 10 and an ask @101 size 10 -> a clean 1-tick market.
    ob.add_limit(limit(1, Side::Buy,  100, 10), sink);
    ob.add_limit(limit(2, Side::Sell, 101, 10), sink);
    CHECK(trades.empty());                 // no cross yet

    const BookTop t0 = ob.top();
    CHECK_EQ(t0.bid_price, 100);
    CHECK_EQ(t0.ask_price, 101);
    CHECK_EQ(t0.bid_qty, 10);
    CHECK_EQ(t0.ask_qty, 10);

    // An aggressive buy @101 for 6 lifts the resting ask -> one trade @101 x6.
    ob.add_limit(limit(3, Side::Buy, 101, 6), sink);
    CHECK_EQ(trades.size(), static_cast<std::size_t>(1));
    CHECK_EQ(trades[0].price, 101);        // trades at the MAKER's price
    CHECK_EQ(trades[0].qty, 6);
    CHECK(trades[0].aggressor_side == Side::Buy);

    // The ask should now show only the remaining 4.
    const BookTop t1 = ob.top();
    CHECK_EQ(t1.ask_qty, 4);
}

static void test_order_book_cancel() {
    qmm::book::OrderBook ob;
    auto sink = [](const Trade&) {};

    ob.add_limit(limit(1, Side::Buy, 100, 10), sink);
    CHECK(ob.cancel(1));                    // known id -> removed
    CHECK(!ob.cancel(1));                   // already gone -> false
    CHECK(!ob.cancel(999));                 // unknown id -> false

    const BookTop t = ob.top();
    CHECK_EQ(t.bid_qty, 0);                 // book empty on the bid side
}

// would_cross() underpins the engine's POST-ONLY guard: a quote that would
// match on entry must be reported as crossing so it can be rejected.
static void test_order_book_would_cross() {
    qmm::book::OrderBook ob;
    auto sink = [](const Trade&) {};

    ob.add_limit(limit(1, Side::Sell, 101, 10), sink);   // best ask @101
    ob.add_limit(limit(2, Side::Buy,  100, 10), sink);   // best bid @100

    // A buy priced >= best ask crosses; below it rests.
    CHECK(ob.would_cross(Side::Buy, 101));
    CHECK(ob.would_cross(Side::Buy, 102));
    CHECK(!ob.would_cross(Side::Buy, 100));
    // A sell priced <= best bid crosses; above it rests.
    CHECK(ob.would_cross(Side::Sell, 100));
    CHECK(!ob.would_cross(Side::Sell, 101));
}

// The engine tags our_resting() so risk logic can introspect our live orders.
static void test_order_book_our_resting() {
    qmm::book::OrderBook ob;
    auto sink = [](const Trade&) {};

    CHECK_EQ(ob.our_resting(), static_cast<std::size_t>(0));
    ob.add_limit(limit(1, Side::Buy,  100, 10, /*ours=*/true), sink);
    ob.add_limit(limit(2, Side::Sell, 101, 10, /*ours=*/true), sink);
    CHECK_EQ(ob.our_resting(), static_cast<std::size_t>(2));
    ob.cancel(1);
    CHECK_EQ(ob.our_resting(), static_cast<std::size_t>(1));
}

// =============================================================================
//  Risk: position limit + loss kill switch
// =============================================================================
static void test_risk_position_limit() {
    qmm::risk::RiskEngine risk(qmm::risk::RiskEngine::Limits{ /*max_position=*/100, /*max_loss=*/1e9 });

    risk.update_state(/*position=*/90, /*equity=*/0.0);
    CHECK(risk.allow(limit(1, Side::Buy, 100, 10, true)));   // 90+10=100 == cap -> ok
    CHECK(!risk.allow(limit(2, Side::Buy, 100, 20, true)));  // 90+20=110 > cap -> reject

    // Cancels are always allowed (they only reduce exposure).
    OrderMsg c{}; c.type = MsgType::Cancel; c.id = 1;
    CHECK(risk.allow(c));
}

static void test_risk_kill_switch() {
    qmm::risk::RiskEngine risk(qmm::risk::RiskEngine::Limits{ 1000, /*max_loss=*/1e6 });

    CHECK(risk.allow(limit(1, Side::Buy, 100, 10, true)));   // healthy -> ok
    risk.update_state(0, /*equity=*/-2e6);                   // breach the loss cap
    CHECK(risk.halted());
    CHECK(!risk.allow(limit(2, Side::Buy, 100, 10, true)));  // halted -> reject new
    OrderMsg c{}; c.type = MsgType::Cancel;
    CHECK(risk.allow(c));                                    // ...but cancels still pass
}

// =============================================================================
//  Analytics: P&L accounting
// =============================================================================
static Trade our_fill(Side aggressor, Price px, Qty q, bool taker_ours) {
    Trade t{};
    t.price = px; t.qty = q; t.aggressor_side = aggressor;
    t.taker_is_ours = taker_ours;
    t.maker_is_ours = !taker_ours;
    return t;
}

static void test_pnl_round_trip() {
    qmm::analytics::PnLTracker pnl;

    // Buy 10 @100 (we are the maker; aggressor sold into us) -> long 10, cash -1000.
    pnl.on_trade(our_fill(Side::Sell, 100, 10, /*taker_ours=*/false));
    CHECK_EQ(pnl.position(), 10);
    CHECK(pnl.cash() == -1000.0);

    // Sell 10 @105 (we are the maker; aggressor bought from us) -> flat, cash +50.
    pnl.on_trade(our_fill(Side::Buy, 105, 10, /*taker_ours=*/false));
    CHECK_EQ(pnl.position(), 0);
    CHECK(pnl.cash() == 50.0);                 // -1000 + 1050

    // Flat book: equity is just realised cash, independent of the mark price.
    CHECK(pnl.equity(1234) == 50.0);
}

static void test_pnl_mark_to_market() {
    qmm::analytics::PnLTracker pnl;
    pnl.on_trade(our_fill(Side::Sell, 100, 10, false));   // long 10 @100, cash -1000
    // Mark at 110: equity = -1000 + 10*110 = 100 (unrealised gain of 100).
    CHECK(pnl.equity(110) == 100.0);
    // Mark at 90: equity = -1000 + 10*90 = -100 (unrealised loss).
    CHECK(pnl.equity(90) == -100.0);
}

// =============================================================================
//  Runner
// =============================================================================
int main() {
    std::printf("running qmm unit tests...\n");

    test_spsc_basic();
    test_spsc_threaded();
    test_mpmc_threaded();
    test_spinlock_mutual_exclusion();

    test_order_book_match();
    test_order_book_cancel();
    test_order_book_would_cross();
    test_order_book_our_resting();

    test_risk_position_limit();
    test_risk_kill_switch();

    test_pnl_round_trip();
    test_pnl_mark_to_market();

    std::printf("\n%d checks, %d failure(s)\n", g_checks, g_failures);
    if (g_failures == 0) std::printf("ALL TESTS PASSED\n");
    else                 std::printf("TESTS FAILED\n");
    return g_failures == 0 ? 0 : 1;
}
