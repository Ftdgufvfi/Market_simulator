// =============================================================================
//  qmm_sim : the multithreaded low-latency market-making pipeline.
// =============================================================================
//  This is where every piece comes together into a real, pinned, lock-free
//  trading pipeline. Five stages run on five dedicated threads and hand work to
//  each other through lock-free SPSC ring buffers -- no mutexes on the hot path.
//
//  DATA FLOW
//  ---------
//        +----------+   feed_ring    +-----------------------+
//        |  FEED    |===============>|  ENGINE (order book   |
//        | (replay) |   (SPSC)       |   + matching)         |
//        +----------+                +-----------------------+
//                                       |   ^            |
//                          top_ring     |   | order_ring |  trade_ring
//                            (SPSC)      v   | (SPSC)     v   (SPSC)
//                                 +----------+   +----------------+
//                                 | STRATEGY |   |   ANALYTICS    |--promise-->main
//                                 +----------+   +----------------+  (final P&L)
//                                       |   ^
//                          risk_in      v   | (approved orders go back to ENGINE
//                            (SPSC)  +--------+   via order_ring)
//                                    |  RISK  |
//                                    +--------+
//
//  CROSS-THREAD STATE (atomics, not locks)
//    * g_position / g_equity : published by ANALYTICS, read by STRATEGY & RISK.
//    * stage-done flags      : a clean cascading shutdown (each stage drains its
//                              input fully before exiting, so no TRADES are lost
//                              and P&L is exact).
//
//  RESULT RETURN (std::promise / std::future)
//    * ANALYTICS returns the final P&L Stats to main via a std::promise.
//    * ENGINE returns the feed->engine hop latency summary the same way.
//
//  SYSTEMS TECHNIQUES ON SHOW
//    * Each stage is PINNED to its own physical core (CPU affinity) to keep
//      caches warm and avoid scheduler migration jitter.
//    * SPSC rings use cache-line padding to avoid false sharing.
//    * The engine measures per-message queue latency with the TSC and reports
//      p50/p99/p99.9 -- the numbers that actually matter in trading.
// =============================================================================
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#if defined(_MSC_VER)
  #include <intrin.h>   // _mm_pause
#endif

// On Windows we opt the console into VT (ANSI escape) processing and UTF-8 so
// the optional live TUI dashboard can redraw in place and use block-glyph
// sparklines. affinity.hpp already pulls in <windows.h> (with NOMINMAX), but we
// guard-include here too so this file is self-contained.
#if defined(_WIN32)
  #ifndef NOMINMAX
    #define NOMINMAX
  #endif
  #include <windows.h>
#endif

#include "qmm/core/affinity.hpp"
#include "qmm/core/latency_histogram.hpp"
#include "qmm/core/spsc_ring.hpp"
#include "qmm/core/timing.hpp"

#include "qmm/md/feed_replay.hpp"
#include "qmm/book/order_book.hpp"
#include "qmm/strategy/market_maker.hpp"
#include "qmm/risk/risk_engine.hpp"
#include "qmm/analytics/pnl.hpp"

using namespace qmm;

// -----------------------------------------------------------------------------
// Pipeline message wrappers.
// Each carries a TSC timestamp taken at enqueue time so a downstream stage can
// measure how long the item spent travelling between threads (queue latency).
// -----------------------------------------------------------------------------
struct FeedMsg  { md::OrderMsg order; std::uint64_t tsc; }; // feed  -> engine
struct OrderMsgT{ md::OrderMsg order; std::uint64_t tsc; }; // risk  -> engine
struct TopMsg   { md::BookTop  top;   std::uint64_t tsc; }; // engine-> strategy
struct TradeMsg { md::Trade    trade; std::uint64_t tsc; }; // engine-> analytics

// Ring capacities (power of two, as SpscRing requires).
constexpr std::size_t kRing = 1 << 16;

using FeedRing  = core::SpscRing<FeedMsg,  kRing>;
using OrderRing = core::SpscRing<OrderMsgT, kRing>;
using TopRing   = core::SpscRing<TopMsg,   kRing>;
using TradeRing = core::SpscRing<TradeMsg, kRing>;

// Small POD returned by the engine describing the queue-latency distribution.
struct LatSummary {
    std::size_t   n    = 0;
    std::uint64_t p50  = 0, p99 = 0, p999 = 0, max = 0;
    double        mean = 0.0;
    // A wider percentile curve (percentile -> latency ns) for the dashboard's
    // "the tail explodes" chart. Populated alongside the headline numbers.
    std::vector<std::pair<double, std::uint64_t>> curve;
};

// -----------------------------------------------------------------------------
// Shared, lock-free coordination state.
// -----------------------------------------------------------------------------

// Hard, engine-enforced inventory bound. The strategy's soft limit is 500 and
// each quote is 50, so a healthy pipeline should never approach this. We give a
// small buffer above the soft limit so that, in steady state, the hard cap is
// dormant and only engages to arrest a latency-driven inventory burst.
static constexpr md::Qty kHardInventoryCap = 600;

struct Shared {
    // Published by analytics; read by strategy & risk. relaxed loads/stores are
    // fine: these are hints/gates, not part of a data-publication handshake.
    std::atomic<md::Qty> position{0};
    std::atomic<double>  equity{0.0};
    // Book mid, published by the ENGINE (the only stage with the live book).
    // Analytics marks open inventory at this mid rather than at the last trade
    // price: as a PASSIVE maker we are always on the opposite side of the
    // aggressor, so the last print is systematically biased against us. Marking
    // at mid removes that microstructure bias and gives a fair P&L.
    std::atomic<md::Price> mid_price{100'000};

    // Cascading shutdown flags. Each is set by a stage AFTER it has fully
    // drained its input and will produce no more output, so the next stage can
    // safely finish once its own input is empty.
    std::atomic<bool> feed_done{false};
    std::atomic<bool> engine_done{false};
    std::atomic<bool> strategy_done{false};
    std::atomic<bool> risk_done{false};

    // --- Risk & execution-quality counters (reported at the end) -------------
    // These are genuine production-style metrics, not debug scaffolding: they
    // quantify how hard the risk controls had to work and how much stale-quote
    // flow the engine's post-only guard absorbed.
    std::atomic<long long> max_abs_pos{0};        // peak |inventory| reached
    std::atomic<long long> risk_rejects{0};       // orders blocked by risk stage
    std::atomic<long long> risk_approved{0};      // orders passed by risk stage
    std::atomic<long long> hard_cap_drops{0};     // orders stopped by hard cap
    std::atomic<long long> post_only_rejects{0};  // stale quotes that would cross

    // --- Live progress counters (for the optional TUI dashboard) -------------
    // The engine owns both. events_processed is published in coarse batches
    // (once per kProgressBatch) so the shared line is touched ~thousands of
    // times less often than the per-event hot path -- the monitor gets smooth
    // progress without perturbing the very latency we are measuring. our_fills
    // is bumped once per fill (fills are rare relative to events, so this is
    // cheap) and lets the dashboard show live execution activity.
    std::atomic<std::uint64_t> events_processed{0};
    std::atomic<std::uint64_t> our_fills{0};

    // Live top-of-book, published by the engine on each throttled snapshot so
    // the dashboard can show the market the strategy is quoting against.
    std::atomic<md::Price> best_bid{0};
    std::atomic<md::Price> best_ask{0};
    std::atomic<md::Qty>   bid_qty{0};
    std::atomic<md::Qty>   ask_qty{0};
    // Exponentially-weighted moving average of feed->engine hand-off latency
    // (ns). A cheap live proxy for the median that the dashboard can display
    // without maintaining a full running histogram on the hot path.
    std::atomic<double>    lat_ewma{0.0};
};

// How many feed events the engine processes between publishes of the shared
// progress counter (see Shared::events_processed). A power of two so the engine
// can test it with a cheap bit-mask.
static constexpr std::uint64_t kProgressBatch = 4096;

// Helper: pin the calling thread to a physical core (first logical CPU of the
// Nth physical core), if pinning is enabled and that core exists.
static void pin_stage(const core::Topology& topo, unsigned stage_index, bool pin) {
    if (!pin) return;
    if (stage_index < topo.cores.size() &&
        !topo.cores[stage_index].logical_cpus.empty()) {
        core::pin_current_thread_to_cpu(topo.cores[stage_index].logical_cpus[0]);
        core::boost_current_thread_priority();
    }
}

// Blocking push that still bails out if the whole pipeline is shutting down, so
// we can never deadlock spinning on a full ring whose consumer has exited.
template <typename Ring, typename T>
static inline void push_blocking(Ring& r, const T& v, const std::atomic<bool>& abort) {
    while (!r.try_push(v)) {
        if (abort.load(std::memory_order_relaxed)) return;
        _mm_pause();
    }
}

// =============================================================================
//  Stage 1: FEED  -- generate market data and publish it to the engine.
//
//  Optional PACING: a real feed arrives at some rate, not infinitely fast. If we
//  let the feed flood, the rings saturate and every measurement degenerates into
//  "buffering delay" (and the cross-thread position feedback goes stale, breaking
//  the control loop). So by default we pace the feed to a target events/second
//  BELOW the pipeline's capacity, keeping queues shallow. That is exactly how you
//  measure true hand-off latency: at a fixed offered load under saturation.
//  Pass target_rate <= 0 to run unpaced (used to measure peak throughput).
// =============================================================================
static void run_feed(md::SyntheticFeed feed, FeedRing& out, Shared& sh,
                     const core::TscClock& clk, double target_rate,
                     const core::Topology& topo, bool pin) {
    pin_stage(topo, 0, pin);

    const bool paced = target_rate > 0.0;
    // How many TSC cycles should elapse between consecutive events.
    const double cycles_per_event =
        paced ? (clk.cycles_per_ns() * 1e9 / target_rate) : 0.0;
    std::uint64_t next_tsc = core::rdtsc();

    md::OrderMsg m;
    while (feed.next(m)) {
        if (paced) {
            next_tsc += static_cast<std::uint64_t>(cycles_per_event);
            // Clamp: never let the schedule fall behind "now". Without this, a
            // scheduler hiccup would leave next_tsc far in the past and the feed
            // would DUMP a catch-up burst, transiently saturating the ring and
            // spiking tail latency. Pacing should shape load, not create bursts.
            const std::uint64_t now = core::rdtsc();
            if (next_tsc < now) next_tsc = now;
            else while (core::rdtsc() < next_tsc) _mm_pause();
        }
        FeedMsg fm{m, core::rdtsc()};   // timestamp AT publish for latency calc
        push_blocking(out, fm, sh.feed_done /*abort stays false during feed*/);
    }
    sh.feed_done.store(true, std::memory_order_release);
}

// =============================================================================
//  Stage 2: ENGINE -- the order book + matching engine.
//  Consumes market messages (feed) AND our approved orders (risk), matches them,
//  emits trades (to analytics) and throttled top-of-book snapshots (to strategy).
// =============================================================================
static void run_engine(FeedRing& feed_in, OrderRing& order_in,
                       TopRing& top_out, TradeRing& trade_out,
                       Shared& sh, const core::TscClock& clk,
                       std::promise<LatSummary> lat_promise,
                       const core::Topology& topo, bool pin) {
    pin_stage(topo, 1, pin);

    book::OrderBook ob;
    core::LatencyHistogram hist(1 << 20);

    // Trade sink: (1) update our REAL-TIME position immediately (the engine sees
    // every fill first, so it is the authoritative, zero-lag writer of
    // sh.position -- this is what lets the strategy/risk control inventory), and
    // (2) forward every execution to analytics (blocking push so no trade is
    // ever lost -- P&L must be exact).
    auto sink = [&](const md::Trade& t) {
        if (t.taker_is_ours || t.maker_is_ours) {
            const md::Side our = t.taker_is_ours
                                     ? t.aggressor_side
                                     : md::opposite(t.aggressor_side);
            const md::Qty d = (our == md::Side::Buy) ? t.qty : -t.qty;
            const md::Qty newpos =
                sh.position.fetch_add(d, std::memory_order_relaxed) + d;
            sh.our_fills.fetch_add(1, std::memory_order_relaxed);  // live fill count
            long long ap = newpos < 0 ? -newpos : newpos;
            long long prev = sh.max_abs_pos.load(std::memory_order_relaxed);
            while (ap > prev && !sh.max_abs_pos.compare_exchange_weak(prev, ap, std::memory_order_relaxed)) {}
        }
        push_blocking(trade_out, TradeMsg{t, core::rdtsc()}, sh.engine_done);
    };

    std::uint64_t since_top = 0;    // throttle counter for top snapshots
    std::uint64_t processed_local = 0; // engine-local feed-event tally (batched publish)
    double lat_ewma_local = 0.0;    // engine-local latency EWMA (batched publish)
    int idle = 0;                   // consecutive idle spins (for clean exit)
    md::Ts last_ts = 0;

    for (;;) {
        bool did_work = false;

        // (a) Drain our own approved orders first (they are latency-critical).
        OrderMsgT om;
        while (order_in.try_pop(om)) {
            const md::OrderMsg& o = om.order;
            if (o.type == md::MsgType::Cancel) {
                ob.cancel(o.id);
            } else {
                // ---------------------------------------------------------
                // HARD INVENTORY CAP (zero-lag, engine-authoritative).
                // The strategy and risk stages gate on sh.position, but that
                // value is one pipeline-length STALE: by the time an order
                // arrives here the market may have moved so our resting quote
                // now crosses and fills as a TAKER, in bursts, before the
                // strategy can react. The engine is the ONLY stage that sees
                // every fill at zero lag, so it is the correct place for a
                // final, hard risk stop. If accepting this order could push
                // our absolute inventory past the hard cap, we DROP it. This
                // bounds inventory tightly regardless of pipeline latency --
                // a realistic "last line of defence" pre-trade risk check.
                // ---------------------------------------------------------
                const md::Qty pos = sh.position.load(std::memory_order_relaxed);
                const bool would_grow_long  = (o.side == md::Side::Buy)  && pos >= kHardInventoryCap;
                const bool would_grow_short = (o.side == md::Side::Sell) && pos <= -kHardInventoryCap;
                if (would_grow_long || would_grow_short) {
                    sh.hard_cap_drops.fetch_add(1, std::memory_order_relaxed);
                } else if (o.is_ours && ob.would_cross(o.side, o.price)) {
                    // POST-ONLY: our quote was priced off a book snapshot that is
                    // now stale; it would cross and fill as a TAKER (paying the
                    // spread + adverse selection). A market maker must only ADD
                    // liquidity, so we reject it rather than let it take.
                    sh.post_only_rejects.fetch_add(1, std::memory_order_relaxed);
                } else {
                    ob.add_limit(o, sink);
                }
            }
            did_work = true;
        }

        // (b) Drain market data from the feed, measuring queue latency.
        FeedMsg fm;
        while (feed_in.try_pop(fm)) {
            // Cross-core __rdtsc reads can occasionally appear very slightly out
            // of order (the counters are synchronised but not perfectly ordered
            // w.r.t. surrounding instructions), which would make `now - fm.tsc`
            // underflow to a garbage huge unsigned value. Guard against that so
            // one bad sample can never corrupt the max/mean of the histogram.
            const std::uint64_t now = core::rdtsc();
            const std::uint64_t lat_cycles = (now > fm.tsc) ? (now - fm.tsc) : 0;
            const double lat_ns = clk.cycles_to_ns(lat_cycles);
            hist.record(static_cast<std::uint64_t>(lat_ns));
            // Update a live latency EWMA (published in batches below). The first
            // sample seeds it; thereafter a small alpha gives a smooth median-ish
            // read that tracks the current regime without hot-path bookkeeping.
            lat_ewma_local = (lat_ewma_local <= 0.0) ? lat_ns
                                                     : lat_ewma_local * 0.995 + lat_ns * 0.005;

            const md::OrderMsg& o = fm.order;
            last_ts = o.ts;
            if (o.type == md::MsgType::Cancel) ob.cancel(o.id);
            else                               ob.add_limit(o, sink);

            // Throttle top-of-book snapshots to the strategy (every 50 events)
            // so the strategy sees a manageable, representative stream.
            if (++since_top >= 50) {
                since_top = 0;
                const md::BookTop t = ob.top(o.ts);
                if (t.has_both()) {
                    top_out.try_push(TopMsg{t, core::rdtsc()}); // ok to drop
                    // Publish the live mid for fair inventory marking downstream.
                    sh.mid_price.store((t.bid_price + t.ask_price) / 2,
                                       std::memory_order_relaxed);
                    // Publish the live top-of-book for the dashboard.
                    sh.best_bid.store(t.bid_price, std::memory_order_relaxed);
                    sh.best_ask.store(t.ask_price, std::memory_order_relaxed);
                    sh.bid_qty.store(t.bid_qty,   std::memory_order_relaxed);
                    sh.ask_qty.store(t.ask_qty,   std::memory_order_relaxed);
                }
            }
            // Publish coarse progress for the optional live dashboard. We only
            // touch the shared line once per kProgressBatch events so the hot
            // path stays clean and the latency numbers stay honest.
            if ((++processed_local & (kProgressBatch - 1)) == 0) {
                sh.events_processed.store(processed_local, std::memory_order_relaxed);
                sh.lat_ewma.store(lat_ewma_local, std::memory_order_relaxed);
            }
            did_work = true;
        }

        if (did_work) { idle = 0; continue; }

        // Nothing to do. Exit only once the feed is finished AND both of our
        // inputs are empty for a sustained stretch (so no in-flight order is
        // missed). A short settle window makes shutdown robust to races.
        if (sh.feed_done.load(std::memory_order_acquire) &&
            feed_in.size_approx() == 0 && order_in.size_approx() == 0) {
            if (++idle > 100000) break;
        } else {
            idle = 0;
        }
        _mm_pause();
    }

    // Publish a final snapshot so late marks use a sane price, then report.
    (void)last_ts;
    sh.events_processed.store(processed_local, std::memory_order_relaxed);
    sh.engine_done.store(true, std::memory_order_release);

    LatSummary s;
    s.n    = hist.count();
    s.p50  = hist.percentile(50.0);
    s.p99  = hist.percentile(99.0);
    s.p999 = hist.percentile(99.9);
    s.max  = hist.max();
    s.mean = hist.mean();
    // Wider percentile curve for the dashboard's tail-latency chart. These
    // ranks visualise how the hand-off stays flat up to ~p95 then explodes in
    // the tail once queues transiently build under load.
    for (double p : {1.0, 10.0, 25.0, 50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99, 100.0})
        s.curve.emplace_back(p, hist.percentile(p));
    lat_promise.set_value(s);
}

// =============================================================================
//  Stage 3: STRATEGY -- turn top-of-book snapshots into quote intents.
// =============================================================================
static void run_strategy(TopRing& top_in, OrderRing& risk_out /*to risk*/,
                         Shared& sh, const core::Topology& topo, bool pin) {
    pin_stage(topo, 2, pin);
    strategy::MarketMaker mm(strategy::MarketMaker::Params{});
    int idle = 0;

    for (;;) {
        TopMsg tm;
        bool did_work = false;
        while (top_in.try_pop(tm)) {
            const md::Qty pos = sh.position.load(std::memory_order_relaxed);
            auto q = mm.on_tick(tm.top, pos, tm.top.ts);
            // Push cancels first, then new quotes (blocking so the risk stage
            // never misses a cancel, which would strand a stale quote).
            if (q.has_cancel_bid) push_blocking(risk_out, OrderMsgT{q.cancel_bid, core::rdtsc()}, sh.strategy_done);
            if (q.has_cancel_ask) push_blocking(risk_out, OrderMsgT{q.cancel_ask, core::rdtsc()}, sh.strategy_done);
            if (q.has_new_bid)    push_blocking(risk_out, OrderMsgT{q.new_bid,    core::rdtsc()}, sh.strategy_done);
            if (q.has_new_ask)    push_blocking(risk_out, OrderMsgT{q.new_ask,    core::rdtsc()}, sh.strategy_done);
            did_work = true;
        }
        if (did_work) { idle = 0; continue; }

        if (sh.engine_done.load(std::memory_order_acquire) &&
            top_in.size_approx() == 0) {
            if (++idle > 100000) break;
        } else idle = 0;
        _mm_pause();
    }
    sh.strategy_done.store(true, std::memory_order_release);
}

// =============================================================================
//  Stage 4: RISK -- gate our orders, then forward approved ones to the engine.
// =============================================================================
static void run_risk(OrderRing& risk_in, OrderRing& engine_out,
                     Shared& sh, const core::Topology& topo, bool pin) {
    pin_stage(topo, 3, pin);
    risk::RiskEngine risk(risk::RiskEngine::Limits{});
    int idle = 0;

    for (;;) {
        OrderMsgT om;
        bool did_work = false;
        while (risk_in.try_pop(om)) {
            // Refresh risk's view of our exposure from the published atomics.
            const md::Qty pos = sh.position.load(std::memory_order_relaxed);
            risk.update_state(pos, sh.equity.load(std::memory_order_relaxed));
            const bool ok = risk.allow(om.order);
            if (om.order.type == md::MsgType::Limit) {
                if (ok) sh.risk_approved.fetch_add(1, std::memory_order_relaxed);
                else    sh.risk_rejects.fetch_add(1, std::memory_order_relaxed);
            }
            // Forward approved orders BACK to the engine (the pipeline's one
            // feedback edge). The abort flag must be the CONSUMER's done-flag
            // (engine_done), NOT our own: the engine can finish and exit while
            // we are still draining a backlog of quotes, and if we passed our
            // own risk_done here (always false inside this loop) a full
            // order_ring would spin forever and hang shutdown at join(). With
            // engine_done, once the engine is gone we simply drop these late
            // quotes -- harmless, since post-only/hard-cap mean they would only
            // rest or be rejected, never a lost trade.
            if (ok) push_blocking(engine_out, om, sh.engine_done);
            did_work = true;
        }
        if (did_work) { idle = 0; continue; }

        if (sh.strategy_done.load(std::memory_order_acquire) &&
            risk_in.size_approx() == 0) {
            if (++idle > 100000) break;
        } else idle = 0;
        _mm_pause();
    }
    sh.risk_done.store(true, std::memory_order_release);
}

// =============================================================================
//  Stage 5: ANALYTICS -- consume trades, track P&L, publish position/equity,
//  and return the final Stats to main via a promise.
// =============================================================================
static void run_analytics(TradeRing& trade_in, Shared& sh,
                          std::promise<analytics::PnLTracker::Stats> stats_promise,
                          const core::Topology& topo, bool pin) {
    pin_stage(topo, 4, pin);
    analytics::PnLTracker pnl;
    md::Price last_price = 100'000;   // fallback mid until first trade seen
    std::uint64_t seen = 0;
    int idle = 0;

    for (;;) {
        TradeMsg tm;
        bool did_work = false;
        while (trade_in.try_pop(tm)) {
            pnl.on_trade(tm.trade);
            last_price = tm.trade.price;      // fallback mark if no mid yet

            // Mark open inventory at the live book MID (fair, unbiased) rather
            // than the last trade price. Fall back to last trade before the
            // engine has published a two-sided mid.
            const md::Price mark = sh.mid_price.load(std::memory_order_relaxed);
            const md::Price mtm  = (mark != 0) ? mark : last_price;

            // Publish updated equity for the risk kill switch. (Position is
            // published in real time by the ENGINE, so we do NOT write it here.)
            sh.equity.store(pnl.equity(mtm), std::memory_order_relaxed);

            // Sample the equity curve periodically for Sharpe/drawdown.
            if (++seen % 100 == 0) pnl.mark(mtm);
            did_work = true;
        }
        if (did_work) { idle = 0; continue; }

        // Analytics is the tail of the pipeline: once the engine is done and no
        // trades remain, every execution has been accounted for.
        if (sh.engine_done.load(std::memory_order_acquire) &&
            trade_in.size_approx() == 0) {
            if (++idle > 100000) break;
        } else idle = 0;
        _mm_pause();
    }

    // Final mark at the last published mid (fair) for the closing P&L snapshot.
    const md::Price final_mark = sh.mid_price.load(std::memory_order_relaxed);
    stats_promise.set_value(pnl.finalize(final_mark != 0 ? final_mark : last_price));
}

// =============================================================================
//  Optional live TUI dashboard + metrics export.
//  Both are OFF by default so a plain run produces the same clean, unperturbed
//  measurements as before. They read only already-published atomics, so they
//  add no cost to the hot path when disabled.
// =============================================================================

#if defined(_WIN32)
// Opt the Windows console into ANSI-escape (VT) processing and UTF-8 output so
// the dashboard can redraw in place and render block-glyph sparklines.
static void enable_vt_console() {
    HANDLE h = GetStdHandle(STD_OUTPUT_HANDLE);
    DWORD mode = 0;
    if (h != INVALID_HANDLE_VALUE && GetConsoleMode(h, &mode))
        SetConsoleMode(h, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
    SetConsoleOutputCP(CP_UTF8);
}
#else
static void enable_vt_console() {}
#endif

// Append a compact block-glyph sparkline (U+2581..U+2588) of `v` to `out`.
// We emit the UTF-8 bytes explicitly (E2 96 81..88) rather than using a string
// literal so the result is independent of the source-file / narrow charset.
static void sparkline(const std::vector<double>& v, std::string& out) {
    out.clear();
    if (v.empty()) return;
    double lo = v.front(), hi = v.front();
    for (double x : v) { lo = std::min(lo, x); hi = std::max(hi, x); }
    const double span = (hi - lo) > 1e-9 ? (hi - lo) : 1.0;
    for (double x : v) {
        int lvl = static_cast<int>((x - lo) / span * 7.0 + 0.5);
        lvl = std::max(0, std::min(7, lvl));
        out += '\xE2'; out += '\x96'; out += static_cast<char>(0x81 + lvl);
    }
}

// Render a fixed-width horizontal bar of length `width`, filled to `frac`.
static std::string bar(double frac, int width) {
    frac = std::max(0.0, std::min(1.0, frac));
    const int fill = static_cast<int>(frac * width + 0.5);
    return std::string(fill, '#') + std::string(width - fill, '.');
}

// The live monitor thread. Runs alongside the pipeline, reads the published
// atomics every ~100 ms and repaints an in-place dashboard until `stop` is set.
static void run_dashboard(const Shared& sh, std::size_t num_events,
                          std::atomic<bool>& stop, std::uint64_t start_ns) {
    // ANSI helpers.
    constexpr const char* HOME = "\x1b[H";      // cursor to top-left
    constexpr const char* CLR  = "\x1b[2J";     // clear whole screen
    constexpr const char* EL   = "\x1b[K";      // clear to end of line
    constexpr const char* HIDE = "\x1b[?25l";   // hide cursor (no flicker)
    constexpr const char* SHOW = "\x1b[?25h";   // restore cursor
    constexpr const char* RST  = "\x1b[0m";
    constexpr const char* CY   = "\x1b[36m";    // cyan
    constexpr const char* YL   = "\x1b[33m";    // yellow
    constexpr const char* GR   = "\x1b[32m";    // green
    constexpr const char* RD   = "\x1b[31m";    // red
    constexpr const char* DIM  = "\x1b[2m";
    constexpr const char* BLD  = "\x1b[1m";

    std::fputs(CLR, stdout);
    std::fputs(HIDE, stdout);

    std::vector<double> eq_hist;                 // rolling equity samples
    std::vector<double> tp_hist;                 // rolling throughput samples
    std::vector<double> pos_hist;                // rolling position samples
    std::uint64_t prev_events = 0;
    std::uint64_t prev_ns = start_ns;
    std::string spark, spark_tp, spark_pos, frame;

    auto paint = [&](bool final_frame) {
        const std::uint64_t now = core::now_ns();
        const std::uint64_t ev  = sh.events_processed.load(std::memory_order_relaxed);
        const long long pos     = static_cast<long long>(sh.position.load(std::memory_order_relaxed));
        const double eq         = sh.equity.load(std::memory_order_relaxed);
        const std::uint64_t fills = sh.our_fills.load(std::memory_order_relaxed);
        const long long peak    = sh.max_abs_pos.load(std::memory_order_relaxed);
        const long long appr    = sh.risk_approved.load(std::memory_order_relaxed);
        const long long rej     = sh.risk_rejects.load(std::memory_order_relaxed);
        const long long po      = sh.post_only_rejects.load(std::memory_order_relaxed);
        const long long hc      = sh.hard_cap_drops.load(std::memory_order_relaxed);
        const long long mid     = static_cast<long long>(sh.mid_price.load(std::memory_order_relaxed));
        const long long bbid    = static_cast<long long>(sh.best_bid.load(std::memory_order_relaxed));
        const long long bask    = static_cast<long long>(sh.best_ask.load(std::memory_order_relaxed));
        const long long bqty    = static_cast<long long>(sh.bid_qty.load(std::memory_order_relaxed));
        const long long aqty    = static_cast<long long>(sh.ask_qty.load(std::memory_order_relaxed));
        const double lat_live   = sh.lat_ewma.load(std::memory_order_relaxed);

        // Instantaneous + overall throughput.
        const double dt   = (now > prev_ns) ? (now - prev_ns) / 1e9 : 1e-9;
        const double inst = (ev >= prev_events) ? (ev - prev_events) / dt / 1e6 : 0.0;
        const double overall = (now > start_ns) ? ev / ((now - start_ns) / 1e9) / 1e6 : 0.0;
        prev_events = ev; prev_ns = now;

        eq_hist.push_back(eq);
        if (eq_hist.size() > 60) eq_hist.erase(eq_hist.begin());
        sparkline(eq_hist, spark);
        tp_hist.push_back(inst);
        if (tp_hist.size() > 40) tp_hist.erase(tp_hist.begin());
        sparkline(tp_hist, spark_tp);
        pos_hist.push_back(static_cast<double>(pos));
        if (pos_hist.size() > 40) pos_hist.erase(pos_hist.begin());
        sparkline(pos_hist, spark_pos);

        const double prog = num_events ? std::min(1.0, (double)ev / (double)num_events) : 0.0;
        const char* eqcol = eq >= 0 ? GR : RD;
        const double invfrac = kHardInventoryCap ? (double)std::llabs(pos) / (double)kHardInventoryCap : 0.0;
        const char* invcol = invfrac < 0.5 ? GR : (invfrac < 0.85 ? YL : RD);

        frame.clear();
        frame += HOME;
        auto line = [&](const std::string& s){ frame += s; frame += EL; frame += "\r\n"; };

        line(std::string(BLD) + CY + "  quant-mm-sim  " + RST + DIM + "| live low-latency market-making pipeline" + RST);
        line(std::string(DIM) + "  ---------------------------------------------------------------" + RST);
        {
            char b[256];
            std::snprintf(b, sizeof b, "  progress  [%s%s%s] %5.1f%%   %s%s events",
                          CY, bar(prog, 32).c_str(), RST, prog * 100.0,
                          std::to_string((unsigned long long)ev).c_str(),
                          num_events ? ("/" + std::to_string((unsigned long long)num_events)).c_str() : "");
            line(b);
        }
        {
            char b[320];
            std::snprintf(b, sizeof b, "  throughput  %s%6.2f M/s%s  (inst)   %s%6.2f M/s%s  (overall)  %s%s%s",
                          YL, inst, RST, DIM, overall, RST, DIM, spark_tp.c_str(), RST);
            line(b);
        }
        {
            // Live feed->engine latency (EWMA proxy for p50) -- colour by regime.
            const char* lcol = lat_live < 300 ? GR : (lat_live < 2000 ? YL : RD);
            char b[256];
            std::snprintf(b, sizeof b, "  latency     %s%7.0f ns%s  (live EWMA, feed->engine hand-off)",
                          lcol, lat_live, RST);
            line(b);
        }
        line("");
        line(std::string(DIM) + "  -- market (top of book) --------------------------------------" + RST);
        {
            // A compact L1 order-book view: best bid size/price | price/size ask,
            // the spread in ticks, and the queue imbalance as a centred gauge.
            const long long spr = (bask > bbid) ? (bask - bbid) : 0;
            const double denom = static_cast<double>(bqty + aqty);
            const double imb = denom > 0 ? (bqty - aqty) / denom : 0.0; // [-1,+1]
            // Centred imbalance bar: left half = sell pressure, right = buy.
            const int half = 12;
            int mag = static_cast<int>(std::llabs((long long)(imb * half)));
            mag = std::max(0, std::min(half, mag));
            std::string g = std::string(half - (imb < 0 ? mag : 0), ' ')
                          + std::string(imb < 0 ? mag : 0, '<')
                          + "|"
                          + std::string(imb > 0 ? mag : 0, '>')
                          + std::string(half - (imb > 0 ? mag : 0), ' ');
            const char* icol = imb > 0.05 ? GR : (imb < -0.05 ? RD : DIM);
            char b[320];
            std::snprintf(b, sizeof b,
                          "  bid %s%lld%s x %lld   %s|%s   %lld x ask %s%lld%s    spread %s%lldt%s",
                          GR, bbid, RST, bqty, DIM, RST, aqty, RD, bask, RST, YL, spr, RST);
            line(b);
            std::snprintf(b, sizeof b, "  imbalance   %s[%s]%s  %s%+.2f%s     mid %s%lld%s",
                          icol, g.c_str(), RST, icol, imb, RST, CY, mid, RST);
            line(b);
        }
        line("");
        {
            char b[320];
            std::snprintf(b, sizeof b, "  position  %s%+6lld%s  [%s%s%s]  cap %lld     %s%s%s",
                          invcol, pos, RST, invcol, bar(invfrac, 24).c_str(), RST,
                          (long long)kHardInventoryCap, DIM, spark_pos.c_str(), RST);
            line(b);
        }
        {
            char b[256];
            std::snprintf(b, sizeof b, "  equity    %s%+12.0f%s tick-$   fills %s%llu%s",
                          eqcol, eq, RST, CY, (unsigned long long)fills, RST);
            line(b);
        }
        {
            char b[256];
            std::snprintf(b, sizeof b, "  P&L path  %s%s%s", eqcol, spark.c_str(), RST);
            line(b);
        }
        line("");
        line(std::string(DIM) + "  -- risk & execution quality ----------------------------------" + RST);
        {
            char b[256];
            std::snprintf(b, sizeof b, "  risk approved %s%lld%s   rejects %s%lld%s   peak|inv| %lld",
                          GR, appr, RST, (rej ? RD : DIM), rej, RST, peak);
            line(b);
        }
        {
            char b[256];
            std::snprintf(b, sizeof b, "  post-only rejects %s%lld%s   hard-cap drops %s%lld%s",
                          YL, po, RST, (hc ? RD : DIM), hc, RST);
            line(b);
        }
        line("");
        line(std::string(DIM) + (final_frame ? "  done. final report below." : "  running...  (Ctrl-C to abort)") + RST);
        // Clear anything left from a previous, taller frame.
        frame += "\x1b[J";
        std::fputs(frame.c_str(), stdout);
        std::fflush(stdout);
    };

    while (!stop.load(std::memory_order_relaxed)) {
        paint(false);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    paint(true);                 // one last frame with the settled numbers
    std::fputs(SHOW, stdout);    // restore the cursor
    std::fputs("\r\n", stdout);
    std::fflush(stdout);
}

// -----------------------------------------------------------------------------
// Metrics export. Writes a compact JSON document describing the run so the HTML
// dashboard (docs/dashboard.html) can visualise it. Hand-rolled to avoid any
// JSON dependency -- the structure is small and fixed.
// -----------------------------------------------------------------------------
static void write_metrics_json(const std::string& path, const core::Topology& topo,
                               const core::TscClock& clk, std::size_t num_events,
                               double rate, bool pin, double secs,
                               const LatSummary& lat,
                               const analytics::PnLTracker::Stats& st,
                               const Shared& sh) {
    std::ofstream f(path, std::ios::binary);
    if (!f) { std::fprintf(stderr, "warning: could not open %s for writing\n", path.c_str()); return; }

    f << "{\n";
    f << "  \"kind\": \"qmm_sim\",\n";
    f << "  \"config\": {"
      << " \"events\": " << num_events
      << ", \"offered_rate\": " << (long long)(rate > 0 ? rate : 0)
      << ", \"paced\": " << (rate > 0 ? "true" : "false")
      << ", \"pinning\": " << (pin ? "true" : "false")
      << ", \"logical\": " << core::hardware_threads()
      << ", \"physical_cores\": " << topo.cores.size()
      << ", \"numa_nodes\": " << topo.numa_nodes
      << ", \"smt\": " << (topo.smt_enabled() ? "true" : "false")
      << ", \"tsc_cycles_per_ns\": " << clk.cycles_per_ns()
      << ", \"hard_inventory_cap\": " << (long long)kHardInventoryCap
      << " },\n";
    f << "  \"throughput\": {"
      << " \"seconds\": " << secs
      << ", \"events\": " << num_events
      << ", \"achieved_mps\": " << (num_events / secs / 1e6)
      << " },\n";
    f << "  \"latency_ns\": {"
      << " \"n\": " << lat.n
      << ", \"p50\": " << lat.p50
      << ", \"p99\": " << lat.p99
      << ", \"p999\": " << lat.p999
      << ", \"max\": " << lat.max
      << ", \"mean\": " << lat.mean
      << ", \"curve\": [";
    for (std::size_t i = 0; i < lat.curve.size(); ++i) {
        if (i) f << ", ";
        f << "{\"p\": " << lat.curve[i].first << ", \"ns\": " << lat.curve[i].second << "}";
    }
    f << "] },\n";
    f << "  \"pnl\": {"
      << " \"our_fills\": " << st.our_fills
      << ", \"final_position\": " << (long long)st.final_position
      << ", \"final_equity\": " << st.final_equity
      << ", \"peak_equity\": " << st.peak_equity
      << ", \"max_drawdown\": " << st.max_drawdown
      << ", \"sharpe\": " << st.sharpe
      << " },\n";
    f << "  \"risk\": {"
      << " \"peak_abs_inventory\": " << sh.max_abs_pos.load()
      << ", \"hard_cap\": " << (long long)kHardInventoryCap
      << ", \"risk_approved\": " << sh.risk_approved.load()
      << ", \"risk_rejects\": " << sh.risk_rejects.load()
      << ", \"post_only_rejects\": " << sh.post_only_rejects.load()
      << ", \"hard_cap_drops\": " << sh.hard_cap_drops.load()
      << " },\n";

    // Downsample the equity curve to at most kMaxPts points so the JSON (and the
    // chart) stay lightweight regardless of run length.
    constexpr std::size_t kMaxPts = 400;
    const std::vector<double>& ec = st.equity_curve;
    const std::size_t stride = ec.size() > kMaxPts ? (ec.size() + kMaxPts - 1) / kMaxPts : 1;
    f << "  \"equity_curve\": [";
    bool first = true;
    for (std::size_t i = 0; i < ec.size(); i += stride) {
        if (!first) f << ", ";
        f << ec[i];
        first = false;
    }
    f << "]\n";
    f << "}\n";
    std::printf("  wrote metrics to %s\n", path.c_str());
}

// =============================================================================
//  main : configure, launch the pinned pipeline, collect results via futures.
// =============================================================================
int main(int argc, char** argv) {
    // ---- Tiny command-line parsing ----
    std::size_t num_events = 2'000'000;
    bool pin = true;
    double rate = 1'000'000.0;   // offered load (events/s); <=0 means unpaced
    bool dashboard = false;      // --dashboard : live in-place TUI
    std::string metrics_out;     // --metrics-out <path> : write JSON metrics
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--events") == 0 && i + 1 < argc)
            num_events = std::strtoull(argv[++i], nullptr, 10);
        else if (std::strcmp(argv[i], "--rate") == 0 && i + 1 < argc)
            rate = std::strtod(argv[++i], nullptr);
        else if (std::strcmp(argv[i], "--no-pin") == 0)
            pin = false;
        else if (std::strcmp(argv[i], "--dashboard") == 0)
            dashboard = true;
        else if (std::strcmp(argv[i], "--metrics-out") == 0 && i + 1 < argc)
            metrics_out = argv[++i];
    }

    const core::Topology topo = core::discover_topology();
    const core::TscClock clk = core::TscClock::calibrate();

    std::printf("=== quant-mm-sim pipeline ===\n");
    std::printf("logical=%u physical_cores=%zu numa=%u smt=%d | pinning=%s | events=%zu | offered_rate=%s\n",
                core::hardware_threads(), topo.cores.size(), topo.numa_nodes,
                (int)topo.smt_enabled(), pin ? "on" : "off", num_events,
                rate > 0 ? (std::to_string((long long)rate) + "/s").c_str() : "unpaced");
    std::printf("tsc calibrated at %.3f cycles/ns\n\n", clk.cycles_per_ns());

    // ---- Allocate the rings on the heap (they are large) ----
    auto feed_ring  = std::make_unique<FeedRing>();
    auto order_ring = std::make_unique<OrderRing>();   // risk -> engine
    auto risk_ring  = std::make_unique<OrderRing>();   // strategy -> risk
    auto top_ring   = std::make_unique<TopRing>();
    auto trade_ring = std::make_unique<TradeRing>();

    Shared sh;

    // ---- Futures for results returned from worker threads ----
    std::promise<LatSummary> lat_promise;
    auto lat_future = lat_promise.get_future();
    std::promise<analytics::PnLTracker::Stats> stats_promise;
    auto stats_future = stats_promise.get_future();

    md::SyntheticFeed::Config fc;
    fc.num_events = num_events;
    md::SyntheticFeed feed(fc);

    const std::uint64_t t_start = core::now_ns();

    // ---- Launch the five stages ----
    std::thread t_feed (run_feed,     std::move(feed), std::ref(*feed_ring), std::ref(sh),
                                      std::cref(clk), rate, std::cref(topo), pin);
    std::thread t_eng  (run_engine,   std::ref(*feed_ring), std::ref(*order_ring),
                                      std::ref(*top_ring), std::ref(*trade_ring),
                                      std::ref(sh), std::cref(clk), std::move(lat_promise),
                                      std::cref(topo), pin);
    std::thread t_strat(run_strategy, std::ref(*top_ring), std::ref(*risk_ring), std::ref(sh), std::cref(topo), pin);
    std::thread t_risk (run_risk,     std::ref(*risk_ring), std::ref(*order_ring), std::ref(sh), std::cref(topo), pin);
    std::thread t_anal (run_analytics,std::ref(*trade_ring), std::ref(sh), std::move(stats_promise), std::cref(topo), pin);

    // ---- Optional live TUI: a monitor thread that repaints in place while the
    //      pipeline runs. It reads only published atomics, so it never touches
    //      the hot path. Started here so it observes the whole run.
    std::atomic<bool> ui_stop{false};
    std::thread t_ui;
    if (dashboard) {
        enable_vt_console();
        t_ui = std::thread(run_dashboard, std::cref(sh), num_events,
                           std::ref(ui_stop), t_start);
    }

    // ---- Collect results (blocks until each worker fulfils its promise) ----
    const LatSummary lat = lat_future.get();
    const analytics::PnLTracker::Stats st = stats_future.get();

    t_feed.join(); t_eng.join(); t_strat.join(); t_risk.join(); t_anal.join();

    // Stop and join the monitor (if running) so its final frame is flushed
    // before we print the textual report underneath it.
    if (dashboard) { ui_stop.store(true, std::memory_order_relaxed); t_ui.join(); }

    const std::uint64_t t_end = core::now_ns();
    const double secs = (t_end - t_start) / 1e9;

    // ---- Report ----
    std::printf("--- throughput ---\n");
    std::printf("  processed %zu feed events in %.3f s  (%.2f M events/s achieved)\n",
                num_events, secs, num_events / secs / 1e6);
    if (rate > 0)
        std::printf("  (feed paced at offered load %.2f M/s; use --rate 0 to measure peak)\n\n",
                    rate / 1e6);
    else
        std::printf("\n");

    std::printf("--- feed->engine hand-off latency (ns) ---\n");
    std::printf("  n=%zu  p50=%llu  p99=%llu  p99.9=%llu  max=%llu  mean=%.1f\n\n",
                lat.n, (unsigned long long)lat.p50, (unsigned long long)lat.p99,
                (unsigned long long)lat.p999, (unsigned long long)lat.max, lat.mean);

    std::printf("--- market-making P&L ---\n");
    std::printf("  our_fills=%llu  final_position=%lld\n",
                (unsigned long long)st.our_fills, (long long)st.final_position);
    std::printf("  final_equity=%.0f (tick-$)  peak=%.0f  max_drawdown=%.0f  sharpe=%.4f\n\n",
                st.final_equity, st.peak_equity, st.max_drawdown, st.sharpe);

    std::printf("--- risk & execution quality ---\n");
    std::printf("  peak_abs_inventory=%lld  (hard cap %lld)\n",
                sh.max_abs_pos.load(), (long long)kHardInventoryCap);
    std::printf("  risk_approved=%lld  risk_rejects=%lld\n",
                sh.risk_approved.load(), sh.risk_rejects.load());
    std::printf("  post_only_rejects=%lld  (stale quotes that would have crossed)\n",
                sh.post_only_rejects.load());
    std::printf("  hard_cap_drops=%lld    (orders stopped by the engine inventory cap)\n",
                sh.hard_cap_drops.load());

    // ---- Optional metrics export for the HTML dashboard ----
    if (!metrics_out.empty()) {
        std::printf("\n--- metrics export ---\n");
        write_metrics_json(metrics_out, topo, clk, num_events, rate, pin, secs, lat, st, sh);
    }
    return 0;
}
