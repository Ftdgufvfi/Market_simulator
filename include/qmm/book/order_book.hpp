#pragma once
// =============================================================================
//  qmm/book/order_book.hpp
// -----------------------------------------------------------------------------
//  A price-time-priority limit order book (LOB) with a matching engine.
//
//  This is the core of any exchange. It maintains all resting (unfilled) orders
//  and, when a new order arrives that crosses the spread, it MATCHES it against
//  the best-priced resting orders on the other side, producing trades.
//
//  THE TWO PRIORITY RULES (in order)
//  ---------------------------------
//    1. PRICE priority : the best price trades first. For bids "best" = highest;
//       for asks "best" = lowest.
//    2. TIME priority  : among orders at the same price, the one that arrived
//       first trades first (FIFO). This is why each price level is a queue.
//
//  DATA STRUCTURES (chosen for clarity first, then speed)
//  ------------------------------------------------------
//    * Each SIDE of the book is a std::map<Price, PriceLevel>, i.e. a sorted
//      ladder of price levels. Sorted order gives us the best price in O(log n)
//      (asks: begin(); bids: rbegin()).
//    * Each PriceLevel holds a std::list<RestingOrder> in arrival order (FIFO)
//      for time priority. std::list gives STABLE iterators, which we exploit for
//      O(1) cancellation.
//    * A hash map `locator_` maps OrderId -> exactly where that order lives
//      (side, price, list iterator), so cancel() is O(1) instead of a search.
//    * Each PriceLevel caches its total resting quantity so top-of-book queries
//      are O(1).
//
//  (A production HFT book would replace the std::map ladder with a flat array
//  indexed by price for O(1) level access; we keep the map here because it reads
//  cleanly and is fast enough for a simulator. The design notes call this out.)
// =============================================================================
#include <cstdint>
#include <list>
#include <map>
#include <unordered_map>

#include "qmm/md/types.hpp"

namespace qmm::book {

using namespace qmm::md;

class OrderBook {
public:
    // One resting order sitting in the book.
    struct RestingOrder {
        OrderId id      = 0;
        Qty     qty     = 0;      // remaining (unfilled) quantity
        bool    is_ours = false;  // did our strategy post it?
        Ts      ts      = 0;      // time it was posted (for reference)
    };

    // A single price level: a FIFO queue of orders plus a cached total size.
    struct PriceLevel {
        std::list<RestingOrder> orders;   // front = oldest = time priority
        Qty total_qty = 0;                // sum of orders[].qty (O(1) top-of-book)
    };

    using Ladder = std::map<Price, PriceLevel>;

    // -------------------------------------------------------------------------
    // add_limit : process an incoming limit order.
    // It first MATCHES against the opposite side (generating trades via `sink`),
    // then RESTS any unfilled remainder in the book.
    //
    // `sink` is any callable  void(const Trade&)  -- templated so the compiler
    // can inline it (no std::function overhead on the hot path).
    // -------------------------------------------------------------------------
    template <typename TradeSink>
    void add_limit(const OrderMsg& in, TradeSink&& sink) {
        Qty remaining = in.qty;

        if (in.side == Side::Buy) {
            // A buy crosses asks priced <= our limit, cheapest first.
            while (remaining > 0 && !asks_.empty()) {
                auto best = asks_.begin();          // lowest ask
                if (best->first > in.price) break;  // no longer crosses
                remaining = match_against_level(best, asks_, in, remaining,
                                                std::forward<TradeSink>(sink));
            }
            if (remaining > 0)
                rest_order(bids_, in, remaining);   // rest remainder on the bid side
        } else {
            // A sell crosses bids priced >= our limit, highest first.
            while (remaining > 0 && !bids_.empty()) {
                auto best = std::prev(bids_.end());  // highest bid
                if (best->first < in.price) break;   // no longer crosses
                remaining = match_against_level(best, bids_, in, remaining,
                                                std::forward<TradeSink>(sink));
            }
            if (remaining > 0)
                rest_order(asks_, in, remaining);
        }
    }

    // -------------------------------------------------------------------------
    // would_cross : true if a limit order at (side, price) would match on entry
    // (i.e. act as a liquidity TAKER) rather than rest passively. A market maker
    // running "post-only" uses this to REJECT any quote that would cross, so it
    // never pays the spread as an aggressor -- crucial when quotes are computed
    // from a slightly stale book and the market has since moved.
    // -------------------------------------------------------------------------
    bool would_cross(Side side, Price price) const {
        if (side == Side::Buy)
            return !asks_.empty() && asks_.begin()->first <= price;   // lifts an ask
        return !bids_.empty() && std::prev(bids_.end())->first >= price; // hits a bid
    }

    // -------------------------------------------------------------------------
    // cancel : remove a resting order by id. O(1) via the locator map.
    // Returns true if the order was found and removed.
    // -------------------------------------------------------------------------
    bool cancel(OrderId id) {
        auto loc_it = locator_.find(id);
        if (loc_it == locator_.end()) return false; // unknown / already gone

        const Locator& loc = loc_it->second;
        Ladder& ladder = (loc.side == Side::Buy) ? bids_ : asks_;
        auto lvl_it = ladder.find(loc.price);
        if (lvl_it != ladder.end()) {
            PriceLevel& lvl = lvl_it->second;
            if (loc.order_it->is_ours) --our_resting_;
            lvl.total_qty -= loc.order_it->qty;     // keep cached size correct
            lvl.orders.erase(loc.order_it);         // O(1) list erase
            if (lvl.orders.empty())
                ladder.erase(lvl_it);               // drop empty price level
        }
        locator_.erase(loc_it);
        return true;
    }

    // -------------------------------------------------------------------------
    // top : O(1) snapshot of the best bid/ask (price + size at that level).
    // -------------------------------------------------------------------------
    BookTop top(Ts ts = 0) const {
        BookTop t;
        t.ts = ts;
        if (!bids_.empty()) {
            auto best = std::prev(bids_.end());     // highest bid
            t.bid_price = best->first;
            t.bid_qty   = best->second.total_qty;
        }
        if (!asks_.empty()) {
            auto best = asks_.begin();              // lowest ask
            t.ask_price = best->first;
            t.ask_qty   = best->second.total_qty;
        }
        return t;
    }

    bool has_bid() const { return !bids_.empty(); }
    bool has_ask() const { return !asks_.empty(); }
    std::size_t resting_orders() const { return locator_.size(); }
    // Number of OUR (strategy) orders currently resting in the book. A market
    // maker should keep this tiny (ideally one per side); watching it reveals
    // "working-order" leaks where stale quotes accumulate faster than we cancel.
    std::size_t our_resting() const { return our_resting_; }

private:
    // Where a resting order lives, so we can cancel it in O(1).
    struct Locator {
        Side  side;
        Price price;
        std::list<RestingOrder>::iterator order_it;
    };

    // Match `in` against the orders resting at one price level (FIFO), emitting
    // a Trade per (partial) fill. Returns the still-unfilled incoming quantity.
    template <typename TradeSink>
    Qty match_against_level(Ladder::iterator lvl_it, Ladder& ladder,
                            const OrderMsg& in, Qty remaining,
                            TradeSink&& sink) {
        PriceLevel& lvl = lvl_it->second;
        const Price exec_price = lvl_it->first;     // trades at the MAKER's price

        while (remaining > 0 && !lvl.orders.empty()) {
            RestingOrder& maker = lvl.orders.front();          // oldest first
            const Qty fill = (remaining < maker.qty) ? remaining : maker.qty;

            // Emit the execution.
            Trade tr;
            tr.price          = exec_price;
            tr.qty            = fill;
            tr.aggressor_side = in.side;
            tr.maker_id       = maker.id;
            tr.taker_id       = in.id;
            tr.ts             = in.ts;
            tr.maker_is_ours  = maker.is_ours;
            tr.taker_is_ours  = in.is_ours;
            sink(tr);

            // Update quantities and cached level size.
            remaining      -= fill;
            maker.qty      -= fill;
            lvl.total_qty  -= fill;

            if (maker.qty == 0) {
                // Maker fully filled: remove it from the book.
                if (maker.is_ours) --our_resting_;
                locator_.erase(maker.id);
                lvl.orders.pop_front();
            }
        }

        if (lvl.orders.empty())
            ladder.erase(lvl_it);                   // drop the emptied level
        return remaining;
    }

    // Rest an unfilled remainder as a new resting order on the given ladder.
    void rest_order(Ladder& ladder, const OrderMsg& in, Qty remaining) {
        PriceLevel& lvl = ladder[in.price];         // creates the level if absent
        lvl.orders.push_back(RestingOrder{in.id, remaining, in.is_ours, in.ts});
        lvl.total_qty += remaining;
        if (in.is_ours) ++our_resting_;
        // Record where it lives for O(1) cancellation. std::list iterators are
        // stable, so this iterator stays valid until we erase this order.
        locator_[in.id] = Locator{in.side, in.price, std::prev(lvl.orders.end())};
    }

    Ladder bids_;   // buy orders,  sorted ascending; best (highest) = rbegin()
    Ladder asks_;   // sell orders, sorted ascending; best (lowest)  = begin()
    std::unordered_map<OrderId, Locator> locator_; // id -> location, O(1) cancel
    std::size_t our_resting_ = 0;                  // count of our live orders
};

} // namespace qmm::book
