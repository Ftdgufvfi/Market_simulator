#pragma once
// =============================================================================
//  qmm/strategy/market_maker.hpp
// -----------------------------------------------------------------------------
//  A market-making strategy.
//
//  WHAT A MARKET MAKER DOES
//  ------------------------
//  It continuously posts a BID (a buy quote) slightly below fair value and an
//  ASK (a sell quote) slightly above it, hoping to buy low and sell high,
//  earning the SPREAD between them. It provides liquidity and gets paid for the
//  risk of holding inventory.
//
//  THREE SIGNALS SHAPE OUR QUOTES
//  ------------------------------
//   1. SPREAD (base_half_spread): how far from mid we quote. Wider = safer but
//      fewer fills; tighter = more fills but more risk.
//   2. ORDER-BOOK IMBALANCE: if there is far more size on the bid than the ask,
//      the price is more likely to tick UP soon. We skew BOTH quotes up so we
//      are less likely to buy right before a rise (and more likely to sell into
//      it), and vice-versa.
//   3. INVENTORY: if we are already LONG a lot, we want to stop accumulating and
//      encourage selling. We skew both quotes DOWN (cheaper ask to attract
//      buyers, less attractive bid) to mean-revert our position toward flat.
//      This "inventory skew" is what keeps a market maker from blowing up.
//
//  Each tick we CANCEL our previous quotes and post fresh ones ("quote replace"),
//  which is how real market makers keep their quotes current.
// =============================================================================
#include <cmath>

#include "qmm/md/types.hpp"

namespace qmm::strategy {

using namespace qmm::md;

class MarketMaker {
public:
    struct Params {
        Price base_half_spread = 2;    // ticks from mid to each quote
        double imbalance_skew  = 0.5;  // ticks of shift per unit imbalance [-1,1]
        double inventory_skew  = 0.10; // ticks of shift per unit of inventory
        Qty    quote_size      = 50;   // size we post on each side
        Qty    max_inventory   = 500;  // stop quoting a side beyond this |pos|
    };

    // The set of actions the strategy wants to take this tick. Any subset of the
    // four may be active (flags say which). Cancels come first so the book never
    // holds two generations of our quotes at once.
    struct Quotes {
        OrderMsg cancel_bid{};
        OrderMsg cancel_ask{};
        OrderMsg new_bid{};
        OrderMsg new_ask{};
        bool has_cancel_bid = false;
        bool has_cancel_ask = false;
        bool has_new_bid    = false;
        bool has_new_ask    = false;
    };

    explicit MarketMaker(Params p) : p_(p) {}

    // Decide new quotes given the current top of book and our inventory.
    // `inventory` is our signed position (positive = long, negative = short).
    Quotes on_tick(const BookTop& top, Qty inventory, Ts ts) {
        Quotes q;
        if (!top.has_both()) return q;   // need a two-sided market to quote around

        const Price fair = top.mid();

        // --- Combine the signals into a single price shift (in ticks) ---------
        // Imbalance in [-1,1] -> shift quotes toward the likely move direction.
        const double imb_shift = p_.imbalance_skew * top.imbalance();
        // Inventory -> shift quotes to mean-revert our position toward flat.
        // Long (inventory>0) pushes quotes DOWN (negative shift) to sell more.
        const double inv_shift = -p_.inventory_skew * static_cast<double>(inventory);
        const Price shift = static_cast<Price>(std::llround(imb_shift + inv_shift));

        Price bid_px = fair - p_.base_half_spread + shift;
        Price ask_px = fair + p_.base_half_spread + shift;

        // --- Stay PASSIVE: never cross the spread ----------------------------
        // A market maker's job is to PROVIDE liquidity (rest in the book and earn
        // the spread), never to TAKE it (pay the spread as an aggressor). So we
        // clamp our bid to sit strictly below the best ask, and our ask strictly
        // above the best bid. Without this, a large imbalance/inventory shift
        // could lift the opposite quote and turn us into a liquidity taker --
        // systematically buying high and selling low (adverse selection).
        if (bid_px >= top.ask_price) bid_px = top.ask_price - 1;
        if (ask_px <= top.bid_price) ask_px = top.bid_price + 1;

        // Never quote a crossed/locked pair: keep at least one tick between them.
        if (ask_px <= bid_px) ask_px = bid_px + 1;

        // --- Cancel last tick's quotes (if any) -------------------------------
        if (active_bid_id_ != 0) {
            q.cancel_bid = make_cancel(active_bid_id_, ts);
            q.has_cancel_bid = true;
            active_bid_id_ = 0;
        }
        if (active_ask_id_ != 0) {
            q.cancel_ask = make_cancel(active_ask_id_, ts);
            q.has_cancel_ask = true;
            active_ask_id_ = 0;
        }

        // --- Post fresh quotes, respecting inventory limits -------------------
        // Stop BUYING when we are already too long; stop SELLING when too short.
        if (inventory < p_.max_inventory) {
            active_bid_id_ = next_id_++;
            q.new_bid = make_quote(active_bid_id_, Side::Buy, bid_px, ts);
            q.has_new_bid = true;
        }
        if (inventory > -p_.max_inventory) {
            active_ask_id_ = next_id_++;
            q.new_ask = make_quote(active_ask_id_, Side::Sell, ask_px, ts);
            q.has_new_ask = true;
        }
        return q;
    }

    // Our order ids live in a high, disjoint range so they never collide with
    // the feed's ids (which start at 1). 2^40 leaves ample room for both.
    static constexpr OrderId kStratIdBase = (OrderId{1} << 40);

private:
    OrderMsg make_quote(OrderId id, Side side, Price px, Ts ts) const {
        OrderMsg m{};
        m.id = id; m.type = MsgType::Limit; m.side = side;
        m.price = px; m.qty = p_.quote_size; m.ts = ts; m.is_ours = true;
        return m;
    }
    OrderMsg make_cancel(OrderId id, Ts ts) const {
        OrderMsg m{};
        m.id = id; m.type = MsgType::Cancel; m.ts = ts; m.is_ours = true;
        return m;
    }

    Params p_;
    OrderId next_id_ = kStratIdBase;
    OrderId active_bid_id_ = 0;   // id of our currently-resting bid (0 = none)
    OrderId active_ask_id_ = 0;   // id of our currently-resting ask (0 = none)
};

} // namespace qmm::strategy
