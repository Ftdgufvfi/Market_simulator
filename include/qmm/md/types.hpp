#pragma once
// =============================================================================
//  qmm/md/types.hpp
// -----------------------------------------------------------------------------
//  The fundamental data types shared across the whole trading pipeline.
//
//  DESIGN CHOICE: INTEGER PRICES ("TICKS"), NEVER FLOAT ON THE HOT PATH
//  -------------------------------------------------------------------
//  Real exchanges quote prices on a discrete grid (the "tick size", e.g. $0.01).
//  We represent every price as a signed integer number of ticks. This buys us:
//    * Exact equality/ordering (no floating-point rounding surprises when
//      comparing or matching prices).
//    * Faster comparisons and array-indexable price levels in the order book.
//  We only convert ticks -> dollars at the very edge, for reporting.
// =============================================================================
#include <cstdint>

namespace qmm::md {

// ---- Scalar aliases (give raw integers meaningful names) --------------------
using Price   = std::int64_t;   // price in integer ticks
using Qty     = std::int64_t;   // quantity / size in shares/lots
using OrderId = std::uint64_t;  // unique id per live order
using Ts      = std::uint64_t;  // timestamp in nanoseconds

// Which side of the book an order rests on / a trade lifts.
enum class Side : std::uint8_t { Buy, Sell };

inline Side opposite(Side s) noexcept {
    return s == Side::Buy ? Side::Sell : Side::Buy;
}

// What an incoming message asks the matching engine to do.
enum class MsgType : std::uint8_t {
    Limit,   // add a new limit order to the book (may cross & trade first)
    Cancel   // remove a resting order by id
};

// -----------------------------------------------------------------------------
// OrderMsg : one instruction flowing INTO the matching engine.
// -----------------------------------------------------------------------------
// Comes from two sources:
//   * the market feed (other participants), and
//   * our own market-making strategy.
// `is_ours` lets downstream P&L attribute fills to our strategy vs the market.
struct OrderMsg {
    OrderId id    = 0;
    MsgType type  = MsgType::Limit;
    Side    side  = Side::Buy;
    Price   price = 0;      // limit price in ticks (ignored for Cancel)
    Qty     qty   = 0;      // size (ignored for Cancel)
    Ts      ts    = 0;      // event timestamp (ns)
    bool    is_ours = false;// true if our strategy submitted it
};

// -----------------------------------------------------------------------------
// Trade : one execution produced by the matching engine.
// -----------------------------------------------------------------------------
// A trade happens when an incoming aggressor order crosses a resting maker
// order. We record who was who so P&L knows whether WE were the maker (we
// earned the spread) or the taker (we paid it), or uninvolved.
struct Trade {
    Price   price          = 0;   // execution price (the maker's price)
    Qty     qty            = 0;   // executed size
    Side    aggressor_side = Side::Buy; // side of the incoming (taker) order
    OrderId maker_id       = 0;   // resting order that was hit
    OrderId taker_id       = 0;   // incoming order that hit it
    Ts      ts             = 0;
    bool    maker_is_ours  = false;
    bool    taker_is_ours  = false;
};

// A compact top-of-book snapshot the strategy consumes to make decisions.
struct BookTop {
    Price bid_price = 0;   // best (highest) bid price in ticks
    Qty   bid_qty   = 0;   // total size resting at the best bid
    Price ask_price = 0;   // best (lowest) ask price in ticks
    Qty   ask_qty   = 0;   // total size resting at the best ask
    Ts    ts        = 0;

    bool has_both() const noexcept { return bid_qty > 0 && ask_qty > 0; }

    // Mid price in ticks (integer average; fine for signal purposes).
    Price mid() const noexcept { return (bid_price + ask_price) / 2; }

    // Spread in ticks.
    Price spread() const noexcept { return ask_price - bid_price; }

    // Order-book imbalance in [-1, +1]: >0 means more buyers (bid-heavy),
    // <0 means more sellers. A classic short-horizon predictor of the next
    // mid-price move, and a key input to our market-making quotes.
    double imbalance() const noexcept {
        const double b = static_cast<double>(bid_qty);
        const double a = static_cast<double>(ask_qty);
        const double denom = b + a;
        return denom > 0.0 ? (b - a) / denom : 0.0;
    }
};

} // namespace qmm::md
