#pragma once
// =============================================================================
//  qmm/risk/risk_engine.hpp
// -----------------------------------------------------------------------------
//  The pre-trade RISK ENGINE: the last gate every one of OUR orders must pass
//  before it reaches the matching engine.
//
//  WHY A SEPARATE RISK LAYER?
//  --------------------------
//  A strategy can be buggy or a market can go haywire. The risk engine is a
//  deliberately simple, independent safety net that enforces hard limits the
//  strategy is NOT allowed to override:
//
//    * POSITION LIMIT  : reject any new order that would push our absolute
//      inventory beyond a cap. (Cancels are always allowed -- reducing exposure
//      must never be blocked.)
//    * LOSS KILL-SWITCH: if our mark-to-market loss breaches a threshold, TRIP
//      the kill switch: from then on, reject ALL new orders (we stop quoting)
//      until a human re-enables trading. This is the "big red button" every
//      trading desk has.
//
//  Keeping this logic tiny, obvious and separate from the strategy is itself a
//  best practice: risk controls should be auditable at a glance.
// =============================================================================
#include "qmm/md/types.hpp"

namespace qmm::risk {

using namespace qmm::md;

class RiskEngine {
public:
    struct Limits {
        Qty    max_position   = 1000;      // max absolute inventory allowed
        double max_loss       = 1.0e7;     // trip kill switch below -max_loss
    };

    explicit RiskEngine(Limits l) : lim_(l) {}

    // Feed the engine the latest position and mark-to-market equity so it can
    // evaluate the loss kill switch. Call whenever P&L is refreshed.
    void update_state(Qty position, double equity) {
        position_ = position;
        equity_   = equity;
        if (equity_ <= -lim_.max_loss)
            halted_ = true;                // kill switch latches ON
    }

    // Decide whether one of our orders may proceed. Returns true if allowed.
    // The rules, in order:
    //   1. Cancels are ALWAYS allowed (they only reduce risk).
    //   2. If the kill switch is tripped, reject every new order.
    //   3. Reject a new order that would grow |position| past the cap.
    bool allow(const OrderMsg& o) {
        if (o.type == MsgType::Cancel) return true;      // rule 1
        if (halted_) { ++rejected_; return false; }      // rule 2

        // rule 3: would this fill (worst case, in full) breach the cap?
        const Qty projected =
            (o.side == Side::Buy) ? position_ + o.qty : position_ - o.qty;
        if (projected > lim_.max_position || projected < -lim_.max_position) {
            ++rejected_;
            return false;
        }
        return true;
    }

    bool halted() const { return halted_; }
    std::uint64_t rejected() const { return rejected_; }

    // Manual controls (a human flattening the desk / re-enabling trading).
    void trip_kill_switch() { halted_ = true; }
    void reset_kill_switch() { halted_ = false; }

private:
    Limits lim_;
    Qty    position_ = 0;
    double equity_   = 0.0;
    bool   halted_   = false;
    std::uint64_t rejected_ = 0;
};

} // namespace qmm::risk
