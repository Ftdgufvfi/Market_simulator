#pragma once
// =============================================================================
//  qmm/analytics/pnl.hpp
// -----------------------------------------------------------------------------
//  Tracks our trading performance: position, cash, profit-and-loss (P&L), and
//  the two headline risk-adjusted metrics quant firms always ask for --
//  the Sharpe ratio and the maximum drawdown.
//
//  HOW P&L IS COMPUTED
//  -------------------
//  We watch every trade the matching engine produces and pick out the ones WE
//  were involved in (as maker or taker). For each such fill:
//     * BUY  : position += qty,  cash -= price * qty   (we spent cash for shares)
//     * SELL : position -= qty,  cash += price * qty   (we received cash)
//  At any moment our MARK-TO-MARKET equity is:
//     equity = cash + position * mid_price
//  i.e. cash on hand plus the value of our open inventory at the current mid.
//  (All values are in "tick-dollars" = ticks * shares; multiply by the tick
//  value to get real currency. We keep integers/ticks to stay exact.)
//
//  SHARPE RATIO
//  ------------
//  Sharpe = mean(returns) / stddev(returns). It measures return per unit of
//  risk (volatility). We sample the equity curve periodically, take the
//  step-to-step changes as "returns", and compute the ratio. Higher = better
//  (steady gains beat the same total gain achieved erratically).
//
//  MAX DRAWDOWN
//  ------------
//  The largest peak-to-trough drop in the equity curve -- the worst loss you
//  would have suffered if you had bought in at the worst moment. Smaller = safer.
// =============================================================================
#include <cmath>
#include <cstdint>
#include <vector>

#include "qmm/md/types.hpp"

namespace qmm::analytics {

using namespace qmm::md;

class PnLTracker {
public:
    struct Stats {
        Qty     final_position = 0;
        double  final_equity   = 0.0;   // tick-dollars
        double  realized_cash  = 0.0;
        std::uint64_t our_fills = 0;
        double  sharpe         = 0.0;
        double  max_drawdown   = 0.0;   // tick-dollars (>= 0)
        double  peak_equity    = 0.0;
    };

    // Process one trade. Updates position & cash only if WE were involved.
    void on_trade(const Trade& t) {
        // Work out OUR side of this trade, if any.
        //  * If we were the taker, our side == the aggressor side.
        //  * If we were the maker, our side == the opposite of the aggressor.
        bool involved = false;
        Side our_side = Side::Buy;
        if (t.taker_is_ours) { involved = true; our_side = t.aggressor_side; }
        else if (t.maker_is_ours) { involved = true; our_side = opposite(t.aggressor_side); }
        if (!involved) return;

        const double notional = static_cast<double>(t.price) *
                                static_cast<double>(t.qty);
        if (our_side == Side::Buy) {
            position_ += t.qty;
            cash_     -= notional;
        } else {
            position_ -= t.qty;
            cash_     += notional;
        }
        ++our_fills_;
    }

    // Sample the equity curve at the current mid price. Call periodically (e.g.
    // once per N events) so Sharpe/drawdown have a time series to work with.
    void mark(Price mid) {
        const double eq = equity(mid);
        equity_curve_.push_back(eq);

        // Running peak / max-drawdown update.
        if (eq > peak_equity_) peak_equity_ = eq;
        const double dd = peak_equity_ - eq;
        if (dd > max_drawdown_) max_drawdown_ = dd;
    }

    Qty    position() const { return position_; }
    double cash() const { return cash_; }
    double equity(Price mid) const {
        return cash_ + static_cast<double>(position_) * static_cast<double>(mid);
    }

    // Compute final statistics from the recorded equity curve.
    Stats finalize(Price last_mid) const {
        Stats s;
        s.final_position = position_;
        s.realized_cash  = cash_;
        s.final_equity   = equity(last_mid);
        s.our_fills      = our_fills_;
        s.max_drawdown   = max_drawdown_;
        s.peak_equity    = peak_equity_;
        s.sharpe         = compute_sharpe();
        return s;
    }

private:
    // Sharpe over the sampled equity curve, using step-to-step P&L changes as
    // the return series. Dimensionless; we scale by sqrt(N) so longer, steadier
    // runs score higher (a simple annualisation-style adjustment).
    double compute_sharpe() const {
        if (equity_curve_.size() < 3) return 0.0;

        std::vector<double> rets;
        rets.reserve(equity_curve_.size());
        for (std::size_t i = 1; i < equity_curve_.size(); ++i)
            rets.push_back(equity_curve_[i] - equity_curve_[i - 1]);

        double mean = 0.0;
        for (double r : rets) mean += r;
        mean /= static_cast<double>(rets.size());

        double var = 0.0;
        for (double r : rets) { const double d = r - mean; var += d * d; }
        var /= static_cast<double>(rets.size());
        const double sd = std::sqrt(var);
        if (sd <= 0.0) return 0.0;

        // Sharpe = mean return per unit of volatility. We report the raw
        // per-sample ratio (dimensionless), which is directly interpretable:
        // higher is better, ~0 is break-even. (We deliberately do NOT multiply
        // by sqrt(N); that would inflate the magnitude and obscure the meaning.)
        return mean / sd;
    }

    Qty    position_ = 0;      // signed inventory
    double cash_     = 0.0;    // realised cash (tick-dollars)
    std::uint64_t our_fills_ = 0;

    std::vector<double> equity_curve_;
    double peak_equity_  = 0.0;
    double max_drawdown_ = 0.0;
};

} // namespace qmm::analytics
