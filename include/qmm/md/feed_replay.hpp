#pragma once
// =============================================================================
//  qmm/md/feed_replay.hpp
// -----------------------------------------------------------------------------
//  Produces a realistic stream of market-data messages to drive the simulator.
//
//  Two ways to get a "tape" of events:
//    1. SyntheticFeed  -- generate a reproducible random market on the fly.
//    2. load_tape_csv  -- replay a previously recorded/saved tape from disk.
//
//  WHY SYNTHETIC (AND DETERMINISTIC)?
//  ----------------------------------
//  Real historical order-by-order data is proprietary and huge. For a portfolio
//  project we instead SIMULATE the rest of the market with a simple but sensible
//  model, seeded by a fixed RNG so every run is byte-for-byte reproducible
//  (essential for A/B comparing systems optimisations fairly). The generated
//  tape can also be saved to CSV and "replayed", mirroring how a real firm would
//  replay a captured session.
//
//  THE MARKET MODEL (kept intentionally simple & readable)
//  -------------------------------------------------------
//    * A hidden "fair value" performs a random walk (this is the true price the
//      market is discovering).
//    * Most messages are PASSIVE limit orders posted near the fair value: buys
//      slightly below, sells slightly above -- these build up the order book.
//    * Some messages are AGGRESSIVE limit orders priced to cross the spread --
//      these consume resting liquidity and generate trades.
//    * Occasionally a previously-posted order is CANCELLED, so the book churns
//      like a real one.
// =============================================================================
#include <cstdint>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "qmm/md/types.hpp"

namespace qmm::md {

class SyntheticFeed {
public:
    struct Config {
        std::uint64_t seed        = 42;     // fixed seed => reproducible tape
        std::size_t   num_events  = 1'000'000; // how many messages to generate
        Price         fair_start  = 100'000;// starting fair value, in ticks
        int           vol_ticks   = 2;      // random-walk step size (ticks)
        Qty           avg_size    = 100;    // mean order size
        double        p_aggressive= 0.25;   // fraction of orders that cross
        double        p_cancel    = 0.15;   // fraction of messages that cancel
        Price         max_offset  = 20;     // how far from fair passive orders sit
    };

    explicit SyntheticFeed(Config cfg)
        : cfg_(cfg), rng_(cfg.seed), fair_(cfg.fair_start) {
        live_ids_.reserve(4096);
    }

    // Stream interface: fill `out` with the next message. Returns false once the
    // configured number of events has been produced. Cheap enough to call from
    // the feed thread in the live pipeline.
    bool next(OrderMsg& out) {
        if (produced_ >= cfg_.num_events) return false;
        ++produced_;
        ts_ += 1000; // advance virtual clock ~1us per event (arbitrary, monotone)

        // 1) Evolve the hidden fair value as a small random walk.
        std::uniform_int_distribution<int> walk(-cfg_.vol_ticks, cfg_.vol_ticks);
        fair_ += walk(rng_);
        if (fair_ < 100) fair_ = 100; // keep it positive/sane

        // 2) Maybe cancel a previously posted order (if we have any live).
        std::uniform_real_distribution<double> u(0.0, 1.0);
        if (!live_ids_.empty() && u(rng_) < cfg_.p_cancel) {
            std::uniform_int_distribution<std::size_t> pick(0, live_ids_.size() - 1);
            const std::size_t idx = pick(rng_);
            const OrderId victim = live_ids_[idx];
            // Swap-and-pop removal (O(1)); we don't care about order.
            live_ids_[idx] = live_ids_.back();
            live_ids_.pop_back();

            out = OrderMsg{};
            out.id   = victim;
            out.type = MsgType::Cancel;
            out.ts   = ts_;
            out.is_ours = false;
            return true;
        }

        // 3) Otherwise post a new limit order (passive or aggressive).
        const Side side = (u(rng_) < 0.5) ? Side::Buy : Side::Sell;
        const bool aggressive = u(rng_) < cfg_.p_aggressive;

        std::uniform_int_distribution<Price> off(1, cfg_.max_offset);
        const Price offset = off(rng_);

        Price price;
        if (side == Side::Buy) {
            // Passive buy sits BELOW fair; aggressive buy sits ABOVE fair so it
            // crosses resting sells.
            price = aggressive ? (fair_ + offset) : (fair_ - offset);
        } else {
            // Passive sell sits ABOVE fair; aggressive sell sits BELOW fair.
            price = aggressive ? (fair_ - offset) : (fair_ + offset);
        }

        // Size drawn around the average (never below 1).
        std::poisson_distribution<long long> sz(
            static_cast<double>(cfg_.avg_size));
        Qty qty = static_cast<Qty>(sz(rng_));
        if (qty < 1) qty = 1;

        const OrderId id = next_id_++;
        // Remember passive orders so we can cancel them later (aggressive ones
        // usually trade immediately, so tracking them for cancel is pointless).
        if (!aggressive) live_ids_.push_back(id);

        out = OrderMsg{};
        out.id    = id;
        out.type  = MsgType::Limit;
        out.side  = side;
        out.price = price;
        out.qty   = qty;
        out.ts    = ts_;
        out.is_ours = false;
        return true;
    }

    // Convenience: materialise the entire tape into a vector (for backtests).
    std::vector<OrderMsg> generate_tape() {
        std::vector<OrderMsg> tape;
        tape.reserve(cfg_.num_events);
        OrderMsg m;
        while (next(m)) tape.push_back(m);
        return tape;
    }

    // Feed-generated order ids start at 1. Our strategy uses a disjoint high
    // range (see market_maker.hpp) so ids never collide in the book.
    static constexpr OrderId kFeedIdBase = 1;

private:
    Config cfg_;
    std::mt19937_64 rng_;          // deterministic PRNG
    Price fair_;                   // current hidden fair value (ticks)
    Ts    ts_ = 0;                 // virtual monotonic clock (ns)
    std::size_t produced_ = 0;
    OrderId next_id_ = kFeedIdBase;
    std::vector<OrderId> live_ids_;// posted (passive) orders eligible for cancel
};

// ---- CSV persistence (optional "record & replay") ---------------------------
// Columns: id,type,side,price,qty,ts,is_ours   (type/side as integers)
inline void save_tape_csv(const std::string& path,
                          const std::vector<OrderMsg>& tape) {
    std::ofstream f(path);
    f << "id,type,side,price,qty,ts,is_ours\n";
    for (const auto& m : tape) {
        f << m.id << ',' << static_cast<int>(m.type) << ','
          << static_cast<int>(m.side) << ',' << m.price << ',' << m.qty << ','
          << m.ts << ',' << (m.is_ours ? 1 : 0) << '\n';
    }
}

inline std::vector<OrderMsg> load_tape_csv(const std::string& path) {
    std::vector<OrderMsg> tape;
    std::ifstream f(path);
    std::string line;
    std::getline(f, line); // skip header
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        OrderMsg m{};
        char comma;
        int type, side, is_ours;
        std::istringstream ss(line);
        ss >> m.id >> comma
           >> type >> comma
           >> side >> comma
           >> m.price >> comma
           >> m.qty >> comma
           >> m.ts >> comma
           >> is_ours;
        m.type    = static_cast<MsgType>(type);
        m.side    = static_cast<Side>(side);
        m.is_ours = (is_ours != 0);
        tape.push_back(m);
    }
    return tape;
}

} // namespace qmm::md
