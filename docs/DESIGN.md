# quant-mm-sim design

This document explains the implementation choices behind the low-latency market-making simulator. For a broader systems tutorial, see [CONCEPTS.md](CONCEPTS.md); this file focuses on how the repository applies those concepts.

## Design goals

- Keep the hot path explicit, readable and allocation-free after startup.
- Separate market-data replay, matching, strategy, risk and analytics into independently pinned stages.
- Use lock-free queues for one-to-one hand-offs and atomics for the few cross-stage signals that must be globally visible.
- Preserve market-microstructure correctness: integer ticks, price-time priority, post-only quoting and mid-price marking.
- Report both systems metrics and trading metrics, because latency only matters if it changes execution quality and P&L.

## Pipeline overview

`src\main.cpp` wires five stages together:

```text
FEED -> ENGINE -> STRATEGY -> RISK -> ENGINE -> ANALYTICS
```

The directed edges are SPSC rings:

| Ring | Producer | Consumer | Payload |
|---|---|---|---|
| `feed_ring` | Feed | Engine | Market `OrderMsg` plus enqueue TSC |
| `top_ring` | Engine | Strategy | `BookTop` plus enqueue TSC |
| `risk_ring` | Strategy | Risk | Strategy order/cancel intent plus enqueue TSC |
| `order_ring` | Risk | Engine | Approved strategy order/cancel plus enqueue TSC |
| `trade_ring` | Engine | Analytics | `Trade` plus enqueue TSC |

The engine also returns feed-to-engine latency through a `std::promise<LatSummary>`, and analytics returns final P&L through a `std::promise<PnLTracker::Stats>`.

## Per-stage responsibilities

| Stage | Thread role | Inputs | Outputs | Owns / guarantees |
|---|---|---|---|---|
| Feed | Generate deterministic market data and optionally pace it with `--rate`. | `SyntheticFeed` | `feed_ring` | Produces exactly `--events` messages, then sets `feed_done`. |
| Engine | Maintain the live order book, match orders, emit trades and book tops. | `feed_ring`, `order_ring` | `trade_ring`, `top_ring`, atomics | Zero-lag authority for book state, live mid price and position updates from fills. |
| Strategy | Convert throttled top-of-book snapshots into cancel/replace quotes. | `top_ring`, atomic position | `risk_ring` | Maintains active bid/ask ids and keeps quotes passive by construction. |
| Risk | Enforce pre-trade position/loss checks independently from strategy logic. | `risk_ring`, atomic position/equity | `order_ring` | Cancels always pass; new orders can be rejected. |
| Analytics | Attribute fills, mark inventory, compute P&L and risk metrics. | `trade_ring`, atomic mid | atomic equity, final future | Consumes every trade before finalising P&L. |

## Header-only core

`qmm_core` is an INTERFACE CMake target. The core building blocks live in headers under `include\qmm\core` so templates such as `SpscRing<T, Capacity>` and `MpmcRing<T>` can be inlined into the application, benchmarks and tests without duplicate library boundaries. That matters for low-latency code: queue operations should compile down to a small sequence of loads, stores and branches, not an opaque call through a library ABI.

The trade-off is that compile times can grow as the project scales, and implementation details are exposed to consumers. For this simulator, the benefit is worth it: the core code is small, template-heavy and performance-sensitive.

## Integer tick prices

All market prices use `qmm::md::Price`, a signed 64-bit integer number of ticks. The book, strategy and P&L never compare floating-point prices on the hot path. Integer ticks give exact equality and ordering, avoid rounding surprises in matching logic, and map naturally to exchange tick-size grids.

Currency-like values are reported only at the edge as tick-dollars (`price * quantity`). This keeps the simulation deterministic and makes matching and P&L attribution auditable.

## Market-data model

`SyntheticFeed` generates a deterministic order stream from a fixed-seed `std::mt19937_64` RNG. A hidden fair value random-walks; passive limit orders sit around fair value; aggressive limit orders cross; and cancels remove previously generated passive order ids. The deterministic seed is intentional: A/B systems changes can be compared against the same logical tape.

`save_tape_csv` and `load_tape_csv` provide simple CSV persistence for generated tapes. The main executable currently constructs `SyntheticFeed` directly and controls count/rate through `--events` and `--rate`.

## Order book and matching engine

`OrderBook` implements price-time priority:

- `std::map<Price, PriceLevel>` stores each side's sorted price ladder.
- Each `PriceLevel` contains a `std::list<RestingOrder>` for FIFO time priority.
- A `locator_` hash map stores `OrderId -> (side, price, list iterator)` for O(1) cancellation.
- Each level caches `total_qty`, so top-of-book quantity is immediate once the best level is found.

This design optimises clarity and correctness. `std::map` makes best bid/ask and crossing rules easy to inspect, and `std::list` gives stable iterators for cancels. A production HFT book would usually replace the map with a flat, tick-indexed array or sparse array around the active price band, avoid node allocation, compact hot fields, and separate cold metadata. That would reduce pointer chasing and improve cache locality, but it would obscure the microstructure logic in a teaching simulator.

The engine is also the correct place for final post-only and inventory enforcement. It is the only stage with the live book at the instant an approved order arrives, so it can reject stale quotes that would cross via `would_cross()` and stop inventory-growth orders at the hard cap without waiting for delayed strategy/risk feedback.

## Strategy design

`MarketMaker` quotes around top-of-book mid:

- `base_half_spread` sets the initial distance from mid.
- Order-book imbalance shifts both quotes toward short-horizon pressure.
- Inventory skew shifts both quotes to mean-revert inventory.
- The bid is clamped below the best ask and the ask above the best bid so generated quotes remain passive.
- The strategy cancels its prior bid/ask before posting replacements.

The strategy has a soft inventory limit (`max_inventory`) that stops quoting the side that would worsen an already large position. This is deliberately not the final safety mechanism: soft strategy state can lag the engine, so the hard cap lives in the engine.

## Risk controls

`RiskEngine` is intentionally small and auditable. Cancels always pass because they reduce or remove exposure. New orders are rejected if the loss kill switch is latched or if the worst-case full fill would exceed the configured absolute position limit.

The full pipeline adds a second, engine-authoritative hard inventory cap of 600. Strategy and risk read position through atomics, which means their view can be one or more pipeline hops stale. The engine sees every fill first and updates `Shared::position` immediately, so it is the zero-lag authority for the final hard stop.

## P&L and marking

`PnLTracker` updates cash and position only for trades where our order was maker or taker. For buys, position increases and cash decreases; for sells, position decreases and cash increases. Equity is marked as:

```text
equity = cash + position * mid_price
```

The pipeline marks open inventory at the engine-published mid price rather than the last trade price. For a passive maker, the last trade is often an aggressor hitting one side of the book and can be directionally biased. Mid marking gives a cleaner snapshot of inventory value.

Sharpe is computed from sampled equity changes, and max drawdown is tracked from the running equity peak. These are intentionally simple but useful enough to connect execution quality to systems latency.

## SPSC ring design

`SpscRing<T, Capacity>` is the hot-path queue between adjacent stages. Its contract is strict: exactly one producer calls `try_push`, exactly one consumer calls `try_pop`, and capacity is a power of two. One slot is left empty to distinguish full from empty, so usable capacity is `Capacity - 1`.

The key invariants are:

- Producer owns `tail_`, the next slot to write.
- Consumer owns `head_`, the next slot to read.
- The number of queued elements is approximately `(tail - head) & mask`.
- Producer publishes a payload by writing the buffer slot, then storing the new tail with `memory_order_release`.
- Consumer observes that publication by loading tail with `memory_order_acquire`, then reading the payload.

The implementation caches the other side's index: producer keeps `cached_head_`, consumer keeps `cached_tail_`. The producer only reloads the real atomic head when its cache says the queue may be full; the consumer only reloads the real atomic tail when its cache says the queue may be empty. This avoids unnecessary cross-core cache-line traffic in the common case.

`CachePadded<std::atomic<std::size_t>>` places the producer-written and consumer-written indices on separate 64-byte cache lines. Without this, the indices would false-share: each side's write would invalidate the other side's cache line even though they update different variables.

## MPMC ring design

`MpmcRing<T>` implements Dmitry Vyukov's bounded MPMC queue. Each cell has an atomic sequence number that tells producers and consumers whether the slot is empty, full, or waiting for a future lap. Producers claim enqueue tickets through `enqueue_pos_`; consumers claim dequeue tickets through `dequeue_pos_`; both use compare-and-swap loops.

This design is lock-free and suitable for true fan-in/fan-out, but it costs more than SPSC because each operation touches per-cell atomics and may retry CAS under contention. The simulator therefore uses SPSC rings for the one-to-one pipeline hops and keeps MPMC as a reusable core primitive for workloads that actually need many producers or many consumers.

## Cache padding and false sharing

`cache.hpp` defines `kCacheLineSize = 64` and `CachePadded<T>`. The code hard-codes 64 bytes because that is the relevant cache-line size on the target x86-64/AMD EPYC class machines and avoids MSVC warnings around `std::hardware_destructive_interference_size` ABI stability.

Padding is not decorative. In a queue, one thread repeatedly writes a producer index and another repeatedly writes a consumer index. If both variables share one line, MESI coherency bounces that line between cores. Separating them lets each core keep ownership of the line it writes most often.

## Thread pinning, topology and NUMA

`affinity.hpp` discovers physical cores with `GetLogicalProcessorInformationEx(RelationProcessorCore)` and identifies SMT siblings. `main.cpp` pins the five hot stages to the first logical CPU of successive physical cores and boosts thread priority. On the measured AMD EPYC 9V74, the machine has 8 physical cores, 16 logical CPUs, SMT enabled and one NUMA node, so each stage can run on a distinct physical core.

Pinning reduces migration-driven cache misses and scheduler jitter. It does not eliminate all operating-system noise: Windows can still preempt a thread, and feed pacing can create bursts after stalls. That is why the full application can show a 110 ns median hand-off and millisecond tail samples in the same run.

`numa_alloc_onnode()` wraps `VirtualAllocExNuma` for systems where ring buffers or large hot data structures should be allocated on a specific node. On a single-node machine it behaves like normal committed virtual memory, but keeping the abstraction in core makes the intended ownership model explicit.

## Shutdown protocol

Shutdown uses cascading atomic done flags:

1. Feed drains its configured synthetic stream, then sets `feed_done` with release ordering.
2. Engine exits only after `feed_done` is visible and both feed/order inputs remain empty through a settle window, then sets `engine_done`.
3. Strategy exits after `engine_done` and an empty top ring, then sets `strategy_done`.
4. Risk exits after `strategy_done` and an empty risk ring, then sets `risk_done`.
5. Analytics exits after `engine_done` and an empty trade ring, then fulfills the P&L promise.

Each stage drains its input before declaring completion. This matters because losing a late trade would corrupt P&L, and losing a late cancel could leave stale strategy orders resting in the simulated book.

## Latency measurement

Each hand-off wrapper carries a TSC timestamp taken at enqueue time. The engine samples `rdtsc()` when it dequeues feed messages, subtracts the enqueue timestamp, and converts cycles to nanoseconds through `TscClock`, which calibrates TSC against `QueryPerformanceCounter` at startup. Samples are recorded into `LatencyHistogram`, which pre-reserves storage and sorts at the end to report p50, p99, p99.9, max and mean.

The feed-to-engine measurement is intentionally an end-to-end application metric: it includes real scheduling and pacing behavior at the offered load. For pure unloaded queue cost, use a dedicated queue micro-benchmark; do not interpret the full-pipeline p99.9 as the intrinsic SPSC queue latency.

## Trading interpretation

The measured negative P&L is consistent with adverse selection. The market maker is passive and post-only, so it is not paying the spread as a taker; instead, losses occur when stale resting quotes are selected by aggressive flow just before the market moves. The post-only guard counts quotes that would have crossed, while the P&L shows the remaining stale resting exposure that still gets picked off.

The spread sensitivity confirms the mechanism: widening half-spread from 2 to 6 reduced fills and shrank the loss from about -765k to about -430k tick-dollars. That is the simulator's main quant-relevant result: it connects systems tail latency to execution quality and then to P&L.
