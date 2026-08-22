# quant-mm-sim

**A C++20, Windows/MSVC low-latency market-making simulator that connects exchange-style order-book mechanics to the systems effects that drive real trading P&L.**

## What this demonstrates

- A five-stage, thread-per-stage trading pipeline: feed -> engine -> strategy -> risk -> analytics.
- Lock-free SPSC rings on the hot one-producer/one-consumer paths, with cache-line padding and head/tail caching.
- Windows CPU affinity and priority control to keep hot stages on distinct physical cores.
- TSC-based nanosecond latency measurement, calibrated against QueryPerformanceCounter.
- A price-time-priority limit order book with O(1) cancellation through order locators.
- A passive, post-only market maker with spread, imbalance and inventory skew.
- Independent pre-trade risk controls plus an engine-authoritative hard inventory cap.
- P&L attribution, mid-price marking, Sharpe and drawdown reporting.

For the deeper systems primer, see [docs\CONCEPTS.md](docs/CONCEPTS.md). For implementation design and trade-offs, see [docs\DESIGN.md](docs/DESIGN.md).

## Repository layout

```text
quant-mm-sim\
|-- CMakeLists.txt              # C++20 CMake/Ninja build; header-only qmm_core
|-- scripts\
|   `-- build.ps1               # Finds VS via vswhere, enters vcvars64, builds
|-- src\
|   `-- main.cpp                # Five-stage pinned pipeline and CLI
|-- include\qmm\
|   |-- core\                   # Cache padding, timing, rings, spinlock, affinity
|   |-- md\                     # Market-data types and deterministic synthetic feed
|   |-- book\                   # Price-time-priority limit order book
|   |-- strategy\               # Market-maker quote logic
|   |-- risk\                   # Position and loss risk gates
|   `-- analytics\              # P&L, Sharpe and drawdown tracking
|-- docs\
|   |-- CONCEPTS.md             # Systems concepts explainer
|   `-- DESIGN.md               # Detailed implementation design
|-- bench\                      # Optional benchmark targets when sources exist
`-- tests\                      # Optional test target when sources exist
```

## Build

Requirements: Windows, Visual Studio 18 Enterprise with MSVC 19.51, CMake and Ninja. The build script locates Visual Studio with `vswhere`, calls `vcvars64.bat`, configures CMake and builds with Ninja.

```powershell
Set-Location C:\Users\chkarthik\projects\quant-mm-sim
.\scripts\build.ps1
```

Manual equivalent:

```powershell
Set-Location C:\Users\chkarthik\projects\quant-mm-sim
& $env:ComSpec /c 'call "C:\Program Files\Microsoft Visual Studio\18\Enterprise\VC\Auxiliary\Build\vcvars64.bat" && cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release && cmake --build build --config Release'
```

## Run

```powershell
.\build\mmsim.exe --events 2000000 --rate 1000000
```

Options:

| Flag | Meaning |
|---|---|
| `--events N` | Number of synthetic market-data messages to generate. Default in `main.cpp` is 2,000,000. |
| `--rate R` | Feed pacing in events/second. `--rate 1000000` offers 1.0 M events/s; `--rate 0` runs unpaced for peak throughput. |
| `--no-pin` | Disable per-stage CPU pinning and priority boost. Useful for A/B comparisons against the Windows scheduler. |

## Architecture

```text
+---------+  feed_ring   +-----------------+  top_ring   +----------+
|  FEED   |=============>| ENGINE / BOOK   |============>| STRATEGY |
| replay  |   SPSC       | match + top/mid |   SPSC      | quotes   |
+---------+              +-----------------+             +----------+
                               ^      |                       |
                               |      | trade_ring            | risk_in
                               |      v                       v
                         order_ring +-----------+        +----------+
                           SPSC     | ANALYTICS |        |   RISK   |
                                    | P&L/stats |        | limits   |
                                    +-----------+        +----------+
                                          |                  |
                                          `-- promise/future  `-- approved orders
```

The stages communicate through fixed-size lock-free SPSC rings. Shared state that must cross stages immediately, such as position, equity, done flags and live mid price, is published with atomics.

## Measured result: AMD EPYC 9V74

Run: `build\mmsim.exe --events 2000000 --rate 1000000` on AMD EPYC 9V74, 8 physical cores / 16 logical CPUs, SMT on, single NUMA node.

| Area | Metric | Result |
|---|---:|---:|
| Throughput | Offered load | 1.0 M events/s |
| Throughput | Achieved | ~0.95 M events/s |
| Feed -> engine latency | p50 | 110 ns |
| Feed -> engine latency | p99 | ~388,000 ns |
| Feed -> engine latency | p99.9 | ~6.2 ms |
| Feed -> engine latency | max | ~7.7 ms |
| Feed -> engine latency | mean | ~28,000 ns |
| Risk/execution | peak_abs_inventory | ~214, with hard cap 600 |
| Risk/execution | risk_approved | 80,000 |
| Risk/execution | risk_rejects | 0 |
| Risk/execution | post_only_rejects | ~507 |
| P&L | our_fills | ~9,500 |
| P&L | final_position | ~70 |
| P&L | final_equity | ~-765,000 tick-$ |
| P&L | Sharpe | ~-0.15 |

The median hand-off latency is the headline systems result: p50 ~= 110 ns shows the lock-free SPSC feed-to-engine hand-off is doing its job. The high p99 and p99.9 values are not evidence that the queue itself takes milliseconds; they are dominated by Windows scheduling jitter and feed-pacing burstiness in the full five-thread application. For clean unloaded queue latency, use the dedicated queue micro-benchmark (`bench\queue_bench`) rather than the paced end-to-end simulator.

## Adverse-selection finding

The negative P&L is a legitimate result, not a cosmetic problem. In this run the strategy is passive and post-only: it rests quotes in the book and refuses to cross the spread. Under queueing, scheduling latency and feed bursts, some resting quotes become stale; informed aggressive flow then trades against those quotes immediately before the market moves against them.

That is the classic market-making failure mode: **latency -> stale quotes -> adverse selection -> negative P&L**. Widening quotes reduced fills and shrank the measured loss, from roughly -765k tick-$ at half-spread 2 to roughly -430k tick-$ at half-spread 6, which supports adverse selection as the driver. The simulator's value is that it quantifies this relationship instead of only demonstrating fast queues in isolation.

## Future work

- Add a clean queue benchmark source to all branches that documents unloaded SPSC and MPMC latency separately from full-pipeline jitter.
- Add configurable strategy parameters for half-spread, quote size and skew coefficients.
- Add CSV input as a runtime option so deterministic synthetic tapes and recorded tapes share the same executable path.
- Add ETW/WPA profiling notes for validating thread migration, context switches and core residency during runs.
