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
|   |-- build.ps1               # Finds VS via vswhere, enters vcvars64, builds
|   |-- run-all.ps1             # One command: build, test, run demos, make data.js
|   `-- gen-dashboard-data.ps1  # Packages metrics JSON into docs\data.js
|-- src\
|   `-- main.cpp                # Five-stage pinned pipeline, CLI, TUI, metrics export
|-- include\qmm\
|   |-- core\                   # Cache padding, timing, rings, spinlock, affinity
|   |-- md\                     # Market-data types and deterministic synthetic feed
|   |-- book\                   # Price-time-priority limit order book
|   |-- strategy\               # Market-maker quote logic
|   |-- risk\                   # Position and loss risk gates
|   `-- analytics\              # P&L, Sharpe and drawdown tracking
|-- docs\
|   |-- CONCEPTS.md             # Systems concepts explainer
|   |-- DESIGN.md               # Detailed implementation design
|   |-- dashboard.html          # Self-contained HTML report dashboard (Chart.js)
|   `-- data.js                 # Generated metrics for the HTML dashboard
|-- bench\                      # queue_bench (lock comparison) + affinity_bench
`-- tests\                      # run_tests.cpp: dependency-free unit harness
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
.\build\qmm_sim.exe --events 2000000 --rate 1000000
```

Options:

| Flag | Meaning |
|---|---|
| `--events N` | Number of synthetic market-data messages to generate. Default in `main.cpp` is 2,000,000. |
| `--rate R` | Feed pacing in events/second. `--rate 1000000` offers 1.0 M events/s; `--rate 0` runs unpaced for peak throughput. |
| `--no-pin` | Disable per-stage CPU pinning and priority boost. Useful for A/B comparisons against the Windows scheduler. |
| `--dashboard` | Draw a live, in-place terminal (TUI) dashboard while the run executes: progress, instantaneous/overall throughput, inventory gauge, a P&L sparkline, and the risk/execution counters. |
| `--metrics-out PATH` | Write a JSON metrics document (config, throughput, latency percentile curve, P&L, risk, downsampled equity curve) to `PATH`. Consumed by the HTML dashboard. |

### Dashboards

Two complementary views are included:

- **Live terminal dashboard** — pass `--dashboard` to watch the pipeline in real time (best with a longer run so the frames are visible):

  ```powershell
  .\build\qmm_sim.exe --events 5000000 --rate 500000 --dashboard
  ```

  It redraws in place using ANSI/VT sequences and a Unicode-block P&L sparkline (the console is switched to VT + UTF-8 automatically on Windows).

- **HTML report dashboard** — `docs\dashboard.html` is a single, self-contained, dark-themed page (Chart.js via CDN) that renders KPI cards, the equity curve, the feed→engine tail-latency curve, the lock-comparison charts, and a risk panel. It loads its data from `docs\data.js`, which is generated from the binaries:

  ```powershell
  .\scripts\run-all.ps1        # builds, runs, and packages docs\data.js
  # then open docs\dashboard.html in a browser (works over file:// and GitHub Pages)
  ```

  Under the hood, `run-all.ps1` runs `qmm_sim --metrics-out build\metrics.json` and `queue_bench --json build\bench.json`, then `scripts\gen-dashboard-data.ps1` wraps both into `docs\data.js` (`window.QMM_METRICS` / `window.QMM_BENCH`). Every panel degrades gracefully with a placeholder if `data.js` is missing.

## Benchmarks and tests

The CMake build also produces two benchmarks and a unit-test binary (all built
alongside `qmm_sim`):

```powershell
.\build\queue_bench.exe      # mutex vs spinlock vs lock-free SPSC
.\build\affinity_bench.exe   # pinning, SMT siblings, and false sharing
.\build\qmm_tests.exe        # or: ctest --test-dir build --output-on-failure
```

`queue_bench` measures the true one-way hand-off latency with a **ping-pong**
(only one message in flight, so the number is the pure core-to-core cost, not
backlog residence) and streaming throughput. Representative results on the
AMD EPYC 9V74 (two distinct physical cores):

| Mechanism | latency p50 | latency p99.9 | throughput |
|---|---:|---:|---:|
| `std::mutex` + queue | ~956 ns | ~15,400 ns | ~12 M items/s |
| spinlock + queue | ~155 ns | ~350 ns | ~12 M items/s |
| lock-free SPSC ring | ~125 ns | ~235 ns | ~312 M items/s |

The lock-free ring wins on both the median and, crucially, the tail: neither
thread ever blocks or enters the kernel, so it avoids the sleep/wake syscall
that inflates the mutex tail. `affinity_bench` additionally shows a ~5x
slowdown from false sharing (two counters on one cache line vs padded onto
separate lines).

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

Run: `build\qmm_sim.exe --events 2000000 --rate 1000000` on AMD EPYC 9V74, 8 physical cores / 16 logical CPUs, SMT on, single NUMA node.

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

- Add configurable strategy parameters for half-spread, quote size and skew coefficients.
- Add CSV input as a runtime option so deterministic synthetic tapes and recorded tapes share the same executable path.
- Add ETW/WPA profiling notes for validating thread migration, context switches and core residency during runs.
- Model uninformed (noise) order flow in the synthetic feed so the maker can be profitable in the zero-latency limit, sharpening the latency-vs-P&L study.
