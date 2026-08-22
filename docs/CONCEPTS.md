# quant-mm-sim — Systems Performance Concepts

> **Audience:** A quant/HFT firm reviewer who wants to understand *why* every design decision in this simulator exists, not just *what* it does.  
> **Tone:** Precise but approachable. Analogies before equations.

---

## Table of Contents

1. [Latency vs Throughput — and Why Tail Latency Rules Trading](#1-latency-vs-throughput--and-why-tail-latency-rules-trading)
2. [CPU Cache Hierarchy — L1/L2/L3 and the 64-Byte Cache Line](#2-cpu-cache-hierarchy--l1l2l3-and-the-64-byte-cache-line)
3. [False Sharing — the Silent Lock-Free Killer](#3-false-sharing--the-silent-lock-free-killer)
4. [CPU Affinity and Thread Pinning](#4-cpu-affinity-and-thread-pinning)
5. [Hyper-Threading / SMT — Shared Execution Units](#5-hyper-threadingsmt--shared-execution-units)
6. [NUMA — Non-Uniform Memory Access](#6-numa--non-uniform-memory-access)
7. [Atomics and the C++ Memory Model](#7-atomics-and-the-c-memory-model)
8. [Locks vs Atomics vs Lock-Free — Latency Trade-offs](#8-locks-vs-atomics-vs-lock-free--latency-trade-offs)
9. [SPSC vs MPMC Lock-Free Ring Buffers](#9-spsc-vs-mpmc-lock-free-ring-buffers)
10. [Measuring Time — TSC vs QueryPerformanceCounter](#10-measuring-time--tsc-vs-queryperformancecounter)
11. [std::thread / std::promise / std::future — Pipeline Coordination](#11-stdthread--stdpromise--stdfuture--pipeline-coordination)
12. [Windows Profiling Toolbox](#12-windows-profiling-toolbox)

---

## 1. Latency vs Throughput — and Why Tail Latency Rules Trading

### The Basic Distinction

| Term | Definition | Analogy |
|---|---|---|
| **Throughput** | How many operations per second you sustain | Litres per minute through a pipe |
| **Latency** | How long one operation takes end-to-end | How long it takes one drop to travel the pipe |

A system can have enormous throughput and terrible latency, or low throughput and superb latency. A market-making engine cares primarily about latency: when the market moves, you must re-quote *before* a taker picks you off at a stale price.

### Why Averages Lie

Imagine your quote-to-fill round trip averages 50 µs. That sounds fine — until you see that 0.1 % of the time it spikes to 2 ms. In a 100 000 msg/s feed that is 100 bad events every second. Each spike is a window in which a fast arbitrageur can trade against your stale quote.

**Percentile notation:**

```
p50  = median — 50 % of observations are at or below this value
p90  = 90 % below
p99  = 99 % below
p99.9 = 99.9 % below  ← the "one in a thousand" spike
max  = worst single observation
```

The max and p99.9 are what kill a market-maker. They are caused by:

- OS scheduling jitter (thread gets descheduled mid-path)
- Cache misses (data evicted between events)
- Memory allocation (heap locks, TLB misses)
- NUMA remote access (data on the wrong socket)
- False sharing invalidating cache lines

Every design choice in this simulator is motivated by shrinking p99/p99.9, not the mean.

### How to Think About It

Picture a histogram of latency samples. A good low-latency system looks like a sharp spike near zero with a very thin right tail. A badly designed one has a fat tail stretching to milliseconds — that fat tail is money left on the table.

```
Count
  |
  |  ██
  | ████
  | █████
  | ███████__________           ← fat tail = bad
  +-----------------------------> Latency (µs)
  0  50 100 200    1000
```

---

## 2. CPU Cache Hierarchy — L1/L2/L3 and the 64-Byte Cache Line

### The Memory Wall

DRAM is roughly 100× slower than the CPU. Caches sit between them:

```
   CPU Core
  ┌──────────────────────────────────┐
  │  Registers      ~0.3 ns          │
  │  L1 Data Cache  ~1 ns   32–48 KB │  per core
  │  L2 Cache       ~4 ns  512 KB–1MB│  per core
  └──────────────┬───────────────────┘
                 │
  ┌──────────────▼───────────────────┐
  │  L3 Cache (LLC) ~10-40 ns  32 MB │  shared across all cores
  └──────────────┬───────────────────┘
                 │
  ┌──────────────▼───────────────────┐
  │  DRAM            ~80-100 ns      │  GBs
  └──────────────────────────────────┘
```

*(Approximate figures for AMD EPYC 9V74 class hardware)*

### The Cache Line

The CPU never fetches a single byte. It fetches a **cache line** — always **64 bytes** on x86/x64. This has a huge implication: if you touch one byte, the whole surrounding 64-byte block comes into cache.

**Consequence 1 — Spatial Locality:** Pack related data together. If fields `a` and `b` are in the same cache line, accessing `a` brings `b` "for free".

```cpp
// Good: hot fields packed into one 64-byte line
struct OrderBookLevel {
    double price;    // 8 bytes
    uint64_t qty;    // 8 bytes
    uint32_t count;  // 4 bytes
    // 44 bytes of padding / next fields fit here
};

// Bad: hot field mixed with cold fields forces extra line fetches
struct BadLevel {
    double price;
    char description[128];  // forces 3 extra cache lines just to reach qty
    uint64_t qty;
};
```

**Consequence 2 — Structure-of-Arrays vs Array-of-Structures:**

If your hot loop only reads `price` from 10 000 order levels, storing them as `struct[]` wastes bandwidth pulling in `qty` and `count` you never use. A `prices[]` array puts only prices in cache.

### Why This Dominates Performance

A single L1 hit costs ~4 clock cycles. An L3 miss that goes to DRAM costs ~300 cycles. If your hot path touches one cache-line-sized block of data, it runs at L1 speed. If it randomly touches megabytes, it runs at DRAM speed — 75× slower. No amount of algorithmic cleverness overcomes a cache-unfriendly data layout.

---

## 3. False Sharing — the Silent Lock-Free Killer

### What It Is

False sharing happens when two threads each write to **different variables** that happen to live on the **same 64-byte cache line**. Even though neither thread is accessing the other's variable, the cache coherence protocol forces the line to bounce between cores every write.

### How Cache Coherence Works (MESI Protocol)

Each cache line has a state: Modified, Exclusive, Shared, Invalid. When Core A writes to a line, it acquires it in Modified state. Every other core holding the line receives an invalidation message and must re-fetch from L3 (or from Core A's cache) before they can read or write again.

### The False Sharing Scenario

```
Cache line (64 bytes):
┌─────────────────────────────────────────────────────────────┐
│  [  head (8B)  ][  tail (8B)  ][  ... padding ...          ]│
└─────────────────────────────────────────────────────────────┘
     ↑                  ↑
  Written by         Written by
  Producer Thread    Consumer Thread
```

Even though `head` and `tail` are logically independent, every write to `tail` by the consumer invalidates the line in the producer's L1 cache, and vice versa. The two threads ping-pong a single cache line across cores at memory-bus speed — completely defeating the purpose of having separate variables.

### The Fix: Cache-Line Padding with `alignas(64)`

```
Two separate cache lines:
┌─────────────────────────────────────────────────────────────┐
│  [  head (8B)  ][  padding (56B)                           ]│  line A
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│  [  tail (8B)  ][  padding (56B)                           ]│  line B
└─────────────────────────────────────────────────────────────┘
     ↑                  ↑
  Producer only      Consumer only
```

In C++20:

```cpp
struct alignas(64) RingBufferIndex {
    std::atomic<uint64_t> head{0};
    // Explicit padding to fill the rest of the cache line
    char pad[64 - sizeof(std::atomic<uint64_t>)];
};

// Producer owns one cache line, consumer owns another
RingBufferIndex producer_idx;  // head lives here
RingBufferIndex consumer_idx;  // tail lives here
```

Now the producer writes only to line A; the consumer writes only to line B. They never invalidate each other's lines. This single change can cut ring-buffer latency by 3–5×.

---

## 4. CPU Affinity and Thread Pinning

### The Problem: OS Scheduler Migration

By default, the OS scheduler is free to migrate a thread to any available core at any time. Every migration:

- **Warms up a new L1/L2 cache** — the thread's working set must be re-fetched from L3 or DRAM.
- **Introduces jitter** — the migration itself takes microseconds.
- **Shares cores with competing work** — OS interrupt handlers, other processes.

### The Solution: Pin to a Dedicated Physical Core

Thread pinning locks a thread to one specific logical CPU. The benefits:

- **Cache stays warm** — data fetched in one iteration is still in L1 for the next.
- **No migration cost** — zero scheduler-induced latency spikes.
- **Predictable interrupts** — you can steer hardware interrupts away from pinned cores using the OS interrupt affinity settings.

### On Windows: `SetThreadAffinityMask`

```cpp
#include <windows.h>

void pin_thread_to_core(HANDLE thread_handle, int logical_cpu) {
    DWORD_PTR mask = DWORD_PTR(1) << logical_cpu;
    DWORD_PTR old = SetThreadAffinityMask(thread_handle, mask);
    if (old == 0) {
        // Handle error via GetLastError()
    }
}

// Usage: pin the market-data replay thread to logical CPU 2
pin_thread_to_core(GetCurrentThread(), 2);
```

### Core Assignment Strategy in This Simulator

| Thread | Logical CPU | Reason |
|---|---|---|
| Market-data replayer | 2 | Isolated; no SMT sibling sharing |
| Matching engine | 4 | Isolated |
| Strategy | 6 | Isolated |
| Risk engine | 8 | Isolated |
| Main / stats | 0 | Shares with OS; not latency-critical |

Logical CPUs 2, 4, 6, 8 are chosen to be on separate physical cores (no SMT sibling pairing) — see Section 5.

### Thread Priority

Pinning alone is not enough if another thread can preempt. Pair affinity with elevated thread priority:

```cpp
SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
```

This minimises preemption from lower-priority OS work on the same core.

---

## 5. Hyper-Threading / SMT — Shared Execution Units

### What SMT Does

A physical core has one set of execution units (ALUs, FPUs, load/store ports) but **two register files and two program counters**. The CPU presents these as two logical CPUs to the OS. When logical CPU 0 stalls on a cache miss, the hardware switches to logical CPU 1's instruction stream, hiding the stall latency.

```
Physical Core 0
┌──────────────────────────────────────────────────┐
│  Logical CPU 0 (registers, PC)                   │
│  Logical CPU 1 (registers, PC)                   │
│                                                  │
│  Shared: L1/L2 caches, execution ports, TLB      │
└──────────────────────────────────────────────────┘
Physical Core 1
┌──────────────────────────────────────────────────┐
│  Logical CPU 2                                   │
│  Logical CPU 3                                   │
│  Shared: L1/L2 caches, execution ports, TLB      │
└──────────────────────────────────────────────────┘
```

On the AMD EPYC 9V74 with 8 physical cores / 16 logical: logical CPUs 0 and 1 share physical core 0; logical CPUs 2 and 3 share physical core 1; and so on.

### When SMT Helps

A thread that frequently stalls (memory-bound, I/O-bound, or with long-latency instructions) benefits from having its sibling fill the execution slots. Throughput-oriented workloads often see 20–30 % uplift from SMT.

### When SMT Hurts a Latency-Critical Thread

A latency-critical thread wants **exclusive access** to every execution port. If the sibling thread is also compute-intensive:

- **Execution port contention** — both threads compete for the same ALUs; your hot path stalls waiting for a port to free up.
- **L1/L2 cache pollution** — the sibling's working set evicts your data, causing extra cache misses.
- **Branch predictor pollution** — the shared BTB and return-address stack are dirtied by the sibling's branches.

### Practical Rules for This Simulator

**Good (Isolation):** Pin latency-critical thread to logical CPU 2. Leave logical CPU 3 (its SMT sibling) **idle or pinned to a non-critical thread**. The hot thread gets all execution ports to itself.

**Bad:** Pin two busy hot-path threads to logical CPUs 2 and 3. They fight over shared L1/L2 and execution units.

**Experiment:** To measure the SMT contention effect, run two busy-loop threads: one on CPU 2 and one on CPU 3 (same physical core) vs CPU 2 and CPU 4 (different physical cores). You will typically see p99 latency increase by 30–100 % in the same-core case.

To discover the physical/logical mapping on Windows:

```powershell
Get-CimInstance Win32_Processor | Select-Object NumberOfCores, NumberOfLogicalProcessors
# Or use Sysinternals Coreinfo: coreinfo.exe -c
```

---

## 6. NUMA — Non-Uniform Memory Access

### The Architecture

In multi-socket or large CCX (Core Complex) AMD systems, memory is divided into **NUMA nodes**. Each node's memory is directly attached to a specific set of cores. Accessing local memory is fast; accessing memory on a remote node traverses the inter-connect (AMD Infinity Fabric), adding ~50–100 ns of latency.

```
NUMA Node 0                      NUMA Node 1
┌─────────────────────┐         ┌─────────────────────┐
│  Cores 0–7          │◄────────►  Cores 8–15          │
│  Local DRAM         │  Fabric   Local DRAM           │
│  ~80 ns local       │         │  ~80 ns local        │
│  ~150 ns remote ────┼─────────┼────►                 │
└─────────────────────┘         └─────────────────────┘
```

*(AMD EPYC 9V74 has one socket but multiple CCDs; the Infinity Fabric creates sub-NUMA topology even within one socket.)*

### Why It Matters for Ring Buffers

The producer writes the ring buffer into DRAM. If the consumer runs on a different NUMA node, every read crosses the fabric. For a ring buffer passing 10 million messages/second that is 10 million remote DRAM accesses per second — catastrophic.

### The Fix: Allocate on the Consumer's Node

If the consumer owns the data, allocate the ring buffer on the consumer's NUMA node. The producer's writes go remote (one direction penalty), but the consumer's reads are local (the critical latency path).

```cpp
#include <windows.h>

void* numa_alloc(size_t bytes, ULONG numa_node) {
    return VirtualAllocExNuma(
        GetCurrentProcess(),
        nullptr,
        bytes,
        MEM_RESERVE | MEM_COMMIT,
        PAGE_READWRITE,
        numa_node   // 0 = node 0, 1 = node 1, etc.
    );
}
```

### Discovering NUMA Topology on Windows

```powershell
# Highest NUMA node index
[System.Environment]::GetEnvironmentVariable("NUMBER_OF_NUMA_NODES")

# Detailed topology
Get-CimInstance Win32_MemoryArray
# Or: Sysinternals Coreinfo -n
```

### NUMA-Aware Allocation Strategy

| Object | Allocate on |
|---|---|
| Ring buffer (replayer → engine) | Consumer (engine) node |
| Order book | Strategy/engine node |
| P&L stats | Main-thread node |

---

## 7. Atomics and the C++ Memory Model

### The Problem Atomics Solve

On modern CPUs, writes are not immediately visible to other cores. The CPU may:

- Reorder stores in the store buffer before flushing to cache.
- Reorder loads speculatively.
- A compiler may reorder instructions for optimisation.

Without explicit synchronisation, a producer writing a value and a consumer reading it may observe the write in the wrong order — or never at all.

### `std::atomic` and Memory Orders

C++11 and later provide `std::atomic<T>` with explicit memory ordering:

| Memory Order | Meaning |
|---|---|
| `memory_order_relaxed` | No ordering guarantee; just atomicity of the operation itself |
| `memory_order_acquire` | No loads/stores **after** this in program order can be moved **before** it |
| `memory_order_release` | No loads/stores **before** this in program order can be moved **after** it |
| `memory_order_seq_cst` | Full sequential consistency; most expensive; implicit default |

### The Release/Acquire Handshake

This is the fundamental pattern for lock-free hand-off:

```cpp
// Shared state
std::atomic<bool> ready{false};
int data = 0;

// Producer thread
data = 42;                             // (1) write payload
ready.store(true, std::memory_order_release); // (2) publish

// Consumer thread
while (!ready.load(std::memory_order_acquire)) {} // (3) observe publish
assert(data == 42);                    // (4) safe to read payload
```

**Why it works:**
- The `release` store at (2) guarantees that everything written *before* it (1) is visible to any thread that observes the store.
- The `acquire` load at (3) guarantees that everything written before the corresponding release is visible *after* the load.
- Together they form a **synchronises-with** relationship: (1) happens-before (4).

### `memory_order_relaxed` — When It Is Safe

Relaxed is safe when you only need the atomicity of the operation itself and ordering is provided by other means (e.g., a separately acquired fence). In the SPSC ring buffer, the head and tail indices can use relaxed for the write to *own* index (only one writer), while the read of the *other* index needs at least acquire/release semantics.

```cpp
// Producer writes its own head with release (publishes new slot)
head_.store(next, std::memory_order_release);

// Consumer reads producer's head with acquire
uint64_t h = head_.load(std::memory_order_acquire);

// Consumer writes its own tail with relaxed (no other reader)
tail_.store(next, std::memory_order_relaxed);
```

---

## 8. Locks vs Atomics vs Lock-Free — Latency Trade-offs

### The Spectrum

```
Higher latency, easier to reason about
        │
        ▼
  std::mutex (blocking)
  std::atomic spinlock
  lock-free SPSC ring buffer
        │
        ▼
Lower latency, harder to implement correctly
```

### Why a Mutex Causes Microsecond Jitter

When a thread calls `mutex.lock()` and the mutex is contended:

1. The thread makes a syscall (`WaitForSingleObject` on Windows).
2. The OS puts the thread in a wait queue and runs another thread.
3. When the lock is released, the OS wakes the sleeping thread.
4. The OS scheduler puts the thread on a run queue; it may not run immediately.

Steps 2–4 involve two context switches (~1–3 µs each on modern Windows). Even an **uncontended** mutex involves an atomic CAS plus a potential kernel transition. For a latency-critical path, this is unacceptable.

### Spinlock (Atomic CAS Loop)

A spinlock busy-waits using an atomic compare-and-swap:

```cpp
std::atomic<bool> lock_flag{false};

void lock() {
    bool expected = false;
    while (!lock_flag.compare_exchange_weak(
        expected, true, std::memory_order_acquire)) {
        expected = false;
        _mm_pause(); // x86 PAUSE: reduce power + give SMT sibling time
    }
}

void unlock() {
    lock_flag.store(false, std::memory_order_release);
}
```

No syscall, no context switch. Cost is dominated by the cache line ownership round-trip (~40–200 ns on a warm cache, worse under contention).

### Lock-Free SPSC Ring Buffer

With a single producer and single consumer, no mutual exclusion is needed at all. The producer and consumer each own their own index. The only shared state is the two indices — one written by each side. With cache-line isolation (Section 3) and correct acquire/release ordering (Section 7), the critical path is just:

1. Load the other side's index (acquire) — L1 hit if recently accessed.
2. Write your slot data.
3. Increment your own index (release).

**Expected Latency Comparison (round-trip, one message, same physical machine):**

| Mechanism | Typical Latency | Why |
|---|---|---|
| `std::mutex` (uncontended) | 100–300 ns | Atomic CAS + potential kernel fence |
| `std::mutex` (contended) | 1–3 µs | Context switch + scheduler wakeup |
| Spinlock (atomic CAS) | 100–300 ns | No syscall; cache coherence RTT |
| Lock-free SPSC ring buffer | 20–80 ns | One store + one load; no CAS under no-contention |
| Lock-free MPMC ring buffer | 50–150 ns | Sequence CAS required per slot |

*Values are for modern x86-64 at 3–4 GHz, same socket, warm caches.*

### Why SPSC Is Faster Than MPMC

SPSC has **no contention by definition** — exactly one producer and one consumer. The producer never races with itself; the consumer never races with itself. The only synchronisation needed is the acquire/release on the index. MPMC must also atomically claim a slot with CAS (a potential retry loop), which adds latency under concurrent producers/consumers.

---

## 9. SPSC vs MPMC Lock-Free Ring Buffers

### SPSC Ring Buffer — Core Design

```
Ring buffer of capacity N (must be power of 2 for fast modulo):

  Indices:    head (producer writes next here)
              tail (consumer reads next from here)

  Invariant:  (head - tail) == number of unconsumed items
              Full when (head - tail) == N
              Empty when head == tail

  Slots:      [0][1][2][3]...[N-1]
               ↑                ↑
             tail            head
```

**The "one empty slot" trick:** Some designs keep one slot permanently empty to distinguish full from empty without a separate size counter. When using unsigned wraparound arithmetic with a power-of-two capacity, `head - tail == N` (full) vs `head == tail` (empty) works cleanly without the wasted slot.

```cpp
template<typename T, size_t N>
class SPSCQueue {
    static_assert((N & (N-1)) == 0, "N must be power of 2");

    alignas(64) std::atomic<uint64_t> head_{0};
    alignas(64) std::atomic<uint64_t> tail_{0};
    T slots_[N];

public:
    bool push(const T& item) {
        uint64_t h = head_.load(std::memory_order_relaxed);
        uint64_t t = tail_.load(std::memory_order_acquire); // see consumer's progress
        if (h - t == N) return false; // full
        slots_[h & (N-1)] = item;
        head_.store(h + 1, std::memory_order_release); // publish
        return true;
    }

    bool pop(T& item) {
        uint64_t t = tail_.load(std::memory_order_relaxed);
        uint64_t h = head_.load(std::memory_order_acquire); // see producer's write
        if (h == t) return false; // empty
        item = slots_[t & (N-1)];
        tail_.store(t + 1, std::memory_order_release); // advance
        return true;
    }
};
```

### The Head/Tail Caching Trick

Every `push()` needs to check whether the buffer is full, which requires reading `tail_` — a cache line owned by the consumer thread. Under high throughput, this causes frequent cross-core cache invalidations.

The fix: cache a local copy of the other side's index:

```cpp
alignas(64) std::atomic<uint64_t> head_{0};
alignas(64) uint64_t cached_tail_{0}; // producer's private cache of tail
alignas(64) std::atomic<uint64_t> tail_{0};
alignas(64) uint64_t cached_head_{0}; // consumer's private cache of head

bool push(const T& item) {
    uint64_t h = head_.load(std::memory_order_relaxed);
    if (h - cached_tail_ == N) {
        // Only hit the atomic when we *think* we might be full
        cached_tail_ = tail_.load(std::memory_order_acquire);
        if (h - cached_tail_ == N) return false;
    }
    slots_[h & (N-1)] = item;
    head_.store(h + 1, std::memory_order_release);
    return true;
}
```

When the buffer is not near-full (the common case), the producer never touches the consumer's cache line. Cross-core traffic is dramatically reduced.

### MPMC Ring Buffer — Sequence Numbers

For multiple producers and consumers, each slot carries a **sequence number**:

```cpp
struct Slot {
    alignas(64) std::atomic<uint64_t> sequence;
    T data;
};
```

A producer atomically claims the next head slot by CAS on the sequence number. A consumer does similarly for the tail. The sequence number encodes whether a slot is empty, being written, or ready to read — without needing a global lock.

MPMC is more complex to implement correctly and has higher overhead than SPSC because every push/pop requires at least one CAS. Use SPSC whenever the topology allows it (which it does in our pipeline: each stage has exactly one producer and one consumer).

---

## 10. Measuring Time — TSC vs QueryPerformanceCounter

### Why `std::chrono::high_resolution_clock` Is Not Enough

On Windows, `std::chrono::high_resolution_clock` typically wraps `QueryPerformanceCounter` (QPC). QPC is excellent for general profiling, but for nanosecond-resolution micro-benchmarking of hot paths, you want direct access to the CPU's **Time Stamp Counter** (TSC).

### The TSC — `__rdtsc` / `__rdtscp`

Every x86-64 CPU has a TSC register that increments at a fixed frequency (the "invariant TSC" — guaranteed to tick at a constant rate regardless of CPU frequency scaling on modern processors).

```cpp
#include <intrin.h>  // MSVC

// Non-serialising read (instructions may reorder around it)
uint64_t cycles_fast() {
    return __rdtsc();
}

// Serialising read (RDTSCP + CPUID prevents reordering)
uint64_t cycles_serialised() {
    unsigned int aux;
    uint64_t tsc = __rdtscp(&aux); // aux = processor ID
    // Optionally follow with __cpuid() as a full fence
    return tsc;
}
```

**`__rdtsc` vs `__rdtscp`:**

| | `__rdtsc` | `__rdtscp` |
|---|---|---|
| Serialisation | None — CPU may execute later instructions before the read | Partial — waits for prior instructions to retire |
| Use case | Throughput measurement (start/end of many iterations) | Single-event latency measurement |
| Overhead | ~7 cycles | ~20 cycles |

### Converting Cycles to Nanoseconds

The TSC ticks at a fixed frequency, but you must calibrate it:

```cpp
double tsc_to_ns_ratio() {
    // Use QPC (known-good ns source) to calibrate TSC frequency
    LARGE_INTEGER freq, start_qpc, end_qpc;
    QueryPerformanceFrequency(&freq);

    uint64_t start_tsc = __rdtsc();
    QueryPerformanceCounter(&start_qpc);

    Sleep(100); // sleep 100ms — long enough to amortise overhead

    uint64_t end_tsc = __rdtsc();
    QueryPerformanceCounter(&end_qpc);

    double elapsed_ns = (end_qpc.QuadPart - start_qpc.QuadPart)
                        * 1e9 / freq.QuadPart;
    double elapsed_cycles = static_cast<double>(end_tsc - start_tsc);

    return elapsed_ns / elapsed_cycles; // ns per cycle
}
```

Store this ratio at startup and multiply every `(end_tsc - start_tsc)` by it to get nanoseconds.

### QueryPerformanceCounter (QPC)

QPC is Windows' high-resolution wall-clock. On modern hardware it reads the TSC internally (via HPET fallback only on very old machines). It is:

- **Monotonic and steady** — safe for wall-clock timing.
- **~100 ns overhead per call** on some paths — too slow for sub-microsecond hot paths.
- **Ideal for calibration and offline profiling** where the overhead is acceptable.

### Summary

| Timer | Resolution | Overhead | Use Case |
|---|---|---|---|
| `__rdtsc` | 1 cycle (~0.25 ns) | ~7 cycles | Intra-function hot-path timing |
| `__rdtscp` | 1 cycle | ~20 cycles | Precise single-event latency |
| `QueryPerformanceCounter` | ~100 ns effective | ~100 ns | Calibration, wall-clock, offline stats |
| `std::chrono::steady_clock` | Wraps QPC on Windows | Same as QPC | General non-HFT code |

---

## 11. `std::thread` / `std::promise` / `std::future` — Pipeline Coordination

### The Pipeline Structure

The simulator runs as a chain of stages, each on its own thread:

```
[Replayer]──ring──►[Matching Engine]──ring──►[Strategy]──ring──►[Risk/PnL]
    │                                                                │
    └──────────────────────main thread◄──────────────────────────────┘
                              (waits for final stats)
```

The main thread starts all worker threads and then needs to retrieve the final P&L report when all processing is done — without using shared global variables (which would require locking or atomics and complicate the data flow).

### `std::promise` / `std::future`

A `promise`/`future` pair is a one-shot channel: the worker writes exactly one value; the main thread reads it (blocking until available).

```cpp
#include <future>
#include <thread>

// Worker thread function: processes data, returns a result
PnLReport run_pnl_engine(std::promise<PnLReport> result_promise,
                          SPSCQueue<RiskEvent>& input) {
    PnLReport report{};
    RiskEvent ev;
    while (input.pop(ev) || !done) {
        report.update(ev);
    }
    result_promise.set_value(report); // signal completion
}

// Main thread
std::promise<PnLReport> pnl_promise;
std::future<PnLReport> pnl_future = pnl_promise.get_future();

std::thread pnl_thread(run_pnl_engine,
                        std::move(pnl_promise),
                        std::ref(pnl_queue));

// ... do other work or just wait ...
PnLReport final_report = pnl_future.get(); // blocks until worker calls set_value
pnl_thread.join();
```

**Why not a global variable?**

- A global requires either a mutex (adds latency) or careful atomic ordering.
- `promise`/`future` expresses the intent clearly: "this value is produced exactly once by the worker and consumed exactly once by main."
- It handles exceptions: if the worker throws, `future::get()` re-throws on the main thread.

### `std::async` — Shorthand

For simpler cases, `std::async` creates the thread and future in one call:

```cpp
auto fut = std::async(std::launch::async, compute_stats, std::ref(data));
auto result = fut.get(); // blocks until done
```

### Thread Lifecycle in the Simulator

```
main()
  │
  ├─ creates ring buffers (NUMA-allocated)
  ├─ creates promise/future pairs for each stage's result
  ├─ spawns threads (pinned, priority elevated)
  │     ├─ replayer_thread
  │     ├─ engine_thread
  │     ├─ strategy_thread
  │     └─ risk_thread  ──► sets promise when done
  │
  ├─ signals replayer to start (atomic flag)
  │
  └─ future.get() ──► blocks until risk_thread sets result
       │
       └─ prints final P&L report, joins all threads, exits
```

---

## 12. Windows Profiling Toolbox

Equivalent to the Linux `perf` / `htop` / `numactl` / `taskset` ecosystem — fully available on Windows.

### Windows Performance Recorder / Analyzer (WPR/WPA)

**WPR** (Windows Performance Recorder) captures ETW (Event Tracing for Windows) traces with kernel-level visibility. **WPA** (Windows Performance Analyzer) visualises them.

**What to look for:**

| Symptom | WPA View |
|---|---|
| High context-switch rate on hot threads | *CPU Usage (Precise)* → context switches column |
| Thread migration across cores | *CPU Usage (Precise)* → CPU column changes per thread |
| I-cache and D-cache misses | *Hardware Counters* (requires ETW hardware counter profile) |
| Scheduling jitter | *CPU Usage (Precise)* → time between scheduling events |

```powershell
# Start a trace (CPU + context switches + hardware counters)
wpr -start CPU -start DiskIO -filemode

# Run your simulator here...

# Stop and save
wpr -stop C:\traces\sim_run.etl

# Open in WPA
wpa C:\traces\sim_run.etl
```

### Intel VTune Profiler

VTune is the gold standard for low-latency C++ profiling on x86. Key analyses:

- **Microarchitecture Exploration** — shows IPC (instructions per clock), bad speculation, memory bound %, port utilisation.
- **Memory Access** — identifies cache miss hotspots by line and function.
- **Thread Concurrency** — shows synchronisation wait time, spin time, idle time per thread.
- **Platform** — hardware event counters including LLC misses, DTLB misses, branch mispredicts.

VTune is free for use and integrates with MSVC via the `/Zi` debug info flag.

### Task Manager / Resource Monitor

Quick sanity checks:

- **Task Manager → Performance → CPU → Right-click → "Show kernel times"** — shows how much CPU time is in kernel mode (syscalls, interrupts) vs user mode. A latency-critical thread should be almost entirely user-mode.
- **Resource Monitor → CPU tab** — shows per-thread CPU usage and affinity. Verify pinned threads stay on their assigned cores.
- **Set affinity via Task Manager** — right-click process → Set Affinity (coarse, per-process; use `SetThreadAffinityMask` for per-thread control).

### `coreinfo` (Sysinternals)

```
coreinfo.exe -c   # Cache topology
coreinfo.exe -n   # NUMA topology
coreinfo.exe -s   # SMT / physical core mapping
```

Sample output (abridged):

```
Logical to Physical Processor Map:
*-  Physical Processor 0 (Hyperthreading 2)   ← CPUs 0,1 share core 0
-*  Physical Processor 0
**  Physical Processor 1                       ← CPUs 2,3 share core 1
...
```

Use this to build the affinity map in Section 4.

### `Get-CimInstance` (PowerShell)

```powershell
# Socket and core count
Get-CimInstance Win32_Processor |
  Select-Object Name, NumberOfCores, NumberOfLogicalProcessors

# NUMA node count
(Get-CimInstance Win32_SystemInformation).NumberOfNumaNodes

# Cache sizes
Get-CimInstance Win32_CacheMemory |
  Select-Object Level, InstalledSize
```

### What Good Numbers Look Like

| Metric | Target for Latency-Critical Thread |
|---|---|
| Context switches / sec | < 100 (ideally < 10) |
| IPC (instructions per clock) | > 2.0 |
| LLC miss rate | < 1 % of all cache accesses |
| Kernel time % | < 1 % |
| Thread migration events | 0 (if pinned correctly) |
| p99 ring-buffer round-trip | < 200 ns |

If context switches are high, check for inadvertent blocking calls (memory allocation, file I/O, `printf`, Windows event objects) on the hot path. If LLC miss rate is high, revisit data layout (Section 2) and false sharing (Section 3).

---

## Quick-Reference Cheat Sheet

| Concept | Key Number | Fix When Wrong |
|---|---|---|
| Cache line size | 64 bytes | Pad shared atomics with `alignas(64)` |
| L1 latency | ~1 ns | Keep hot data ≤ 32 KB per core |
| L3 miss → DRAM | ~80–100 ns | Improve locality; prefetch |
| Mutex uncontended | ~100–300 ns | Use SPSC ring buffer |
| Mutex contended | ~1–3 µs | Eliminate shared state |
| TSC read (`rdtsc`) | ~7 cycles | Use for hot-path timing |
| False sharing fix | `alignas(64)` pad | Verify with VTune memory analysis |
| NUMA remote penalty | +50–100 ns | `VirtualAllocExNuma` on consumer node |
| Thread pin (Windows) | `SetThreadAffinityMask` | Verify with Resource Monitor |
| SMT sibling contention | +30–100 % p99 | Leave SMT sibling idle or use different cores |

---

*Document maintained by Intern B — documentation & design. For implementation details see the source files in `src/`.*
