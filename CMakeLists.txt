# =============================================================================
#  quant-mm-sim : C++ Low-Latency Market-Making Simulator
# =============================================================================
#  Top-level CMake build script.
#
#  Design notes:
#   * The reusable low-latency building blocks live in an INTERFACE (header-only)
#     library called `qmm_core`. Header-only keeps the hot-path code inlinable and
#     lets the app, benchmarks and tests all share the exact same definitions.
#   * We target C++20: MSVC 19.5x supports it fully, and every concept we need
#     (std::atomic, std::jthread, concepts, <bit>) is available without relying on
#     bleeding-edge C++23-only features.
#   * Build with the bundled Ninja generator from a `vcvars64` shell (see
#     scripts/build.ps1 which sets that environment up for you).
# =============================================================================
cmake_minimum_required(VERSION 3.20)

project(quant_mm_sim
        VERSION 0.1.0
        DESCRIPTION "Low-latency market-making simulator"
        LANGUAGES CXX)

# ---- Global language settings ------------------------------------------------
set(CMAKE_CXX_STANDARD 20)          # Use C++20 features (atomics, concepts, <bit>).
set(CMAKE_CXX_STANDARD_REQUIRED ON) # Fail rather than silently downgrade the standard.
set(CMAKE_CXX_EXTENSIONS OFF)       # Prefer portable ISO C++ over compiler extensions.

# Default to a Release build when the user did not specify one. Low-latency code is
# meaningless when measured in a Debug build (bounds checks, no inlining, /Od).
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# ---- MSVC-specific tuning ----------------------------------------------------
if(MSVC)
    # /O2  : maximise speed.   /Oi : intrinsics (needed for __rdtsc).
    # /permissive- : strict standard conformance.   /Zc:__cplusplus : report the
    # real __cplusplus value so feature-test macros behave.   /EHsc : normal C++ EH.
    add_compile_options(/permissive- /Zc:__cplusplus /EHsc /W4)
    # C4324 fires when a struct is padded because of our own alignas(64). That is
    # exactly what we WANT (false-sharing avoidance), so silence this one warning.
    add_compile_options(/wd4324)
    add_compile_options($<$<CONFIG:Release>:/O2>)
    add_compile_options($<$<CONFIG:Release>:/Oi>)
endif()

# ---- Header-only core library ------------------------------------------------
add_library(qmm_core INTERFACE)
target_include_directories(qmm_core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(qmm_core INTERFACE cxx_std_20)

# =============================================================================
#  Executables
#  These are added as their source files come into existence (phase by phase),
#  each guarded by an EXISTS check so the project always configures cleanly even
#  before every module has been written.
# =============================================================================

# --- Main simulator application ---
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp)
    add_executable(qmm_sim src/main.cpp)
    target_link_libraries(qmm_sim PRIVATE qmm_core)
endif()

# --- Benchmarks ---
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/bench/queue_bench.cpp)
    add_executable(queue_bench bench/queue_bench.cpp)
    target_link_libraries(queue_bench PRIVATE qmm_core)
endif()
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/bench/affinity_bench.cpp)
    add_executable(affinity_bench bench/affinity_bench.cpp)
    target_link_libraries(affinity_bench PRIVATE qmm_core)
endif()

# --- Tests ---
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/tests/run_tests.cpp)
    enable_testing()
    add_executable(qmm_tests tests/run_tests.cpp)
    target_link_libraries(qmm_tests PRIVATE qmm_core)
    add_test(NAME qmm_tests COMMAND qmm_tests)
endif()
