#pragma once
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>

// #define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
// #define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO

#include "fmt/core.h"
#include "fmt/ranges.h"
#include "fmt/std.h"
#include "spdlog/common.h"
#include "spdlog/spdlog.h"

#include "fwd.hh"

struct NoType {};

using VID = uint32_t; // Vertex Id Type
// using VID64 = uint64_t;
constexpr VID NO_VERTEX = std::numeric_limits<VID>::max();
using atomic_vid = std::atomic_uint32_t;

using IDX = int64_t; // Index type
using Offset = int64_t;
using DIS = uint8_t; // Distance Type

#define USE_STACK_COLUMN_MAJOR // COLUMN MAJOR IF THIS DIABLED

constexpr bool use_simd = true;
// constexpr bool use_simd = false;

// #define LOCK_BASED
// #define OCC_ARRAY
#define ATOMIC_CC
// #define NO_CC

#define USE_BLOCK_HILBERT // HILBERT IF THIS DISABLED

#ifndef USE_BLOCK_HILBERT
#if defined(NO_CC) + defined(ATOMIC_CC) + defined(OCC_ARRAY) +                 \
        defined(LOCK_BASED) !=                                                 \
    1
#error Only one of the macros should be defined.
#endif
#endif

#define WITH_PREFETCH

#ifdef WITH_PREFETCH

#define BLOCK_PREFETCH
// #define PRE_PREFETCH

#if defined(BLOCK_PREFETCH) + defined(PRE_PREFETCH) != 1
#error Only one of the macros should be defined.
#endif

constexpr size_t BLOCK_PREFETCH_COUNT = 20;

constexpr size_t PRE_PREFETCH_OFFSET = 20;

#endif

#define PURE_MEM

// #define PRINT_EXPAND_COUNT

inline void print_compile_ops() {
#ifdef PURE_MEM
  SPDLOG_INFO("PURE_MEM");
#endif

#ifdef USE_STACK_COLUMN_MAJOR
  SPDLOG_INFO("USE_STACK_COLUMN_MAJOR");
#else
  SPDLOG_INFO("USE_COLUMN_MAJOR");
#endif

#ifdef USE_BLOCK_HILBERT
  SPDLOG_INFO("BLOCK_HILBERT");
#else
#ifdef LOCK_BASED
  SPDLOG_INFO("LOCK_BASED");
#endif

#ifdef OCC_ARRAY
  SPDLOG_INFO("OCC_ARRAY");
#endif

#ifdef ATOMIC_CC
  SPDLOG_INFO("ATOMIC_CC");
#endif

#ifdef NO_CC
  SPDLOG_INFO("NO_CC");
#endif

#endif

  if constexpr (use_simd) {
    SPDLOG_INFO("USE_SIMD");
  } else {
    SPDLOG_INFO("NO_SIMD");
  }

#ifdef WITH_PREFETCH
#ifdef BLOCK_PREFETCH
  SPDLOG_INFO("BLOCK_PREFETCH {}", BLOCK_PREFETCH_COUNT);
#endif

#ifdef PRE_PREFETCH
  SPDLOG_INFO("PRE_PREFETCH {}", PRE_PREFETCH_OFFSET);
#endif

#else
  SPDLOG_INFO("NO_PREFETCH");
#endif

#ifdef PRINT_EXPAND_COUNT
  SPDLOG_INFO("PRINT_EXPAND_COUNT");
#endif
}

constexpr size_t MAX_DIS =
    static_cast<std::size_t>(std::numeric_limits<DIS>::max());

constexpr size_t BITMAP_ADD_BATCH_SIZE = 64;

constexpr size_t CACHELINE_SIZE = 64;
constexpr size_t NUMA_NODE_COUNT = 2;

constexpr size_t SKT_LLCACHE_SIZE = 67.5 * 1024 * 1024;
// constexpr size_t SKT_LLCACHE_SIZE = 18 * 1024 * 1024;
constexpr size_t SKT_L2CACHE_SIZE = 15 * 1024 * 1024;
constexpr size_t SKT_L1DCACHE_SIZE = 0.55 * 1024 * 1024;

constexpr size_t TOTAL_LLCACHE_SIZE = SKT_LLCACHE_SIZE * NUMA_NODE_COUNT;
constexpr size_t TOTAL_L2CACHE_SIZE = SKT_L2CACHE_SIZE * NUMA_NODE_COUNT;
constexpr size_t TOTAL_L1DCACHE_SIZE = SKT_L1DCACHE_SIZE * NUMA_NODE_COUNT;

constexpr double LLCACHE_USAGE_RATIO = 0.8;

constexpr size_t CACHE_ON_USAGE = SKT_LLCACHE_SIZE * LLCACHE_USAGE_RATIO;

using VIDArrayRef = std::pair<VID *, size_t>;

constexpr size_t PAGE_SIZE = 4096;

enum class Direction {
  Forward,
  Backward,
  Both,
  Undecide,
};

// impl libfmt for Direction
template <> struct fmt::formatter<Direction> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const Direction &p, FormatContext &ctx) const
      -> format_context::iterator {
    switch (p) {

    case Direction::Forward:
      return format_to(ctx.out(), "Forward");
    case Direction::Backward:
      return format_to(ctx.out(), "Backward");
    case Direction::Both:
      return format_to(ctx.out(), "Both");
    case Direction::Undecide:
      return format_to(ctx.out(), "Undecide");
    default:
      assert(0);
    }
  }
};

inline std::string to_string(const Direction &dir) {
  switch (dir) {
  case Direction::Forward:
    return "Forward";
  case Direction::Backward:
    return "Backward";
  case Direction::Both:
    return "Both";
  case Direction::Undecide:
    return "Undecide";
  }
  assert(0);
}

inline Direction reverse_direction(Direction dir) {
  switch (dir) {

  case Direction::Forward:
    return Direction::Backward;
  case Direction::Backward:
    return Direction::Forward;
  case Direction::Both:
    return Direction::Both;
  case Direction::Undecide:
    return Direction::Undecide;
  default:
    assert(0);
  }
}

enum class GraphType {
  Analytic,
  Normal,
};

constexpr uint64_t MASK64[64] = {0x1,
                                 0x2,
                                 0x4,
                                 0x8,
                                 0x10,
                                 0x20,
                                 0x40,
                                 0x80,
                                 0x100,
                                 0x200,
                                 0x400,
                                 0x800,
                                 0x1000,
                                 0x2000,
                                 0x4000,
                                 0x8000,
                                 0x10000,
                                 0x20000,
                                 0x40000,
                                 0x80000,
                                 0x100000,
                                 0x200000,
                                 0x400000,
                                 0x800000,
                                 0x1000000,
                                 0x2000000,
                                 0x4000000,
                                 0x8000000,
                                 0x10000000,
                                 0x20000000,
                                 0x40000000,
                                 0x80000000,
                                 0x100000000,
                                 0x200000000,
                                 0x400000000,
                                 0x800000000,
                                 0x1000000000,
                                 0x2000000000,
                                 0x4000000000,
                                 0x8000000000,
                                 0x10000000000,
                                 0x20000000000,
                                 0x40000000000,
                                 0x80000000000,
                                 0x100000000000,
                                 0x200000000000,
                                 0x400000000000,
                                 0x800000000000,
                                 0x1000000000000,
                                 0x2000000000000,
                                 0x4000000000000,
                                 0x8000000000000,
                                 0x10000000000000,
                                 0x20000000000000,
                                 0x40000000000000,
                                 0x80000000000000,
                                 0x100000000000000,
                                 0x200000000000000,
                                 0x400000000000000,
                                 0x800000000000000,
                                 0x1000000000000000,
                                 0x2000000000000000,
                                 0x4000000000000000,
                                 0x8000000000000000};

#include "nlohmann/json.hpp"
// partial specialization (full specialization works too)
namespace nlohmann {
template <typename T> struct adl_serializer<std::optional<T>> {
  static void to_json(json &j, const std::optional<T> &opt) {
    if (opt == std::nullopt) {
      j = nullptr;
    } else {
      j = opt.value(); // this will call adl_serializer<T>::to_json which will
                       // find the free function to_json in T's namespace!
    }
  }

  static void from_json(const json &j, std::optional<T> &opt) {
    if (j.is_null()) {
      opt = std::nullopt;
    } else {
      opt = j.template get<T>(); // same as above, but with
                                 // adl_serializer<T>::from_json
    }
  }
};
} // namespace nlohmann
