#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <tuple>
#include <utility>
/*

| A B |
| C D |

| 0 1 |
| 2 3 |


*/

enum class PeanoDirection {
  RA, // clock-wise start from A, ABDC
  RD, // clock-wise start from D, DCAB
  CA, // counter-clock-wise start from A, ACDB
  CD, // counter-clock-wise start from D, DBAC
};

inline std::array<int, 4> order(PeanoDirection dir) {
  switch (dir) {
  case PeanoDirection::RA:
    return {0, 1, 3, 2};
  case PeanoDirection::RD:
    return {3, 2, 0, 1};
  case PeanoDirection::CA:
    return {0, 2, 3, 1};
  case PeanoDirection::CD:
    return {3, 1, 0, 2};
  };
  assert(0);
}

inline std::array<PeanoDirection, 4> subPeanoDirection(PeanoDirection dir) {
  using PD = PeanoDirection;
  switch (dir) {
  case PD::RA:
    return {PD::CA, PD::RA, PD::RA, PD::CD};
  case PD::RD:
    return {PD::CD, PD::RD, PD::RD, PD::CA};
  case PD::CA:
    return {PD::RA, PD::CA, PD::CA, PD::RD};
  case PD::CD:
    return {PD::RD, PD::CD, PD::CD, PD::RA};
  }
  assert(0);
}

template <typename F, typename A>
void do_by_peano(PeanoDirection pd, F f, std::array<A, 4> args) {
  auto subpd = subPeanoDirection(pd);
  auto ord = order(pd);
  for (int i = 0; i < 4; i++) {
    f(subpd[i], args[ord[i]]);
  }
}

inline size_t round_to_2_power(size_t x) {
  size_t ret = 1;
  while (ret < x) {
    ret <<= 1;
  }
  return ret;
}

// n must be power of 2
inline size_t XYToIndex(size_t n, size_t x, size_t y, PeanoDirection d) {
  if (n == 1) {
    return 0;
  }

  size_t next_n = n / 2;
  size_t next_x = x % next_n;
  size_t next_y = y % next_n;

  size_t idx = x / next_n * 2 + y / next_n;
  auto ord = order(d);

  size_t pre = std::find(ord.begin(), ord.end(), idx) - ord.begin();
  return pre * next_n * next_n +
         XYToIndex(next_n, next_x, next_y, subPeanoDirection(d)[pre]);
}
