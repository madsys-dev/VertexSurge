#pragma once

// #include "big_operators.hh"
#include "big_operators2.hh"
// #include "big_operators3.hh"

template <BitMatrixTable BT> void convert_BMT_to_Flat(FlatTable *ft, BT *bt) {
  auto its = bt->get_iterators(omp_get_max_threads());
#pragma omp parallel for
  for (auto &it : its) {
    VID r, c;
    while (it.next(r, c)) {
      ft->push_back_single(0, &r);
      ft->push_back_single(1, &c);
    }
  }
}
