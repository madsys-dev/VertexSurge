#include "operator/intersect.hh"
#include <cassert>
#include <cstddef>
#include <omp.h>
#include <vector>

using namespace std;


template <> void IntersectMatrix<DenseBitMatrix>::do_matrix_intersect() {
  assert(out.get_row_count() == left.get_row_count());
  assert(out.get_col_count() == right.get_row_count());
#pragma omp parallel for collapse(2) schedule(dynamic, 1000)
  for (size_t i = 0; i < left.get_row_count(); i++) {
    for (size_t j = 0; j < right.get_row_count(); j++) {
      if (out.get(i, j)) {
        continue;
      }
      out.set_atomic(i, j, left.get_row(i).has_intersection(right.get_row(j)));
    }
  }
}

template <typename M>
requires BitMatrix<M>
IntersectMatrix<M>::IntersectMatrix(string desc, M &left, M &right, M &out,
                                    IntersectStrategy strategy)
    : left(left), right(right), out(out), strategy(strategy), desc(desc) {}

template <typename M>
requires BitMatrix<M>
void IntersectMatrix<M>::run_internal() {
  switch (strategy) {

  case IntersectStrategy::Adptive: {
    break;
  }
  case IntersectStrategy::CSR: {
    break;
  }
  case IntersectStrategy::BitMatrix: {
    do_matrix_intersect();
    break;
  }
  }
}

template class IntersectMatrix<DenseBitMatrix>;
