#pragma once

#include "common.hh"
#include "storage/disk_array.hh"
#include <type_traits>

template <typename T>
concept BitMatrix = requires(T &x, size_t r, size_t c, bool s) {
  { x.get_row_count() } -> std::same_as<size_t>;
  { x.get_col_count() } -> std::same_as<size_t>;

  { x.count() } -> std::same_as<size_t>;

  { x.get(r, c) } -> std::same_as<bool>;

  { x.get_atomic(r, c) } -> std::same_as<bool>;

  { x.set(r, c, s) } -> std::same_as<void>;

  { x.set_atomic(r, c, s) } -> std::same_as<void>;

  { T::dedup_and_update_visit_and_count(x, x) } -> std::same_as<size_t>;

  { x |= x } -> std::same_as<T &>;
};

template <typename T>
concept BitMatrixTableIterator = requires(T x, VID &r, VID &c) {
  { x.next(r, c) } -> std::same_as<bool>;
};

template <typename T>
concept BitMatrixTable = requires(T &x, size_t r) {
  typename T::UnderlyingBitMatrix;
  requires BitMatrix<typename T::UnderlyingBitMatrix>;

  typename T::Iterator;
  requires BitMatrixTableIterator<typename T::Iterator>;
  { x.get_iterators(r) } -> std::same_as<std::vector<typename T::Iterator>>;
};

template <typename M>
requires BitMatrix<M> M from_csr_to_M(std::filesystem::path dir, MTCSR &csr,
                                      size_t graph_vertex_count);

template <typename M>
requires BitMatrix<M>
void from_csr_to_M2(MTCSR &csr, M &m);

template <typename M>
requires BitMatrix<M> MTCSR from_M_to_csr(std::filesystem::path dir,
                                          size_t layer_idx, DiskArray<VID> &v,
                                          M &m);

template <typename M>
requires BitMatrix<M>
void from_M_to_csr2(M &m, MTCSR &csr);

template <typename M, typename U>
requires BitMatrix<M> and BitMatrix<U>
void from_M_to_U(M &m, U &u) {
#pragma omp parallel for
  for (size_t i = 0; i < m.get_row_count(); i++) {
    for (size_t j = 0; j < m.get_col_count(); j++) {
      u.set_atomic(i, j, m.get(i, j));
    }
  }
}

// Inplace union [len] matrix in [ms] into ms[0]
template <typename M>
requires BitMatrix<M>
void destructive_fast_union(M **ms, size_t len) {
  if (len == 1) {
    return;
  } else if (len == 2) {
    (*ms[0]) |= (*ms[1]);
  } else {
    destructive_fast_union(ms, len / 2);
    destructive_fast_union(ms + len / 2, len - len / 2);
    (*ms[0]) |= (*ms[len / 2]);
  }
}
