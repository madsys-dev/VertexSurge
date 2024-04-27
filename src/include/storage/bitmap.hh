#pragma once
#include "common.hh"
#include "meta/type.hh"
#include "storage/CSR.hh"
#include "storage/disk_array.hh"
#include "storage/runtime_concepts.hh"
#include <atomic>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <immintrin.h>
#include <type_traits>
#include <utility>

// Assume the new space are default set to 0 when using lseek to extend the file
class MmapBitmap {
  // almost deprecated

  std::filesystem::path path;
  bool valid = true;
  size_t bit_length, v_length;
  uint64_t *bits;
  int fd;

public:
  static const size_t BITSET_SIZE_LOG2 = 6;
  static const size_t BITSET_SIZE = 1 << BITSET_SIZE_LOG2;

  static const size_t ALIGN_SIZE = 512;
  static const size_t ALIGN_BIT_RATIO = ALIGN_SIZE / BITSET_SIZE;

  explicit MmapBitmap(std::filesystem::path path, size_t bit_length,
                      bool overwirte);

  MmapBitmap(const MmapBitmap &) = delete;
  MmapBitmap &operator=(const MmapBitmap &) = delete;

  MmapBitmap(MmapBitmap &&);
  MmapBitmap &operator=(MmapBitmap &&) = delete;

  ~MmapBitmap();

  void set_zero();

  bool get(size_t at);
  void set(size_t at, bool to);

  bool atomic_get(size_t at);
  void atomic_set(size_t at, bool to);
  void prefetch_all();

  size_t count();
  size_t or_count(MmapBitmap &other);

  MmapBitmap &operator|=(MmapBitmap &other);
  MmapBitmap &operator&=(MmapBitmap &other);
  MmapBitmap &operator-=(MmapBitmap &other);
  static size_t dedup_and_update_visit_and_count(MmapBitmap &vis,
                                                 MmapBitmap &out);

  MmapBitmap &flip();

  void clone_from(MmapBitmap &other);

  template <typename F> void for_each(F f) {
    for (size_t i = 0; i < bit_length; i++) {
      if (get(i)) {
        f(i);
      }
    }
  }

  bool has_intersection(MmapBitmap &other);

  inline uint64_t *start_addr() { return bits; }

  // load fully to memory, subsequent change will not be written to disk
  void to_volatile();

  size_t size_in_bytes();

  void debug();
};

class DenseVarStackMatrix;

struct BitmapIterator {
  using V = uint64_t;
  static const size_t V_BITSIZE = sizeof(V) * 8;
  V *start;
  V now_data, now_bit;
  // now_data 101010000, now_bit: 000010000
  size_t now_v_idx, v_idx_max;

  BitmapIterator() = default;
  BitmapIterator(V *start, size_t bit_len) : start(start) {
    v_idx_max = align_to(bit_len, V_BITSIZE) / V_BITSIZE;
    now_v_idx = 0;
    now_bit = 0;
    now_data = start[0];
  }

  // true if out is ok, false if no more out
  inline bool next(VID &out) {
    while (now_v_idx < v_idx_max) {
      if (now_data) {
        out = now_v_idx * 64 + __builtin_ctzll(now_data);
        V b = now_data - 1;
        now_bit = (now_data ^ b) & now_data;
        now_data &= b;
        return true;
      }
      now_v_idx++;
      if (now_v_idx < v_idx_max) {
        now_data = start[now_v_idx];
      }
    }
    return false;
  }

  void set_now_false() { start[now_v_idx] &= (~now_bit); }
};

struct DenseBitMatrixSubRef;

class DenseBitMatrix {
  std::filesystem::path dir;
  size_t row_count, col_count;
  std::vector<MmapBitmap> bitmaps;

public:
  DenseBitMatrix(std::filesystem::path dir);
  // DenseBitMatrix(size_t row_count, size_t col_size);
  DenseBitMatrix(std::filesystem::path dir, size_t row_count, size_t col_size);

  inline size_t get_row_count() { return row_count; }
  inline size_t get_col_count() { return col_count; }

  inline bool get(size_t r, size_t c) { return bitmaps[r].get(c); }
  inline bool get_atomic(size_t r, size_t c) {
    return bitmaps[r].atomic_get(c);
  }

  inline void set(size_t r, size_t c, bool to) { bitmaps[r].set(c, to); }
  inline void set_atomic(size_t r, size_t c, bool to) {
    bitmaps[r].atomic_set(c, to);
  }

  inline MmapBitmap &get_row(size_t r) { return bitmaps[r]; }

  inline void prefetch(size_t r) { bitmaps[r].prefetch_all(); }
  size_t count();

  DenseBitMatrixSubRef whole();

  friend DenseBitMatrixSubRef;

  DenseBitMatrix &operator|=(DenseBitMatrix &other);
  DenseBitMatrix &operator-=(DenseBitMatrix &ohter);
  static size_t dedup_and_update_visit_and_count(DenseBitMatrix &vis,
                                                 DenseBitMatrix &out);

  void set_zero();
  void to_volatile();
  size_t size_in_bytes();
};

struct DenseBitMatrixSubRef {
  const static size_t MIN_ROW_COUNT = 5;

  DenseBitMatrix *matrix;
  size_t row_start, row_end; // [row_start,row_end)
public:
  inline DenseBitMatrixSubRef(){};
  explicit DenseBitMatrixSubRef(DenseBitMatrix &matrix, size_t row_start,
                                size_t row_end);

  inline size_t len() { return row_end - row_start; }

  bool devide_to_size(size_t max_sub_size,
                      std::vector<DenseBitMatrixSubRef> &out);

  bool devide_to(size_t to, std::vector<DenseBitMatrixSubRef> &out);
  bool split(std::pair<DenseBitMatrixSubRef, DenseBitMatrixSubRef> &out);
};

struct DenseStackMatrix {

public:
  using V = uint64_t;
  static const size_t STACK_SIZE_LOG2 = 6;
  static const size_t STACK_SIZE = 1 << STACK_SIZE_LOG2;
  static const size_t COL_ALIGN = 64 / sizeof(V);

private:
  std::filesystem::path dir;
  size_t row_count, col_count, stack_count;
  std::vector<DiskArray<V>> bits;

public:
  DenseStackMatrix(size_t row_count, size_t col_size);
  DenseStackMatrix(std::filesystem::path dir, size_t row_count,
                   size_t col_size);

  inline size_t get_row_count() { return row_count; }
  inline size_t get_col_count() { return col_count; }
  inline size_t get_stack_count() { return stack_count; }

  inline size_t stack_idx(size_t row_idx) { return row_idx >> STACK_SIZE_LOG2; }
  inline size_t stack_offset(size_t row_idx) { return row_idx % STACK_SIZE; }

  inline DiskArray<V> &get_stack_by_row_id(size_t row_idx) {
    return bits[stack_idx(row_idx)];
  }

  inline DiskArray<V> &get_stack(size_t stack_idx) { return bits[stack_idx]; }

  inline bool get(size_t r, size_t c) {
    return get_stack_by_row_id(r)[c] & MASK64[stack_offset(r)];
  }

  inline bool get_atomic(size_t r, size_t c) {
    std::atomic_uint64_t *ap =
        (std::atomic_uint64_t *)&get_stack_by_row_id(r)[c];
    return ap->load(std::memory_order_relaxed) & MASK64[stack_offset(r)];
  }

  inline void set(size_t r, size_t c, bool to) {
    if (to) {
      get_stack_by_row_id(r)[c] |= MASK64[stack_offset(r)];
    } else {
      get_stack_by_row_id(r)[c] &= ~MASK64[stack_offset(r)];
    }
  }

  inline void set_atomic(size_t r, size_t c, bool to) {
    std::atomic_uint64_t *ap =
        (std::atomic_uint64_t *)&get_stack_by_row_id(r)[c];
    if (to)
      ap->fetch_or(MASK64[stack_offset(r)], std::memory_order_relaxed);
    else
      ap->fetch_and(~MASK64[stack_offset(r)], std::memory_order_relaxed);
  }

  DenseStackMatrix &operator|=(DenseStackMatrix &other);
  DenseStackMatrix &operator-=(DenseStackMatrix &other);
  static size_t dedup_and_update_visit_and_count(DenseStackMatrix &vis,
                                                 DenseStackMatrix &out);

  inline size_t size_in_bytes() { return stack_count * col_count * sizeof(V); }
  size_t count();
  void debug();
};
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-attributes"

// Some modifications should be made to make V is 64-bit integer.
struct DenseColMainMatrix {
public:
  using V = uint64_t;
  static const size_t V_BITSIZE_LOG2 = 6;
  static const size_t V_BITSIZE = 1 << V_BITSIZE_LOG2;

  static const size_t V_ALIGN = 64 / sizeof(V);

private:
  std::filesystem::path dir;
  size_t row_count, col_count;

  size_t v_per_col, total_v_length;
  DiskArray<V> bits;

  inline size_t get_idx(size_t r, size_t c) {
    return (c * v_per_col) + (r / 64);
  };

  inline uint64_t *get_v(size_t r, size_t c) { return &bits[get_idx(r, c)]; }

public:
  DenseColMainMatrix(size_t row_count, size_t col_count);
  DenseColMainMatrix(std::filesystem::path dir, size_t row_count,
                     size_t col_count);

  // Read
  inline size_t get_row_count() { return row_count; }
  inline size_t get_col_count() { return col_count; }

  size_t count();
  size_t count_bits_in_col(size_t col_idx);
  bool has_bits_in_col(size_t col_idx);

  bool get(size_t r, size_t c);
  bool get_atomic(size_t r, size_t c);

  size_t col_size_in_bytes();
  void load_to_memory();

  // Write

  void set(size_t r, size_t c, bool to);
  void set_atomic(size_t r, size_t c, bool to);

  DenseColMainMatrix &operator|=(DenseColMainMatrix &other);
  DenseColMainMatrix &operator-=(DenseColMainMatrix &other);
  static size_t dedup_and_update_visit_and_count(DenseColMainMatrix &vis,
                                                 DenseColMainMatrix &out);

  void set_col_to(size_t col_idx, bool to);
  void or_col_from(size_t col_idx, DenseColMainMatrix &other,
                   size_t other_col_idx);
  void and_col_from(size_t col_idx, DenseColMainMatrix &other,
                    size_t other_col_idx);

  // random read, sequential write
  void copy_from_variable_row_mat(DenseVarStackMatrix *mat);

  // Get Ref
  V *get_col_start(size_t col_idx);

  friend DenseVarStackMatrix;
  friend ColMainMatrixTable;
};
#pragma GCC diagnostic pop

class DenseVarStackMatrix {
public:
  using V = uint64_t;
  static const size_t V_BITSIZE_LOG2 = 6;
  static const size_t V_BITSIZE = 1 << V_BITSIZE_LOG2;

  static const size_t V_ALIGN = 64 / sizeof(V);

private:
  // Args
  std::filesystem::path dir;
  size_t row_count, col_count, stack_row_count;

  // Derived
  size_t v_per_stack_col, v_per_stack, stack_count, total_v_aligned;

  // Data
  DiskArray<V> bits;

  inline size_t get_stack_idx(size_t r) { return r / stack_row_count; }
  inline size_t get_in_stack_row_idx(size_t r) { return r % stack_row_count; }
  inline size_t get_v_idx(size_t r, size_t c) {
    return get_stack_idx(r) * v_per_stack + c * v_per_stack_col +
           get_in_stack_row_idx(r) / V_BITSIZE;
  }
  inline size_t get_v512_idx(size_t r, size_t c) {
    return get_v_idx(r, c) / V_ALIGN;
  }

  void set_derived();

public:
  // Will create dir/dense-var-stack-matrix.bin
  // Each stack is a col-major matrix of size (v_per_stack_col,
  // stack_row_count), total v count is aligned by V_ALIGN
  DenseVarStackMatrix(size_t row_count, size_t col_count,
                      size_t stack_row_count);
  DenseVarStackMatrix(std::filesystem::path dir, size_t row_count,
                      size_t col_count, size_t stack_row_count);

  inline size_t get_row_count() { return row_count; }
  inline size_t get_col_count() { return col_count; }

  inline size_t get_stack_row_count() { return stack_row_count; }
  inline size_t get_stack_col_size_in_bytes() { return stack_row_count / 8; }
  inline size_t get_stack_count() { return stack_count; }
  inline size_t get_v_count() { return total_v_aligned; }
  inline size_t get_v512_count() { return total_v_aligned / V_ALIGN; }

  size_t count();
  size_t count_bits_in_col(size_t col_idx);
  bool has_bits_in_col(size_t col_idx);

  bool get(size_t r, size_t c);
  bool get_atomic(size_t r, size_t c);
  void set(size_t r, size_t c, bool to);
  void set_atomic(size_t r, size_t c, bool to);

  DenseVarStackMatrix &operator|=(DenseVarStackMatrix &other);
  DenseVarStackMatrix &operator-=(DenseVarStackMatrix &other);

  static size_t update_visit_and_count(DenseVarStackMatrix &vis,
                                       DenseVarStackMatrix &out, bool dedup);
  static size_t dedup_and_update_visit_and_count(DenseVarStackMatrix &vis,
                                                 DenseVarStackMatrix &out);

  void load_to_memory();

  void prefetch_cols(size_t stack_idx, DenseVarStackMatrix &other, VIDPair *oc,
                     size_t len);

  void prefetch_or_stack_cols_from(size_t stack_idx, DenseVarStackMatrix &other,
                                   VIDPair *oc, size_t len);

  void or_stack_col_from(size_t stack_idx, size_t c, DenseVarStackMatrix &other,
                         size_t oc);

#ifdef PRE_PREFETCH
  void pre_prefetch(size_t stack_idx, DenseVarStackMatrix &other, VIDPair *vp);
  void or_stack_col_from_with_prefetch(size_t stack_idx,
                                       DenseVarStackMatrix &other, VIDPair *vp,
                                       size_t at);
#endif

  void atomic_or_stack_col_from(size_t stack_idx, size_t c,
                                DenseVarStackMatrix &other, size_t oc,
                                std::atomic_uint8_t *cc_array);

  void atomic_or_lock_stack_col_from(size_t stack_idx, size_t c,
                                     DenseVarStackMatrix &other, size_t oc,
                                     std::mutex *lock_array);
  void atomic_v_or_stack_col_from(size_t stack_idx, size_t c,
                                  DenseVarStackMatrix &other, size_t oc);

  friend DenseColMainMatrix;
  friend VarRowMatrixTable;
};

static_assert(BitMatrix<DenseVarStackMatrix>);
