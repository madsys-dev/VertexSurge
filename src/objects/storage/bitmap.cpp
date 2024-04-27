
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <immintrin.h>
#include <ios>
#include <linux/stat.h>
#include <omp.h>
#include <popcntintrin.h>
#include <storage/bitmap.hh>
#include <string>
#include <utils/timer.hh>

#include "storage/disk_array.hh"
#include "utils.hh"

using namespace std;

MmapBitmap::MmapBitmap(filesystem::path path, size_t bit_length, bool overwrite)
    : path(path), bit_length(bit_length),
      v_length((bit_length + BITSET_SIZE - 1) / BITSET_SIZE) {
  v_length =
      (v_length + ALIGN_BIT_RATIO - 1) / ALIGN_BIT_RATIO * ALIGN_BIT_RATIO;
  auto [bits, fd] = bigArrayWithMmap<uint64_t>(path, v_length, overwrite);
  this->bits = bits;
  this->fd = fd;
  assert(v_length * sizeof(*bits) * 8 >= bit_length);
}

MmapBitmap::MmapBitmap(MmapBitmap &&other) {
  path = other.path;
  bit_length = other.bit_length;
  v_length = other.v_length;
  bits = other.bits;
  fd = other.fd;
  other.valid = false;
}

MmapBitmap::~MmapBitmap() {
  if (valid) {
    // cout << __func__ << " " << path << endl;
    bigArrayUnmap(bits, fd, v_length);
  }
}

bool MmapBitmap::get(size_t at) {
  size_t v_at = at >> BITSET_SIZE_LOG2;
  size_t bit_at = at & (BITSET_SIZE - 1);
  return bits[v_at] & MASK64[bit_at];
}

void MmapBitmap::set(size_t at, bool to) {
  size_t v_at = at >> BITSET_SIZE_LOG2;
  size_t bit_at = at & (BITSET_SIZE - 1);
  // cout << "at v_at bit_at: " << at << " " << v_at << " " << bit_at << endl;
  if (to) {
    bits[v_at] |= MASK64[bit_at];
  } else {
    bits[v_at] &= ~MASK64[bit_at];
  }
}

bool MmapBitmap::atomic_get(size_t at) {
  size_t byte_at = at >> BITSET_SIZE_LOG2;
  size_t bit_at = at & (BITSET_SIZE - 1);
  atomic_uint64_t *ap = reinterpret_cast<atomic_uint64_t *>(&bits[byte_at]);
  return ap->load(memory_order_relaxed) & MASK64[bit_at];
}

void MmapBitmap::atomic_set(size_t at, bool to) {
  size_t byte_at = at >> BITSET_SIZE_LOG2;
  size_t bit_at = at & (BITSET_SIZE - 1);
  atomic_uint64_t *ap = reinterpret_cast<atomic_uint64_t *>(&bits[byte_at]);
  if (to) {
    ap->fetch_or(MASK64[bit_at], memory_order_relaxed);
  } else {
    ap->fetch_and(~MASK64[bit_at], memory_order_relaxed);
  }
}

void MmapBitmap::prefetch_all() {
  if (madvise(bits, v_length * sizeof(*bits), MADV_WILLNEED) == -1) {
    perror("madvise");
    exit(1);
  }
  for (size_t i = 0; i < v_length; i += CACHELINE_SIZE / sizeof(*bits)) {
    __builtin_prefetch(bits + i, 0, 0);
  }
}

size_t MmapBitmap::count() {
  size_t re = 0;
  __m512i *p = reinterpret_cast<__m512i *>(bits);
  for (size_t i = 0; i < v_length / ALIGN_BIT_RATIO; i++) {
    auto x = _mm512_popcnt_epi64(p[i]);
    re += _mm512_reduce_add_epi64(x);
  }
  return re;
}

size_t MmapBitmap::or_count(MmapBitmap &other) {
  size_t re = 0;
  __m512i *p = reinterpret_cast<__m512i *>(bits);
  __m512i *q = reinterpret_cast<__m512i *>(other.bits);
  for (size_t i = 0; i < v_length / ALIGN_BIT_RATIO; i++) {
    auto x = _mm512_popcnt_epi64(_mm512_or_si512(p[i], q[i]));
    re += _mm512_reduce_add_epi64(x);
  }
  return re;
}

void MmapBitmap::set_zero() {
  __m512i *p = reinterpret_cast<__m512i *>(bits);
  for (size_t i = 0; i < v_length / ALIGN_BIT_RATIO; i++) {
    p[i] = _mm512_setzero_si512();
  }
}

MmapBitmap &MmapBitmap::operator|=(MmapBitmap &other) {
  assert(bit_length == other.bit_length);
  __m512i *p = reinterpret_cast<__m512i *>(bits);
  __m512i *q = reinterpret_cast<__m512i *>(other.bits);
  for (size_t i = 0; i < v_length / ALIGN_BIT_RATIO; i++) {
    p[i] = _mm512_or_si512(p[i], q[i]);
  }
  return *this;
}

MmapBitmap &MmapBitmap::operator&=(MmapBitmap &other) {
  assert(bit_length == other.bit_length);
  __m512i *p = reinterpret_cast<__m512i *>(bits);
  __m512i *q = reinterpret_cast<__m512i *>(other.bits);
  for (size_t i = 0; i < v_length / ALIGN_BIT_RATIO; i++) {
    p[i] = _mm512_and_si512(p[i], q[i]);
  }
  return *this;
}

MmapBitmap &MmapBitmap::operator-=(MmapBitmap &other) {
  assert(bit_length == other.bit_length);
  __m512i *p = reinterpret_cast<__m512i *>(bits);
  __m512i *q = reinterpret_cast<__m512i *>(other.bits);
  for (size_t i = 0; i < v_length / ALIGN_BIT_RATIO; i++) {
    auto x = _mm512_and_si512(p[i], q[i]);
    p[i] = _mm512_xor_si512(p[i], x);
  }
  return *this;
}

size_t MmapBitmap::dedup_and_update_visit_and_count(MmapBitmap &vis,
                                                    MmapBitmap &out) {
  size_t vis_cnt = 0;
  assert(vis.bit_length == out.bit_length);
  __m512i *v = reinterpret_cast<__m512i *>(vis.bits);
  __m512i *o = reinterpret_cast<__m512i *>(out.bits);
  for (size_t i = 0; i < vis.v_length / ALIGN_BIT_RATIO; i++) {
    auto x = _mm512_and_si512(v[i], o[i]);
    o[i] = _mm512_xor_si512(o[i], x);
    v[i] = _mm512_or_si512(v[i], o[i]);
    vis_cnt += popcnt512(&v[i]);
  }
  return vis_cnt;
}

MmapBitmap &MmapBitmap::flip() {
  __m512i *p = reinterpret_cast<__m512i *>(bits);
  for (size_t i = 0; i < v_length / ALIGN_BIT_RATIO; i++) {
    p[i] = _mm512_xor_si512(p[i], _mm512_set1_epi64(-1));
  }
  return *this;
}

void MmapBitmap::clone_from(MmapBitmap &other) {
  assert(bit_length == other.bit_length);
  memcpy(bits, other.bits, v_length * sizeof(*bits));
}

bool MmapBitmap::has_intersection(MmapBitmap &other) {
  assert(bit_length == other.bit_length);
  __m512i *p = reinterpret_cast<__m512i *>(bits);
  __m512i *q = reinterpret_cast<__m512i *>(other.bits);
  for (size_t i = 0; i < v_length / ALIGN_BIT_RATIO; i++) {
    __m512i x = _mm512_and_si512(p[i], q[i]);
    if (_mm512_test_epi64_mask(x, x)) {
      return true;
    }
  }
  return false;
}

void MmapBitmap::to_volatile() {
  assert(valid);

  valid = false;
  uint64_t *new_bits = new uint64_t[v_length];
  for (size_t i = 0; i < v_length; i++) {
    new_bits[i] = bits[i];
  }
  bigArrayUnmap(bits, fd, v_length);
  bits = new_bits;
}

size_t MmapBitmap::size_in_bytes() { return v_length * sizeof(*bits); }

void MmapBitmap::debug() {
  cout << "Bit Length: " << bit_length << endl;
  cout << "V Length: " << v_length << endl;
  cout << "Count: " << count() << endl;
  if (count() < 10) {
    for_each([](size_t x) { cout << x << " "; });
  }
  cout << endl;
}

size_t DenseBitMatrix::count() {
  atomic_size_t re(0);
#pragma omp parallel for
  for (size_t i = 0; i < row_count; i++) {
    re.fetch_add(bitmaps[i].count(), std::memory_order_relaxed);
  }
  return re.load(std::memory_order_seq_cst);
}

void DenseBitMatrix::to_volatile() {
  for (size_t i = 0; i < row_count; i++) {
    bitmaps[i].to_volatile();
  }
}

void DenseBitMatrix::set_zero() {
#pragma omp parallel for
  for (size_t i = 0; i < row_count; i++) {
    bitmaps[i].set_zero();
  }
}

DenseBitMatrixSubRef DenseBitMatrix::whole() {
  return DenseBitMatrixSubRef(*this, 0, row_count);
}

DenseBitMatrix &DenseBitMatrix::operator|=(DenseBitMatrix &other) {
  assert(row_count == other.row_count);
  assert(col_count == other.col_count);
#pragma omp parallel for
  for (size_t i = 0; i < row_count; i++) {
    bitmaps[i] |= other.bitmaps[i];
  }
  return *this;
}

DenseBitMatrix &DenseBitMatrix::operator-=(DenseBitMatrix &other) {
  assert(row_count == other.row_count);
  assert(col_count == other.col_count);
#pragma omp parallel for
  for (size_t i = 0; i < row_count; i++) {
    bitmaps[i] -= other.bitmaps[i];
  }
  return *this;
}

size_t DenseBitMatrix::dedup_and_update_visit_and_count(DenseBitMatrix &vis,
                                                        DenseBitMatrix &out) {
  atomic_size_t re(0);
#pragma omp parallel for
  for (size_t i = 0; i < vis.get_row_count(); i++) {
    size_t x = MmapBitmap::dedup_and_update_visit_and_count(vis.get_row(i),
                                                            out.get_row(i));
    re.fetch_add(x, memory_order_relaxed);
  }
  return re;
}

DenseBitMatrix::DenseBitMatrix(filesystem::path dir) : dir(dir) {
  if (filesystem::exists(dir) == false) {
    filesystem::create_directory(dir);
  }
}

// DenseBitMatrix::DenseBitMatrix(size_t row_count,
//                                size_t col_count)
//     :  row_count(row_count), col_count(col_count) {
//   this->row_count = row_count;
//   this->col_count = col_count;
//   for (size_t i = 0; i < row_count; i++) {
//     bitmaps.emplace_back( col_count );
//   }
// }

DenseBitMatrix::DenseBitMatrix(filesystem::path dir, size_t row_count,
                               size_t col_count)
    : dir(dir), row_count(row_count), col_count(col_count) {
  if (filesystem::exists(dir) == false) {
    filesystem::create_directory(dir);
  }
  this->row_count = row_count;
  this->col_count = col_count;
  for (size_t i = 0; i < row_count; i++) {
    std::filesystem::path xp = dir / ("row-" + to_string(i) + ".bin");
    bitmaps.emplace_back(xp, col_count, true);
  }
}

size_t DenseBitMatrix::size_in_bytes() {
  return row_count * bitmaps[0].size_in_bytes();
}

DenseBitMatrixSubRef::DenseBitMatrixSubRef(DenseBitMatrix &matrix,
                                           size_t row_start, size_t row_end)
    : matrix(&matrix), row_start(row_start), row_end(row_end) {}

bool DenseBitMatrixSubRef::devide_to_size(
    size_t max_sub_size, std::vector<DenseBitMatrixSubRef> &out) {
  size_t start = row_start, end = start;

  while (start < row_end) {
    end += max_sub_size;
    end = min(end, row_end);
    out.emplace_back(*matrix, start, end);
    start = end;
  }
  return true;
}

bool DenseBitMatrixSubRef::devide_to(size_t to,
                                     std::vector<DenseBitMatrixSubRef> &out) {
  if (len() < to * MIN_ROW_COUNT) {
    return false;
  }

  size_t start = row_start, end = start;
  size_t sub_size = (row_end - row_start) / to;
  size_t remain = (row_end - row_start) % to;
  while (start < row_end) {
    end += sub_size;
    if (remain > 0) {
      end++;
      remain--;
    }
    end = min(end, row_end);
    out.emplace_back(*matrix, start, end);
    start = end;
  }
  return true;
}

bool DenseBitMatrixSubRef::split(
    std::pair<DenseBitMatrixSubRef, DenseBitMatrixSubRef> &out) {
  if (len() < 2 * MIN_ROW_COUNT) {
    return false;
  }
  size_t mid = (row_start + row_end) / 2;
  out.first = DenseBitMatrixSubRef(*matrix, row_start, mid);
  out.second = DenseBitMatrixSubRef(*matrix, mid, row_end);
  return true;
}

DenseStackMatrix::DenseStackMatrix(size_t row_count, size_t col_count)
    : row_count(row_count), col_count(col_count) {

  stack_count = align_to(row_count, STACK_SIZE) / STACK_SIZE;
  for (size_t i = 0; i < stack_count; i++) {
    bits.emplace_back(align_to(col_count, COL_ALIGN));
  }
}

DenseStackMatrix::DenseStackMatrix(filesystem::path dir, size_t row_count,
                                   size_t col_count)
    : dir(dir), row_count(row_count), col_count(col_count) {
  if (filesystem::exists(dir) == false) {
    filesystem::create_directory(dir);
  }

  stack_count = align_to(row_count, STACK_SIZE) / STACK_SIZE;
  for (size_t i = 0; i < stack_count; i++) {
    std::filesystem::path xp = dir / ("stack-" + to_string(i) + ".bin");
    bits.emplace_back(xp, align_to(col_count, COL_ALIGN), true);
  }
}

DenseStackMatrix &DenseStackMatrix::operator|=(DenseStackMatrix &other) {
  assert(this->get_row_count() == other.get_row_count());
  assert(this->get_col_count() == other.get_col_count());

#pragma omp parallel for
  for (size_t i = 0; i < stack_count; i++) {
    for (size_t j = 0; j < col_count; j++) {
      bits[i][j] |= other.bits[i][j];
    }
  }

  return *this;
}

DenseStackMatrix &DenseStackMatrix::operator-=(DenseStackMatrix &other) {
  assert(this->get_row_count() == other.get_row_count());
  assert(this->get_col_count() == other.get_col_count());

#pragma omp parallel for
  for (size_t i = 0; i < stack_count; i++) {
    for (size_t j = 0; j < col_count; j++) {
      auto intersect = bits[i][j] & other.bits[i][j];
      bits[i][j] ^= intersect;
    }
  }

  return *this;
}

size_t
DenseStackMatrix::dedup_and_update_visit_and_count(DenseStackMatrix &vis,
                                                   DenseStackMatrix &out) {
  assert(vis.get_row_count() == out.get_row_count());
  assert(vis.get_col_count() == out.get_col_count());
  vector<size_t> vis_cnts(omp_get_max_threads(), 0);

#pragma omp parallel for
  for (size_t i = 0; i < vis.stack_count; i++) {
    for (size_t j = 0; j < vis.col_count; j++) {
      auto x = vis.bits[i][j] & out.bits[i][j];
      out.bits[i][j] ^= x;
      vis.bits[i][j] |= out.bits[i][j];
      vis_cnts[omp_get_thread_num()] += _mm_popcnt_u64(vis.bits[i][j]);
    }
  }

  size_t vis_cnt = 0;
  for (auto x : vis_cnts) {
    vis_cnt += x;
  }
  return vis_cnt;
}

size_t DenseStackMatrix::count() {
  vector<size_t> res(stack_count);
  assert(reinterpret_cast<uint64_t>(&bits[0][0]) % 64 == 0);
#pragma omp parallel for
  for (size_t i = 0; i < stack_count; i++) {
    __m512i *p = reinterpret_cast<__m512i *>(&bits[i][0]);
    assert(reinterpret_cast<uint64_t>(p) % 64 == 0);
    for (size_t j = 0; j < align_to(col_count, COL_ALIGN) / COL_ALIGN; j++) {
      auto x = _mm512_popcnt_epi64(p[j]);
      res[i] += _mm512_reduce_add_epi64(x);
    }
  }

  size_t re = 0;
  for (auto &x : res) {
    re += x;
  }
  return re;
}

void DenseStackMatrix::debug() {
  for (size_t r = 0; r < row_count; r++) {
    for (size_t c = 0; c < col_count; c++) {
      if (get(r, c)) {
        cout << r << "," << c << endl;
      }
    }
  }
}

template <>
DenseStackMatrix from_csr_to_M<DenseStackMatrix>(std::filesystem::path dir,
                                                 MTCSR &csr,
                                                 size_t graph_vertex_count) {
  DenseStackMatrix re(dir, csr.get_vertex_count(), graph_vertex_count);
#pragma omp parallel for
  for (size_t rs = 0; rs < csr.get_vertex_count(); rs += re.STACK_SIZE) {
    for (size_t r = rs; r < min(rs + re.STACK_SIZE, csr.get_vertex_count());
         r++) {
      auto [data, len] = csr.get_data(r);
      for (size_t k = 0; k < len; k++) {
        re.set(r, data[k], true);
      }
    }
  }
  return re;
}

template <typename M>
requires BitMatrix<M>
void from_csr_to_M2(MTCSR &csr, M &m) {
#pragma omp parallel for
  for (size_t r = 0; r < csr.get_vertex_count(); r++) {
    auto [data, len] = csr.get_data(r);
    for (size_t k = 0; k < len; k++) {
      m.set_atomic(r, data[k], true);
    }
  }
}

template void from_csr_to_M2<DenseStackMatrix>(MTCSR &csr, DenseStackMatrix &m);
template void from_csr_to_M2<DenseColMainMatrix>(MTCSR &csr,
                                                 DenseColMainMatrix &m);

template <>
DenseColMainMatrix
from_csr_to_M<DenseColMainMatrix>(std::filesystem::path dir, MTCSR &csr,
                                  size_t graph_vertex_count) {
  DenseColMainMatrix re(dir, csr.get_vertex_count(), graph_vertex_count);
#pragma omp parallel for
  for (size_t r = 0; r < csr.get_vertex_count(); r++) {
    auto [data, len] = csr.get_data(r);
    for (size_t k = 0; k < len; k++) {
      re.set_atomic(r, data[k], true);
    }
  }
  return re;
}

template <> void from_csr_to_M2(MTCSR &csr, DenseVarStackMatrix &m) {
#pragma omp parallel for
  for (size_t r = 0; r < csr.get_vertex_count(); r++) {
    auto [data, len] = csr.get_data(r);
    for (size_t k = 0; k < len; k++) {
      m.set_atomic(r, data[k], true);
    }
  }
}

template <typename M>
requires BitMatrix<M> MTCSR from_M_to_csr(std::filesystem::path dir,
                                          size_t layer_idx, DiskArray<VID> &v,
                                          M &m) {
  MTCSR re(dir, layer_idx, v.size(), omp_get_max_threads(), true);
#pragma omp parallel for
  for (size_t r = 0; r < m.get_row_count(); r++) {
    auto pusher = re.get_data_pusher(r);
    for (size_t c = 0; c < m.get_col_count(); c++) {
      if (m.get(r, c)) {
        pusher.push(c);
      }
    }
  }
  return re;
}

template <typename M>
requires BitMatrix<M>
void from_M_to_csr2(M &m, MTCSR &csr) {
#pragma omp parallel for
  for (size_t r = 0; r < m.get_row_count(); r++) {
    auto pusher = csr.get_data_pusher(r);
    for (size_t c = 0; c < m.get_col_count(); c++) {
      if (m.get(r, c)) {
        pusher.push(c);
      }
    }
  }
}

template MTCSR from_M_to_csr<DenseStackMatrix>(std::filesystem::path dir,
                                               size_t layer_idx,
                                               DiskArray<VID> &v,
                                               DenseStackMatrix &m);
template MTCSR from_M_to_csr<DenseColMainMatrix>(std::filesystem::path dir,
                                                 size_t layer_idx,
                                                 DiskArray<VID> &v,
                                                 DenseColMainMatrix &m);
template MTCSR from_M_to_csr<DenseVarStackMatrix>(std::filesystem::path dir,
                                                  size_t layer_idx,
                                                  DiskArray<VID> &v,
                                                  DenseVarStackMatrix &m);

template void from_M_to_csr2<DenseStackMatrix>(DenseStackMatrix &m, MTCSR &csr);
template void from_M_to_csr2<DenseColMainMatrix>(DenseColMainMatrix &m,
                                                 MTCSR &csr);
template void from_M_to_csr2<DenseVarStackMatrix>(DenseVarStackMatrix &m,
                                                  MTCSR &csr);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-attributes"

DenseColMainMatrix::DenseColMainMatrix(size_t row_count, size_t col_count)
    : row_count(row_count), col_count(col_count) {
  v_per_col = align_to(row_count, V_BITSIZE) / V_BITSIZE;
  v_per_col = align_to(v_per_col, V_ALIGN);

  total_v_length = v_per_col * col_count;
  SPDLOG_DEBUG("V every col {}, total v {}", v_per_col, total_v_length);

#ifdef PURE_MEM

  bits = DiskArray<V>(total_v_length, DiskArrayMode::Memory);
#else
  bits = DiskArray<V>(total_v_length, DiskArrayMode::Anonymous);

#endif
}

DenseColMainMatrix::DenseColMainMatrix(filesystem::path dir, size_t row_count,
                                       size_t col_count)
    : dir(dir), row_count(row_count), col_count(col_count) {
  if (filesystem::exists(dir) == false) {
    filesystem::create_directory(dir);
  }
  v_per_col = align_to(row_count, V_BITSIZE) / V_BITSIZE;
  v_per_col = align_to(v_per_col, V_ALIGN);

  total_v_length = v_per_col * col_count;
  SPDLOG_DEBUG("V every col {}, total v {}", v_per_col, total_v_length);
  bits = DiskArray<V>(dir / "bits", total_v_length, true);
}
#pragma GCC diagnostic pop

size_t DenseColMainMatrix::count() {
  if constexpr (use_simd) {

    vector<size_t> res(omp_get_max_threads(), 0);
    __m512i *avx = reinterpret_cast<__m512i *>(&bits[0]);
#pragma omp parallel for
    for (size_t i = 0; i < total_v_length / V_ALIGN; i++) {
      res[omp_get_thread_num()] += popcnt512(&avx[i]);
    }
    size_t re = 0;
    for (auto r : res) {
      re += r;
    }
    return re;
  } else {
    vector<size_t> res(omp_get_max_threads(), 0);
    V *avx = &bits[0];
#pragma omp parallel for
    for (size_t i = 0; i < total_v_length; i++) {
      res[omp_get_thread_num()] += std::popcount(avx[i]);
    }
    size_t re = 0;
    for (auto r : res) {
      re += r;
    }
    return re;
  }
}

size_t DenseColMainMatrix::count_bits_in_col(size_t col_idx) {
  if constexpr (use_simd) {
    vector<size_t> res(omp_get_max_threads(), 0);
    __m512i *avx = reinterpret_cast<__m512i *>(&bits[col_idx * v_per_col]);
#pragma omp parallel for
    for (size_t i = 0; i < v_per_col / V_ALIGN; i++) {
      res[omp_get_thread_num()] += popcnt512(&avx[i]);
    }
    size_t re = 0;
    for (auto r : res) {
      re += r;
    }
    return re;
  } else {
    vector<size_t> res(omp_get_max_threads(), 0);
    V *avx = &bits[col_idx * v_per_col];
#pragma omp parallel for
    for (size_t i = 0; i < v_per_col; i++) {
      res[omp_get_thread_num()] += std::popcount(avx[i]);
    }
    size_t re = 0;
    for (auto r : res) {
      re += r;
    }
    return re;
  }
}

bool DenseColMainMatrix::has_bits_in_col(size_t col_idx) {
  if constexpr (use_simd) {
    __m512i zero = _mm512_setzero_si512();
    __m512i *avx = reinterpret_cast<__m512i *>(&bits[col_idx * v_per_col]);
    for (size_t i = 0; i < v_per_col / V_ALIGN; i++) {
      if (_mm512_cmpeq_epi64_mask(avx[i], zero) != MASK8ALL1) {
        return true;
      }
    }
    return false;
  } else {
    V *avx = &bits[col_idx * v_per_col];
    for (size_t i = 0; i < v_per_col; i++) {
      if (avx[i] != 0) {
        return true;
      }
    }
    return false;
  }
}

bool DenseColMainMatrix::get(size_t r, size_t c) {
  return *get_v(r, c) & MASK64[r % 64];
}

bool DenseColMainMatrix::get_atomic(size_t r, size_t c) {
  return reinterpret_cast<atomic_uint64_t *>(get_v(r, c))
             ->load(std::memory_order_relaxed) &
         MASK64[r % 64];
}

void DenseColMainMatrix::set(size_t r, size_t c, bool to) {
  if (to) {
    *get_v(r, c) |= MASK64[r % 64];
  } else {
    *get_v(r, c) &= ~MASK64[r % 64];
  }
}

void DenseColMainMatrix::set_atomic(size_t r, size_t c, bool to) {
  if (to) {
    reinterpret_cast<atomic_uint64_t *>(get_v(r, c))
        ->fetch_or(MASK64[r % 64], std::memory_order_relaxed);
  } else {
    reinterpret_cast<atomic_uint64_t *>(get_v(r, c))
        ->fetch_and(~MASK64[r % 64], std::memory_order_relaxed);
  }
}

DenseColMainMatrix &DenseColMainMatrix::operator-=(DenseColMainMatrix &other) {
  assert(row_count == other.row_count);
  assert(col_count == other.col_count);

  if constexpr (use_simd) {
    __m512i *p = reinterpret_cast<__m512i *>(&bits[0]);
    __m512i *q = reinterpret_cast<__m512i *>(&other.bits[0]);

#pragma omp parallel for
    for (size_t i = 0; i < total_v_length / V_ALIGN; i++) {
      auto x = _mm512_and_si512(p[i], q[i]);
      p[i] = _mm512_xor_si512(p[i], x);
    }

    return *this;
  } else {
    V *p = &bits[0];
    V *q = &other.bits[0];

#pragma omp parallel for
    for (size_t i = 0; i < total_v_length; i++) {
      auto x = (p[i] & q[i]);
      p[i] = (p[i] ^ x);
    }
    return *this;
  }
}

DenseColMainMatrix &DenseColMainMatrix::operator|=(DenseColMainMatrix &other) {
  assert(row_count == other.row_count);
  assert(col_count == other.col_count);

  if constexpr (use_simd) {
    __m512i *p = reinterpret_cast<__m512i *>(&bits[0]);
    __m512i *q = reinterpret_cast<__m512i *>(&other.bits[0]);

#pragma omp parallel for
    for (size_t i = 0; i < total_v_length / V_ALIGN; i++) {
      p[i] = _mm512_or_si512(p[i], q[i]);
    }

    return *this;
  } else {
    V *p = &bits[0];
    V *q = &other.bits[0];

#pragma omp parallel for
    for (size_t i = 0; i < total_v_length; i++) {
      p[i] = p[i] | q[i];
    }

    return *this;
  }
}

size_t
DenseColMainMatrix::dedup_and_update_visit_and_count(DenseColMainMatrix &vis,
                                                     DenseColMainMatrix &out) {
  assert(vis.total_v_length == out.total_v_length);
  if constexpr (use_simd) {
    vector<size_t> vis_cnts(omp_get_max_threads(), 0);
    __m512i *vis_b = reinterpret_cast<__m512i *>(&vis.bits[0]);
    __m512i *out_b = reinterpret_cast<__m512i *>(&out.bits[0]);
#pragma omp parallel for
    for (size_t i = 0; i < vis.total_v_length / V_ALIGN; i++) {
      auto x = _mm512_and_si512(vis_b[i], out_b[i]);
      out_b[i] = _mm512_xor_si512(out_b[i], x);
      vis_b[i] = _mm512_or_si512(vis_b[i], out_b[i]);
      vis_cnts[omp_get_thread_num()] += popcnt512(&vis_b[i]);
    }

    size_t re = 0;
    for (auto r : vis_cnts) {
      re += r;
    }
    return re;
  } else {
    vector<size_t> vis_cnts(omp_get_max_threads(), 0);
    V *vis_b = &vis.bits[0];
    V *out_b = &out.bits[0];
#pragma omp parallel for
    for (size_t i = 0; i < vis.total_v_length; i++) {
      auto x = vis_b[i] & out_b[i];
      out_b[i] = out_b[i] ^ x;
      vis_b[i] = vis_b[i] | out_b[i];
      vis_cnts[omp_get_thread_num()] += std::popcount(vis_b[i]);
    }

    size_t re = 0;
    for (auto r : vis_cnts) {
      re += r;
    }
    return re;
  }
}

void DenseColMainMatrix::set_col_to(size_t col_idx, bool to) {
  if constexpr (use_simd) {

    __m512i *p = reinterpret_cast<__m512i *>(&bits[col_idx * v_per_col]);

    if (to) {
      for (size_t i = 0; i < v_per_col / V_ALIGN; i++) {
        p[i] = _mm512_set1_epi64(~0LL);
      }
    } else {
      for (size_t i = 0; i < v_per_col / V_ALIGN; i++) {
        p[i] = _mm512_setzero_si512();
      }
    }
  } else {
    V *p = &bits[col_idx * v_per_col];

    if (to) {
      for (size_t i = 0; i < v_per_col; i++) {
        p[i] = 0;
        p[i] = ~p[i];
      }
    } else {
      for (size_t i = 0; i < v_per_col; i++) {
        p[i] = 0;
      }
    }
  }
}

void DenseColMainMatrix::or_col_from(size_t c, DenseColMainMatrix &other,
                                     size_t oc) {
  if (use_simd) {
    __m512i *me_c = reinterpret_cast<__m512i *>(&bits[c * v_per_col]);
    __m512i *other_c =
        reinterpret_cast<__m512i *>(&other.bits[oc * other.v_per_col]);

    for (size_t i = 0; i < v_per_col / V_ALIGN; i++) {
      me_c[i] = _mm512_or_si512(me_c[i], other_c[i]);
    }
  } else {
    V *me_c = &bits[c * v_per_col];
    V *other_c = &other.bits[oc * other.v_per_col];

    for (size_t i = 0; i < v_per_col; i++) {
      me_c[i] = me_c[i] | other_c[i];
    }
  }
}

void DenseColMainMatrix::and_col_from(size_t col_idx, DenseColMainMatrix &other,
                                      size_t other_col_idx) {
  if constexpr (use_simd) {
    __m512i *me_c = reinterpret_cast<__m512i *>(&bits[col_idx * v_per_col]);
    __m512i *other_c = reinterpret_cast<__m512i *>(
        &other.bits[other_col_idx * other.v_per_col]);
    for (size_t i = 0; i < v_per_col / V_ALIGN; i++) {
      me_c[i] = _mm512_and_si512(me_c[i], other_c[i]);
    }
  } else {
    V *me_c = &bits[col_idx * v_per_col];
    V *other_c = &other.bits[other_col_idx * other.v_per_col];
    for (size_t i = 0; i < v_per_col; i++) {
      me_c[i] = me_c[i] & other_c[i];
    }
  }
}

void DenseColMainMatrix::copy_from_variable_row_mat(DenseVarStackMatrix *mat) {

  assert(sizeof(DenseVarStackMatrix::V) == sizeof(V));
  if constexpr (use_simd) {
    __m512i *me = reinterpret_cast<__m512i *>(&bits[0]);
    __m512i *other = reinterpret_cast<__m512i *>(&mat->bits[0]);

#pragma omp parallel for collapse(2)
    for (size_t c = 0; c < col_count; c++) {
      for (size_t r = 0; r < v_per_col; r++) {
        me[c * v_per_col / V_ALIGN + r / V_ALIGN] =
            other[mat->get_v512_idx(r * V_BITSIZE, c)];
      }
    }
  } else {
    // I do not know how to do this now, and this does not matter
    assert(0);
  }
}

size_t DenseColMainMatrix::col_size_in_bytes() { return v_per_col * sizeof(V); }

void DenseColMainMatrix::load_to_memory() { bits.load_to_memory(); }

DenseColMainMatrix::V *DenseColMainMatrix::get_col_start(size_t col_idx) {
  return &bits[col_idx * v_per_col];
}

void DenseVarStackMatrix::set_derived() {
  assert(stack_row_count % (V_ALIGN * V_BITSIZE) == 0);
  v_per_stack_col = stack_row_count / V_BITSIZE;
  v_per_stack = v_per_stack_col * col_count;
  v_per_stack = align_to(v_per_stack, V_ALIGN);

  stack_count = align_to(row_count, stack_row_count) / stack_row_count;

  total_v_aligned = v_per_stack * stack_count;
  total_v_aligned = align_to(total_v_aligned, V_ALIGN);
}

DenseVarStackMatrix::DenseVarStackMatrix(size_t row_count, size_t col_count,
                                         size_t stack_row_count)
    : row_count(row_count), col_count(col_count),
      stack_row_count(stack_row_count) {
  set_derived();

#ifdef PURE_MEM

  bits = DiskArray<V>(total_v_aligned, DiskArrayMode::Memory);

#else

  bits = DiskArray<V>(total_v_aligned, DiskArrayMode::Anonymous);

#endif
}

DenseVarStackMatrix::DenseVarStackMatrix(std::filesystem::path dir,
                                         size_t row_count, size_t col_count,
                                         size_t stack_row_count)
    : dir(dir), row_count(row_count), col_count(col_count),
      stack_row_count(stack_row_count) {
  set_derived();

  if (filesystem::exists(dir) == false) {
    filesystem::create_directory(dir);
  }
  bits =
      DiskArray<V>(dir / "dense-var-stack-matrix.bin", total_v_aligned, true);
}

bool DenseVarStackMatrix::get(size_t r, size_t c) {
  auto &v = bits[get_v_idx(r, c)];
  return v & MASK64[r % 64];
}

bool DenseVarStackMatrix::get_atomic(size_t r, size_t c) {
  atomic_uint64_t *ap =
      reinterpret_cast<atomic_uint64_t *>(&bits[get_v_idx(r, c)]);
  return ap->load(memory_order_relaxed) & MASK64[r % 64];
}

void DenseVarStackMatrix::set(size_t r, size_t c, bool to) {
  auto &v = bits[get_v_idx(r, c)];
  if (to) {
    v |= MASK64[r % 64];
  } else {
    v &= ~MASK64[r % 64];
  }
}

void DenseVarStackMatrix::set_atomic(size_t r, size_t c, bool to) {
  atomic_uint64_t *ap =
      reinterpret_cast<atomic_uint64_t *>(&bits[get_v_idx(r, c)]);
  if (to) {
    ap->fetch_or(MASK64[r % 64], memory_order_relaxed);
  } else {
    ap->fetch_and(~MASK64[r % 64], memory_order_relaxed);
  }
}

auto DenseVarStackMatrix::operator|=(DenseVarStackMatrix &other)
    -> DenseVarStackMatrix & {
  if constexpr (use_simd) {
    __m512i *v = bits.as_ptr<__m512i>();
    __m512i *o = other.bits.as_ptr<__m512i>();
#pragma omp parallel for
    for (size_t i = 0; i < total_v_aligned / DenseVarStackMatrix::V_ALIGN;
         i++) {
      v[i] = _mm512_or_si512(v[i], o[i]);
    }
    return *this;
  } else {
    V *v = bits.as_ptr<V>();
    V *o = other.bits.as_ptr<V>();
#pragma omp parallel for
    for (size_t i = 0; i < total_v_aligned; i++) {
      v[i] = (v[i] | o[i]);
    }
    return *this;
  }
}

size_t DenseVarStackMatrix::update_visit_and_count(DenseVarStackMatrix &vis,
                                                   DenseVarStackMatrix &out,
                                                   bool dedup) {
  assert(vis.total_v_aligned == out.total_v_aligned);

  if constexpr (use_simd) {
    __m512i *v = reinterpret_cast<__m512i *>(&vis.bits[0]);
    __m512i *o = reinterpret_cast<__m512i *>(&out.bits[0]);
    vector<size_t> vis_cnts(omp_get_max_threads(), 0);

#pragma omp parallel for
    for (size_t i = 0; i < vis.total_v_aligned / DenseVarStackMatrix::V_ALIGN;
         i++) {
      if (dedup) {
        auto x = _mm512_and_si512(v[i], o[i]);
        o[i] = _mm512_xor_si512(o[i], x);
      }
      v[i] = _mm512_or_si512(v[i], o[i]);
      vis_cnts[omp_get_thread_num()] += popcnt512(&v[i]);
    }

    size_t re = 0;
    for (auto r : vis_cnts) {
      re += r;
    }
    return re;
  } else {
    V *v = &vis.bits[0];
    V *o = &out.bits[0];
    vector<size_t> vis_cnts(omp_get_max_threads(), 0);

#pragma omp parallel for
    for (size_t i = 0; i < vis.total_v_aligned; i++) {
      if (dedup) {
        auto x = (v[i] & o[i]);
        o[i] = (o[i] ^ x);
      }
      v[i] = (v[i] | o[i]);
      vis_cnts[omp_get_thread_num()] += std::popcount(v[i]);
    }

    size_t re = 0;
    for (auto r : vis_cnts) {
      re += r;
    }
    return re;
  }
}

size_t DenseVarStackMatrix::dedup_and_update_visit_and_count(
    DenseVarStackMatrix &vis, DenseVarStackMatrix &out) {
  assert(vis.total_v_aligned == out.total_v_aligned);

  if constexpr (use_simd) {
    __m512i *v = reinterpret_cast<__m512i *>(&vis.bits[0]);
    __m512i *o = reinterpret_cast<__m512i *>(&out.bits[0]);
    vector<size_t> vis_cnts(omp_get_max_threads(), 0);

#pragma omp parallel for
    for (size_t i = 0; i < vis.total_v_aligned / DenseVarStackMatrix::V_ALIGN;
         i++) {
      auto x = _mm512_and_si512(v[i], o[i]);
      o[i] = _mm512_xor_si512(o[i], x);
      v[i] = _mm512_or_si512(v[i], o[i]);
      vis_cnts[omp_get_thread_num()] += popcnt512(&v[i]);
    }

    size_t re = 0;
    for (auto r : vis_cnts) {
      re += r;
    }
    return re;
  } else {
    V *v = &vis.bits[0];
    V *o = &out.bits[0];
    vector<size_t> vis_cnts(omp_get_max_threads(), 0);

#pragma omp parallel for
    for (size_t i = 0; i < vis.total_v_aligned; i++) {
      auto x = (v[i] & o[i]);
      o[i] = (o[i] ^ x);
      v[i] = (v[i] | o[i]);
      vis_cnts[omp_get_thread_num()] += std::popcount(v[i]);
    }

    size_t re = 0;
    for (auto r : vis_cnts) {
      re += r;
    }
    return re;
  }
}

size_t DenseVarStackMatrix::count() {
  if constexpr (use_simd) {
    vector<size_t> res(omp_get_max_threads(), 0);
    __m512i *p = reinterpret_cast<__m512i *>(&bits[0]);
#pragma omp parallel for
    for (size_t i = 0; i < total_v_aligned / V_ALIGN; i++) {
      res[omp_get_thread_num()] += popcnt512(&p[i]);
    }
    // SPDLOG_INFO("Total V: {}, Counts: {}",total_v_aligned,res);
    size_t re = 0;
    for (auto r : res) {
      re += r;
    }
    return re;
  } else {
    vector<size_t> res(omp_get_max_threads(), 0);
    V *p = &bits[0];
#pragma omp parallel for
    for (size_t i = 0; i < total_v_aligned; i++) {
      res[omp_get_thread_num()] += std::popcount(p[i]);
    }
    size_t re = 0;
    for (auto r : res) {
      re += r;
    }
    return re;
  }
}

size_t DenseVarStackMatrix::count_bits_in_col(size_t col_idx) {
  if constexpr (use_simd) {
    size_t re = 0;
    for (size_t stack_idx = 0; stack_idx < stack_count; stack_idx++) {
      auto *p = reinterpret_cast<__m512i *>(
          &bits[get_v_idx(stack_idx * stack_row_count, col_idx)]);
      for (size_t i = 0; i < v_per_stack_col / V_ALIGN; i++) {
        re += popcnt512(&p[i]);
      }
    }

    return re;
  } else {
    size_t re = 0;
    for (size_t stack_idx = 0; stack_idx < stack_count; stack_idx++) {
      auto *p = &bits[get_v_idx(stack_idx * stack_row_count, col_idx)];
      for (size_t i = 0; i < v_per_stack_col; i++) {
        re += std::popcount(p[i]);
      }
    }

    return re;
  }
}

bool DenseVarStackMatrix::has_bits_in_col(size_t col_idx) {
  if constexpr (use_simd) {

    for (size_t stack_idx = 0; stack_idx < stack_count; stack_idx++) {
      auto *p = reinterpret_cast<__m512i *>(
          &bits[get_v_idx(stack_idx * stack_row_count, col_idx)]);
      for (size_t i = 0; i < v_per_stack_col / V_ALIGN; i++) {
        if (_mm512_cmpeq_epi64_mask(p[i], _mm512_setzero_si512()) !=
            MASK8ALL1) {
          return true;
        }
      }
    }
    return false;
  } else {
    assert(0);
  }
}

void DenseVarStackMatrix::load_to_memory() { bits.load_to_memory(); }

void DenseVarStackMatrix::prefetch_cols(size_t stack_idx,
                                        DenseVarStackMatrix &other, VIDPair *oc,
                                        size_t len) {
  size_t r = stack_idx * stack_row_count;
  for (size_t i = 0; i < len; i++) {
    auto *p = reinterpret_cast<__m512i *>(&bits[get_v_idx(r, oc[i].second)]);
    auto *q = reinterpret_cast<__m512i *>(
        &other.bits[other.get_v_idx(r, oc[i].first)]);
    for (size_t i = 0; i < v_per_stack_col / V_ALIGN; i++) {
      __builtin_prefetch(&p[i], 1);
      __builtin_prefetch(&q[i], 0);
    }
  }
}

void DenseVarStackMatrix::prefetch_or_stack_cols_from(
    size_t stack_idx, DenseVarStackMatrix &other, VIDPair *oc, size_t len) {
  prefetch_cols(stack_idx, other, oc, len);
  for (size_t i = 0; i < len; i++) {
    or_stack_col_from(stack_idx, oc[i].second, other, oc[i].first);
  }
}

void DenseVarStackMatrix::or_stack_col_from(size_t stack_idx, size_t c,
                                            DenseVarStackMatrix &other,
                                            size_t oc) {
  if (use_simd) {
    size_t r = stack_idx * stack_row_count;
    auto *p = reinterpret_cast<__m512i *>(&bits[get_v_idx(r, c)]);
    auto *q = reinterpret_cast<__m512i *>(&other.bits[other.get_v_idx(r, oc)]);
    for (size_t i = 0; i < v_per_stack_col / V_ALIGN; i++) {
      p[i] = _mm512_or_si512(p[i], q[i]);
    }
  } else {
    size_t r = stack_idx * stack_row_count;
    auto *p = &bits[get_v_idx(r, c)];
    auto *q = &other.bits[other.get_v_idx(r, oc)];
    for (size_t i = 0; i < v_per_stack_col; i++) {
      p[i] = (p[i] | q[i]);
    }
  }
}

#ifdef PRE_PREFETCH

void DenseVarStackMatrix::pre_prefetch(size_t stack_idx,
                                       DenseVarStackMatrix &other,
                                       VIDPair *vp) {
  size_t r = stack_idx * stack_row_count;
  for (size_t x = 0; x < PRE_PREFETCH_OFFSET; x++) {
    size_t c = vp[x].second;
    size_t oc = vp[x].first;

    auto *pf = reinterpret_cast<__m512i *>(&bits[get_v_idx(r, c)]);
    auto *qf = reinterpret_cast<__m512i *>(&other.bits[other.get_v_idx(r, oc)]);
    for (size_t i = 0; i < v_per_stack_col / V_ALIGN; i++) {
      __builtin_prefetch(&pf[i], 1);
      __builtin_prefetch(&qf[i], 0);
    }
  }
}

void DenseVarStackMatrix::or_stack_col_from_with_prefetch(
    size_t stack_idx, DenseVarStackMatrix &other, VIDPair *vp, size_t at) {
  size_t r = stack_idx * stack_row_count;
  size_t c = vp[at].second;
  size_t oc = vp[at].first;
  size_t cf = vp[at + PRE_PREFETCH_OFFSET].second;
  size_t ocf = vp[at + PRE_PREFETCH_OFFSET].first;

  auto *p = reinterpret_cast<__m512i *>(&bits[get_v_idx(r, c)]);
  auto *pf = reinterpret_cast<__m512i *>(&bits[get_v_idx(r, cf)]);
  auto *q = reinterpret_cast<__m512i *>(&other.bits[other.get_v_idx(r, oc)]);
  auto *qf = reinterpret_cast<__m512i *>(&other.bits[other.get_v_idx(r, ocf)]);

  for (size_t i = 0; i < v_per_stack_col / V_ALIGN; i++) {
    p[i] = _mm512_or_si512(p[i], q[i]);
    __builtin_prefetch(&pf[i], 1);
    __builtin_prefetch(&qf[i], 0);
  }
}
#endif

void DenseVarStackMatrix::atomic_v_or_stack_col_from(size_t stack_idx, size_t c,
                                                     DenseVarStackMatrix &other,
                                                     size_t oc) {
  size_t r = stack_idx * stack_row_count;
  auto *p = reinterpret_cast<atomic_uint64_t *>(&bits[get_v_idx(r, c)]);
  auto *q = reinterpret_cast<V *>(&other.bits[other.get_v_idx(r, oc)]);
  for (size_t i = 0; i < v_per_stack_col; i++) {
    p[i].fetch_or(q[i], memory_order_relaxed);
  }
}

void DenseVarStackMatrix::atomic_or_stack_col_from(size_t stack_idx, size_t c,
                                                   DenseVarStackMatrix &other,
                                                   size_t oc,
                                                   atomic_uint8_t *lock) {
  size_t r = stack_idx * stack_row_count;
  auto *p = reinterpret_cast<__m512i *>(&bits[get_v_idx(r, c)]);
  auto *q = reinterpret_cast<__m512i *>(&other.bits[other.get_v_idx(r, oc)]);
  for (size_t i = 0; i < v_per_stack_col / V_ALIGN; i++) {
  // OCC
  restart:
    uint8_t before = lock[i].load(memory_order_seq_cst);
    p[i] = _mm512_or_si512(p[i], q[i]);
    uint8_t after = before + 1;
    if (lock[i].compare_exchange_strong(before, after, memory_order_seq_cst) ==
        false)
      goto restart;
  }
}
void DenseVarStackMatrix::atomic_or_lock_stack_col_from(
    size_t stack_idx, size_t c, DenseVarStackMatrix &other, size_t oc,
    mutex *locks) {
  size_t r = stack_idx * stack_row_count;
  auto *p = reinterpret_cast<__m512i *>(&bits[get_v_idx(r, c)]);
  auto *q = reinterpret_cast<__m512i *>(&other.bits[other.get_v_idx(r, oc)]);
  for (size_t i = 0; i < v_per_stack_col / V_ALIGN; i++) {
    locks[i].lock();
    p[i] = _mm512_or_si512(p[i], q[i]);
    locks[i].unlock();
  }
}

template <typename M>
requires BitMatrix<M> size_t get_row_count(M &m) { return m.get_row_count(); }

//////////////////UNIT TEST/////////////////////////

TEST(DenseMatrix, ColMainMatrixSpeed) {
  size_t row_count = 2000, col_count = 1000000;
  DenseColMainMatrix sm("/tmp/test-sm.bin", row_count, col_count);
#pragma omp parallel for
  for (size_t i = 0; i < row_count; i++) {
    for (size_t j = 0; j < col_count; j++) {
      sm.set_atomic(i, j, true);
    }
  }
  ASSERT_EQ(row_count * col_count, sm.count());
}

TEST(DenseMatrix, StackMatrixSpeed) {
  size_t row_count = 2000, col_count = 1000000;
  DenseStackMatrix sm("/tmp/test-sm.bin", row_count, col_count);
#pragma omp parallel for
  for (size_t i = 0; i < row_count; i++) {
    for (size_t j = 0; j < col_count; j++) {
      sm.set_atomic(i, j, true);
    }
  }
  ASSERT_EQ(row_count * col_count, sm.count());
}

TEST(DenseMatrix, TestConceptBitMatrix) {
  DenseBitMatrix sm("/tmp/test-sm.bin", 200, 1000);
  size_t row_count = get_row_count<DenseBitMatrix>(sm);
  ASSERT_EQ(200, row_count);
}

TEST(DenseMatrix, TestStackSize) {
  DenseStackMatrix sm("/tmp/test-sm.bin", 200, 1000);
  ASSERT_EQ(sm.get_stack_count(), 4);
}

TEST(DenseMatrix, VarStackSize) {
  GTEST_SKIP() << "Not enough memory";
  mt19937 gen(1234);

  size_t v = 1e6;
  SPDLOG_INFO("Expected Size for mat ({},{}) {}B", v, v,
              readable_number(v * v / 8));
  DenseVarStackMatrix sm(v, v, 1024);
  uniform_int_distribution<size_t> dis(0, v - 1);

  set<pair<size_t, size_t>> p_set;
  for (size_t i = 0; i < v * 2; i++) {
    size_t r = dis(gen);
    size_t c = dis(gen);
    while (p_set.contains({r, c})) {
      r = dis(gen);
      c = dis(gen);
    }
    p_set.insert({r, c});

    sm.set(r, c, true);
  }

  for (auto [r, c] : p_set) {
    ASSERT_TRUE(sm.get(r, c));
  }

  Timer t;
  t.start();
  ASSERT_EQ(sm.count(), v * 2);

  t.stop();
  SPDLOG_INFO("Count ({},{}) with {} element using {}", v, v, v * 2,
              t.elapsedTime());
}