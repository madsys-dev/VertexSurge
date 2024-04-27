#pragma once

#include <cassert>
#include <cstdint>
#include <immintrin.h>

template <typename T> bool is_aligned(T *ptr, std::size_t alignment) {
  return reinterpret_cast<std::uintptr_t>(ptr) % alignment == 0;
}

inline void para_avx_memset0(void *start, std::size_t size) {
  assert(is_aligned(start, 64));
  assert(size % 64 == 0);
  __m512i *ptr = (__m512i *)start;
  __m512i zero = _mm512_setzero_si512();
#pragma omp parallel for schedule(static, 1024)
  for (std::size_t i = 0; i < size / 64; i++) {
    ptr[i] = zero;
  }
}
