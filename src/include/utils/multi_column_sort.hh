
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <omp.h>
#include <random>

/*
sort from [begin,begin+n)
cmp(begin,x,y) x<y return false , if they need to be swapped
swap(begin,x,y) swaps
*/

const size_t BUBBLE_SORT_THRESHOLD = 8;
const size_t PARALLEL_THRESHOLD = 1024;
const size_t PARTITION_PARALLEL_THRESHOLD = 100000000;

template <class Visitor, class Compare, class SwapWith>
size_t partition(Visitor visitor, std::size_t low, std::size_t high,
                 Compare &&cmp, SwapWith &&swap) {
  if (high - low <= 1)
    return low;
  if (high - low == 2) {
    if (cmp(visitor, low, high - 1) == false) {
      swap(visitor, low, high - 1);
    }
    return low;
  }
  // Choose element at mid as pivot
  size_t mid = (low + high) / 2;
  swap(visitor, mid, high - 1);

  size_t j = low;
  for (size_t i = low; i < high; i++) {
    if (cmp(visitor, i, high - 1)) {
      if (i != j)
        swap(visitor, i, j);
      j++;
    }
  }
  if (j != high - 1)
    swap(visitor, j, high - 1);

  return j;
}

template <class Visitor, class Compare, class SwapWith>
size_t para_partition(Visitor visitor, std::size_t low, std::size_t high,
                      Compare &&cmp, SwapWith &&swap, size_t parallelism,
                      std::mt19937_64 &gen) {
  size_t mid = (low + high) / 2;
  swap(visitor, mid, high - 1);
  size_t p_idx = para_partition_with_pivot(visitor, low, high - 1, cmp, swap,
                                           high - 1, parallelism, gen);
  swap(visitor, p_idx, high - 1);
  return p_idx;
}

template <class Visitor, class Compare, class SwapWith>
size_t para_partition_with_pivot(Visitor visitor, std::size_t low,
                                 std::size_t high, Compare &cmp, SwapWith &swap,
                                 size_t pivot_idx, size_t parallelism,
                                 std::mt19937_64 &gen) {

  if (high - low < PARTITION_PARALLEL_THRESHOLD || parallelism == 1) {
    // std::cout <<"serial "<<low<<" "<<high<<" , "<<parallelism<<std::endl;
    auto p_idx =
        serial_partition_with_pivot(visitor, low, high, cmp, swap, pivot_idx);
    return p_idx;
  } else {
    // std::cout <<"para "<< low << " " << high << std::endl;
    size_t count_per_U = (high - low + parallelism - 1) / parallelism;

    std::vector<size_t> X(parallelism);
    for (size_t i = 0; i < parallelism; i++)
      X[i] = i;
    std::shuffle(X.begin(), X.end(), gen);

    std::vector<size_t> v_jth(parallelism, 0);
    auto idx_fn = [count_per_U, &X, parallelism, low](size_t i, size_t j) {
      // idx for j-th element of thread i
      return low + j * parallelism + (X[i] + j) % parallelism;
    };

    omp_set_num_threads(parallelism);
#pragma omp parallel for
    for (size_t tid = 0; tid < parallelism; tid++) {
      for (size_t j = 0; j < count_per_U; j++) {
        size_t idx = idx_fn(tid, j);
        if (idx >= high)
          break;
        if (cmp(visitor, idx, pivot_idx)) {
          swap(visitor, idx, idx_fn(tid, v_jth[tid]));
          v_jth[tid]++;
        }
      }
    }

    size_t new_low = high, new_high = low;
    for (size_t i = 0; i < parallelism; i++) {
      new_low = std::min(new_low, idx_fn(i, v_jth[i]));
      new_high = std::max(new_high, idx_fn(i, v_jth[i]));
    }
    // std::cout << "para new " << new_low<< " " << new_high<< std::endl;
    if (new_low == low && new_high == high)
      return serial_partition_with_pivot(visitor, low, high, cmp, swap,
                                         pivot_idx);

    else
      return para_partition_with_pivot(visitor, new_low, new_high, cmp, swap,
                                       pivot_idx, parallelism, gen);
  }
}

template <class Visitor, class Compare, class SwapWith>
size_t serial_partition_with_pivot(Visitor visitor, std::size_t low,
                                   std::size_t high, Compare &cmp,
                                   SwapWith &swap, std::size_t pivot_idx) {
  //   std::cout <<"serial" << low << " " << high << std::endl;
  size_t j = low;
  for (size_t i = low; i < high; i++) {
    if (cmp(visitor, i, pivot_idx)) {
      swap(visitor, i, j);
      j++;
    }
  }
  return j;
}

template <class Visitor, class Compare, class SwapWith>
void parasort_impl(Visitor visitor, std::size_t low, std::size_t high,
                   Compare &&cmp, SwapWith &&swap) {
  // std::cout<<low<<" "<<high<<std::endl;
  if (high - low < BUBBLE_SORT_THRESHOLD) {
    for (size_t i = low; i < high; i++) {
      for (size_t j = i + 1; j < high; j++) {
        if (cmp(visitor, i, j) == false) {
          swap(visitor, i, j);
        }
      }
    }
  } else {
    std::mt19937_64 gen(1234);
    size_t p = para_partition(visitor, low, high, cmp, swap,
                              omp_get_num_threads(), gen);
    // size_t p = partition(visitor, low, high, cmp, swap);

    if (high - low > PARALLEL_THRESHOLD) {
#pragma omp task
      parasort_impl(visitor, low, p, cmp, swap);
#pragma omp task
      parasort_impl(visitor, p + 1, high, cmp, swap);
#pragma omp taskwait
    } else {
      parasort_impl(visitor, low, p, cmp, swap);
      parasort_impl(visitor, p + 1, high, cmp, swap);
    }

    // parasort(visitor, low, p, cmp, swap);

    // parasort(visitor, p + 1, high, cmp, swap);
  }
}

template <class Visitor, class Compare, class SwapWith>
void parasort(Visitor visitor, std::size_t low, std::size_t high, Compare &&cmp,
              SwapWith &&swap) {
#pragma omp parallel
  {
#pragma omp single
    parasort_impl(visitor, low, high, cmp, swap);
  }
}

template <class Visitor, class Compare, class SwapWith>
void serialsort(Visitor visitor, std::size_t low, std::size_t high,
                Compare &&cmp, SwapWith &&swap) {
  // std::cout << low << " " << high << std::endl;
  if (high - low < BUBBLE_SORT_THRESHOLD) {
    for (size_t i = low; i < high; i++) {
      for (size_t j = i + 1; j < high; j++) {
        if (cmp(visitor, i, j) == false) {
          swap(visitor, i, j);
        }
      }
    }
  } else {
    std::mt19937_64 gen(1234);
    // size_t p = para_partition(visitor, low, high, cmp, swap,
    //                           omp_get_max_threads(), gen);

    size_t p = partition(visitor, low, high, cmp, swap);

    serialsort(visitor, low, p, cmp, swap);
    serialsort(visitor, p + 1, high, cmp, swap);
  }
}