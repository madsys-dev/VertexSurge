#pragma once

#include "disk_array.hh"
#include <omp.h>

/*
    Concurrent Disk Array
    This Disk Array utilized multiple files to improve performance when
   concurrent access is needed.
*/

template <typename T> class ConcurrentDiskArray {
  DiskArrayMode mode;
  std::filesystem::path parent_dir;
  std::optional<size_t> sizeof_data = std::nullopt;

  static constexpr size_t MAX_CONCURRENCY = 256;
  std::unique_ptr<DiskArray<T>> arraies[MAX_CONCURRENCY];

  size_t index[MAX_CONCURRENCY + 1];

  void init_array(size_t i);

  DiskArray<T> *first_non_null_array();
  DiskArray<T> *get_thread_disk_array();
  std::pair<DiskArray<T> *, size_t> get_disk_and_didx_by_index(size_t idx);

public:
  ConcurrentDiskArray();

  ConcurrentDiskArray(std::filesystem::path path, DiskArrayMode mode);
  ConcurrentDiskArray(std::filesystem::path path, bool overwrite);

  ~ConcurrentDiskArray() = default;

  // Disable copy
  ConcurrentDiskArray(const ConcurrentDiskArray &) = delete;
  ConcurrentDiskArray &operator=(const ConcurrentDiskArray &) = delete;

  // Define move
  ConcurrentDiskArray(ConcurrentDiskArray &&other) noexcept = default;
  ConcurrentDiskArray &
  operator=(ConcurrentDiskArray &&other) noexcept = default;

  // Metadata
  inline bool is_anonymous() { return mode == DiskArrayMode::Anonymous; }
  // inline bool is_valid(){return valid;}
  inline bool is_new() { return first_non_null_array()->is_new(); }
  size_t size_in_bytes();
  void update_index();
  size_t get_sizeof_data() { return sizeof_data.value(); }
  void set_sizeof_data(size_t to) {
    assert(sizeof_data.has_value() == false);
    sizeof_data = to;
  }

  // Read
  size_t size();

  // Write

  /*
      These are concurrent safe.
  */

  void push_back_nt(void *ptr);
  //   void push_back(T& t);
  void push_back(T &&t);

  void push_many(T *t, size_t len);
  void push_many_nt(void *ptr, size_t len);

  void set_nt(size_t offset, void *t);
  void set_many(size_t offset, T *t, size_t len);

  // merge to first non null array, and destory myself, must update index
  std::unique_ptr<DiskArray<T>> merge_to_first_array();
  void merge_to_provided_array(DiskArray<T> &to);

  // Ref
  T &operator[](size_t idx);
};

template <typename T> void ConcurrentDiskArray<T>::init_array(size_t i) {
  if (parent_dir == "") {
    arraies[i] = std::make_unique<DiskArray<T>>();
  } else {
    arraies[i] =
        std::make_unique<DiskArray<T>>(parent_dir / std::to_string(i), mode);
  }
  if (std::is_same<T, NoType>::value) {
    arraies[i]->set_sizeof_data(sizeof_data.value());
  }
}

template <typename T>
DiskArray<T> *ConcurrentDiskArray<T>::first_non_null_array() {
  for (size_t i = 0; i < MAX_CONCURRENCY; i++) {
    if (arraies[i] != nullptr) {
      return arraies[i].get();
    }
  }
  return nullptr;
}

template <typename T>
DiskArray<T> *ConcurrentDiskArray<T>::get_thread_disk_array() {

  size_t tid = omp_get_thread_num();

  if (arraies[tid] == nullptr) {
    // SPDLOG_INFO("Init thread id {} array, before {}, at {}", tid,
    // reinterpret_cast<size_t>( arraies[tid].get()),
    // reinterpret_cast<size_t>(this) );
    init_array(tid);
  }
  // SPDLOG_INFO("Get thread id {} array", tid);
  return arraies[tid].get();
}

template <typename T>
std::pair<DiskArray<T> *, size_t>
ConcurrentDiskArray<T>::get_disk_and_didx_by_index(size_t idx) {
  size_t which =
      std::upper_bound(index, index + MAX_CONCURRENCY + 1, idx) - index;
  if (which == MAX_CONCURRENCY + 1)
    return {nullptr, MAX_CONCURRENCY};
  return {arraies[which - 1].get(), which - 1};
}

template <typename T>
ConcurrentDiskArray<T>::ConcurrentDiskArray() : mode(DiskArrayMode::Anonymous) {
  //   SPDLOG_INFO("ConcurrentDiskArray: Anonymous");
  if (std::is_same<T, NoType>::value == false)
    sizeof_data = sizeof(T);
}

template <typename T>
ConcurrentDiskArray<T>::ConcurrentDiskArray(std::filesystem::path path,
                                            DiskArrayMode mode)
    : mode(mode), parent_dir(path) {
  //   SPDLOG_INFO("ConcurrentDiskArray: {} {}", path, mode);

  if (mode == DiskArrayMode::Anonymous) {
    SPDLOG_ERROR("Anonymous array can't be created with path");
    assert(0);
  }
  if (std::is_same<T, NoType>::value == false)
    sizeof_data = sizeof(T);

  bool exist = std::filesystem::exists(path);
  if (!exist)
    std::filesystem::create_directories(path);
}

template <typename T>
ConcurrentDiskArray<T>::ConcurrentDiskArray(std::filesystem::path path,
                                            bool overwrite)
    : ConcurrentDiskArray<T>(path, overwrite ? DiskArrayMode::OverWrite
                                             : DiskArrayMode::ReadWrite) {}

template <typename T> size_t ConcurrentDiskArray<T>::size_in_bytes() {
  size_t re = 0;
  for (size_t i = 0; i < MAX_CONCURRENCY; i++) {
    re += arraies[i] == nullptr ? 0 : arraies[i]->size_in_bytes();
  }
  return re;
}

template <typename T> void ConcurrentDiskArray<T>::update_index() {
  index[0] = 0;
  for (size_t i = 1; i < MAX_CONCURRENCY + 1; i++) {
    index[i] =
        index[i - 1] + (arraies[i - 1] == nullptr ? 0 : arraies[i - 1]->size());
  }
}

template <typename T> size_t ConcurrentDiskArray<T>::size() {
  size_t re = 0;
  for (size_t i = 0; i < MAX_CONCURRENCY; i++) {
    re += arraies[i] == nullptr ? 0 : arraies[i]->size();
  }
  return re;
}

template <> inline void ConcurrentDiskArray<NoType>::push_back_nt(void *ptr) {
  // SPDLOG_INFO("{}",__func__);
  get_thread_disk_array()->push_back_nt(ptr);
}

// template <typename T> void ConcurrentDiskArray<T>::push_back(T &t) {
//   get_thread_disk_array()->push_back(t);
// }

template <typename T> void ConcurrentDiskArray<T>::push_back(T &&t) {
  // SPDLOG_INFO("{}", __func__);
  get_thread_disk_array()->push_back(std::move(t));
}

template <typename T> void ConcurrentDiskArray<T>::push_many(T *t, size_t len) {
  // SPDLOG_INFO("{}", __func__);
  get_thread_disk_array()->push_many(t, len);
}

template <>
inline void ConcurrentDiskArray<NoType>::push_many_nt(void *ptr, size_t len) {
  // SPDLOG_INFO("{}", __func__);
  get_thread_disk_array()->push_many_nt(ptr, len);
}

template <typename T>
void ConcurrentDiskArray<T>::set_nt(size_t offset, void *t) {
  memcpy(&(*this)[offset], t, sizeof_data.value());
}

template <typename T>
void ConcurrentDiskArray<T>::set_many([[maybe_unused]] size_t offset,
                                      [[maybe_unused]] T *t,
                                      [[maybe_unused]] size_t len) {
  assert(0);
}

template <typename T>
std::unique_ptr<DiskArray<T>> ConcurrentDiskArray<T>::merge_to_first_array() {
  std::unique_ptr<DiskArray<T>> *first;
  for (size_t i = 0; i < MAX_CONCURRENCY; i++) {
    if (arraies[i] != nullptr) {
      first = &arraies[i];
      break;
    }
  }
  first->get()->resize(size());
#pragma omp parallel for
  for (size_t i = 0; i < MAX_CONCURRENCY; i++) {
    if (arraies[i] != nullptr && &arraies[i] != first) {
      first->get()->set_many(index[i], &(*arraies[i])[0], arraies[i]->size());
      arraies[i].release();
    }
  }
  return std::move(*first);
}

template <typename T>
void ConcurrentDiskArray<T>::merge_to_provided_array(DiskArray<T> &to) {
  assert(to.size() == 0);
  assert(to.get_sizeof_data() == get_sizeof_data());
  to.resize(size());
#pragma omp parallel for
  for (size_t i = 0; i < MAX_CONCURRENCY; i++) {
    if (arraies[i] != nullptr) {
      to.set_many(index[i], &(*arraies[i])[0], arraies[i]->size());
      arraies[i].release();
    }
  }
}

template <typename T> T &ConcurrentDiskArray<T>::operator[](size_t idx) {
  auto [disk, didx] = get_disk_and_didx_by_index(idx);
  if (disk == nullptr) {
    throw std::out_of_range("ConcurrentDiskArray: out of range");
  }
  return (*disk)[idx - index[didx]];
}
