#pragma once

#include "common.hh"
#include "utils/mem.hh"
#include "utils/timer.hh"
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

inline void lseek_to_strech_file(int fd, int64_t size) {

  if (lseek(fd, size - 1, SEEK_SET) == -1) {
    close(fd);
    SPDLOG_ERROR("Error calling lseek() to 'stretch' the file: size: {}", size);
    perror("Error calling lseek() to 'stretch' the file");
    exit(1);
  }
  if (write(fd, "\0", 1) == -1) {
    close(fd);
    perror("Error writing last byte of the file");
    exit(1);
  }
}

struct AlignedDeleter {
  void operator()(void *p) const noexcept { std::free(p); }
};

enum class DiskArrayMode {
  Memory,
  Anonymous,
  ReadOnly,
  ReadWrite,
  OverWrite,
};

// Will create a file if parent dir exists
template <typename T> class DiskArray {

  // remeber to modify move constructor and move assignment
  // after add a new member
  static const size_t HEADER_RESERVED_BYTES = 64;
  bool valid = true;
  DiskArrayMode mode;

  bool m_is_new;
  std::filesystem::path path;

  size_t capacity = (PAGE_SIZE - HEADER_RESERVED_BYTES) / sizeof(T);
  size_t size_m;
  size_t sizeof_data = sizeof(T);
  int fd;

  inline size_t get_file_size() {
    return get_data_area_total_size() + HEADER_RESERVED_BYTES;
  };
  inline size_t get_data_area_total_size() { return capacity * sizeof_data; }

  int64_t *size_p;
  T *data;

  std::unique_ptr<char, AlignedDeleter> mem_data;

  std::shared_mutex wl;

  void expand_capacity_to_at_least(size_t count);

  void *data_at(size_t offset);

public:
  DiskArray();
  DiskArray(DiskArrayMode mode);

  DiskArray(size_t init_size);
  DiskArray(size_t init_size, DiskArrayMode mode);

  DiskArray(std::filesystem::path path, DiskArrayMode mode);
  DiskArray(std::filesystem::path path, bool overwrite);
  DiskArray(std::filesystem::path path, size_t init_size, bool overwrite);
  ~DiskArray();

  DiskArray(const DiskArray &) = delete;
  DiskArray &operator=(const DiskArray &) = delete;

  DiskArray(DiskArray &&other) noexcept {
    // std::cout << "Moving Construct " << other.path << std::endl;
    assert(other.valid);
    mode = other.mode;
    mem_data = std::move(other.mem_data);
    m_is_new = other.m_is_new;
    path = other.path;
    capacity = other.capacity;
    fd = other.fd;
    size_p = other.size_p;
    data = other.data;
    size_m = other.size_m;
    sizeof_data = other.sizeof_data;
    other.valid = false;
  };

  DiskArray &operator=(DiskArray &&other) noexcept {
    if (this != &other) {
      // std::cout << "Moving Assignment " << other.path << std::endl;
      assert(other.valid);
      mode = other.mode;
      mem_data = std::move(other.mem_data);
      m_is_new = other.m_is_new;
      path = other.path;
      capacity = other.capacity;
      fd = other.fd;
      size_p = other.size_p;
      data = other.data;
      size_m = other.size_m;
      sizeof_data = other.sizeof_data;
      other.valid = false;
    }
    return *this;
  };

  // Read

  T *start() { return data; }
  void *get_nt(size_t offset);

  inline size_t size() { return size_m; }
  inline bool is_memory() { return mode == DiskArrayMode::Memory; }
  inline bool is_anonymous() { return mode == DiskArrayMode::Anonymous; }
  inline bool is_new() { return m_is_new; }
  size_t size_in_bytes();

  // Write

  void push_back(T &&t);
  void push_back_nt(void *ptr); // no type

  // return size_m after push many
  size_t push_many(T *t, size_t len);
  size_t push_many_nt(void *ptr, size_t len);

  void set_nt(size_t offset, void *t);                      // No check size
  void set_many(size_t offset, T *t, size_t len);           // will check size
  void fill_many_nt(size_t offset, size_t len, void *with); // will check size

  size_t push_back_nt_atomic(void *t);
  size_t push_back_atomic(T *t);
  size_t push_many_atomic(T *t, size_t len);

  void set_nt_hold(size_t at, void *t); // Prevent parallel remap operation
  size_t allocate_atomic(size_t len);

  void set_zero();
  // Ref
  T &operator[](size_t idx);
  template <typename D> D *as_ptr();

  // Metadata
  bool is_valid() { return valid; }

  // Only resize larger
  void resize(size_t count, T &&t);
  void resize(size_t count);

  bool ever_loaded = false;
  void load_to_memory();
  void set_sizeof_data(size_t size) {
    if (size_m != 0) {
      assert(mode == DiskArrayMode::ReadOnly);
    }
    // assert(size_m == 0);
    assert((std::is_same<T, NoType>::value));
    capacity = (size_in_bytes() - HEADER_RESERVED_BYTES) / size;
    sizeof_data = size;
    SPDLOG_TRACE("Set sizeof data to {} capacity {}", sizeof_data, capacity);
  }
  size_t get_sizeof_data() { return sizeof_data; }
};

// impl libfmt for DiskArrayMode
template <> struct fmt::formatter<DiskArrayMode> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const DiskArrayMode &p, FormatContext &ctx) const
      -> format_context::iterator {
    switch (p) {
    case DiskArrayMode::Memory:
      return format_to(ctx.out(), "Memory");
    case DiskArrayMode::Anonymous:
      return format_to(ctx.out(), "Anonymous");
    case DiskArrayMode::ReadOnly:
      return format_to(ctx.out(), "ReadOnly");
    case DiskArrayMode::ReadWrite:
      return format_to(ctx.out(), "ReadWrite");
    case DiskArrayMode::OverWrite:
      return format_to(ctx.out(), "OverWrite");
    default:
      return format_to(ctx.out(), "Unknown");
    }
  }
};

template <typename T>
DiskArray<T>::DiskArray() : mode(DiskArrayMode::Anonymous) {
  sizeof_data = sizeof(T);
  SPDLOG_TRACE(
      "Anonymous array init size bytes {}, capacity {}, sizeof data {}",
      get_file_size(), capacity, sizeof_data);
  auto p = mmap(0, get_file_size(), PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

  if (p == MAP_FAILED) {
    perror("mmap");
    exit(1);
  }
  m_is_new = true;
  size_p = static_cast<int64_t *>(p);
  data = reinterpret_cast<T *>(reinterpret_cast<char *>(size_p) +
                               HEADER_RESERVED_BYTES);
  size_m = 0;
}

template <typename T> DiskArray<T>::DiskArray(DiskArrayMode mode) : mode(mode) {
  switch (mode) {
  case DiskArrayMode::Memory: {
    sizeof_data = sizeof(T);
    m_is_new = true;
    size_p = nullptr;
    data = nullptr;
    size_m = 0;
    capacity = 0;
    break;
  }
  case DiskArrayMode::Anonymous: {
    *this = DiskArray<T>();
    break;
  }
  default: {
    SPDLOG_ERROR("Not supported for no file backed on this mode");
    assert(0);
  }
  }
}

template <typename T>
DiskArray<T>::DiskArray(size_t init_size) : DiskArray<T>() {
  resize(init_size);
}

template <typename T>
DiskArray<T>::DiskArray(size_t init_size, DiskArrayMode mode)
    : DiskArray<T>(mode) {
  // SPDLOG_INFO("Creating DiskArray {} {}", init_size, mode);
  resize(init_size);
}

template <typename T>
DiskArray<T>::DiskArray(std::filesystem::path path, DiskArrayMode mode)
    : mode(mode), path(path) {
  bool exist = std::filesystem::exists(path);
  int64_t first_map_size = -1;
  if (mode == DiskArrayMode::Anonymous) {
    SPDLOG_ERROR("Anonymous array can't be created with path");
    exit(1);
  }

  if (mode == DiskArrayMode::OverWrite ||
      (mode == DiskArrayMode::ReadWrite && exist == false)) {
    if (exist)
      std::filesystem::remove(path);
    m_is_new = true;
    fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, (mode_t)0666);
    if (fd == -1) {
      perror("open");
      exit(1);
    }

    sizeof_data = sizeof(T); // temporarily set
    lseek_to_strech_file(fd, get_file_size());
    first_map_size = get_file_size();
  }

  if (exist &&
      (mode == DiskArrayMode::ReadOnly || mode == DiskArrayMode::ReadWrite)) {
    m_is_new = false;

    if (mode == DiskArrayMode::ReadWrite)
      fd = open(path.c_str(), O_RDWR);
    if (mode == DiskArrayMode::ReadOnly)
      fd = open(path.c_str(), O_RDONLY);

    if (fd == -1) {
      perror("open");
      exit(1);
    }
    struct stat statbuf;
    if (fstat(fd, &statbuf) == -1) {
      std::cerr << "Error getting file size" << std::endl;
      close(fd);
      exit(1);
    }
    first_map_size = statbuf.st_size;
  }
  if (mode == DiskArrayMode::ReadOnly && exist == false) {
    SPDLOG_ERROR("Read only mode can't be used with non-existing file");
    assert(0);
  }

  void *p;
  if (mode == DiskArrayMode::ReadOnly) {
    p = mmap(0, first_map_size, PROT_READ, MAP_SHARED, fd, 0);
  } else {
    p = mmap(0, first_map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  }

  if (p == MAP_FAILED) {
    perror("mmap");
    exit(1);
  }
  size_p = static_cast<int64_t *>(p);

  if (std::is_same<T, NoType>::value) {
    if (m_is_new) {
      sizeof_data = sizeof(T);
    } else {
      sizeof_data = size_p[1];
    }
  } else {
    sizeof_data = sizeof(T);
  }
  capacity = (first_map_size - HEADER_RESERVED_BYTES) / sizeof_data;

  // SPDLOG_DEBUG("{}, capacity:{}, is_new: {},

  data = reinterpret_cast<T *>(reinterpret_cast<char *>(p) +
                               HEADER_RESERVED_BYTES);
  if (m_is_new) {
    size_p[0] = 0;
    size_p[1] = sizeof_data;
    size_m = 0;
  } else {
    size_m = size_p[0];
  }
}

template <typename T>
DiskArray<T>::DiskArray(std::filesystem::path path, bool overwrite)
    : DiskArray<T>(path, overwrite ? DiskArrayMode::OverWrite
                                   : DiskArrayMode::ReadWrite) {}

template <typename T>
DiskArray<T>::DiskArray(std::filesystem::path path, size_t init_size,
                        bool overwrite)
    : DiskArray<T>(path, overwrite) {
  resize(init_size);
}

template <typename T> DiskArray<T>::~DiskArray() {

  if (is_memory() == false && valid) {
    switch (mode) {
    case DiskArrayMode::ReadOnly:
      break;
    case DiskArrayMode::Anonymous:
      [[fallthrough]];
    case DiskArrayMode::ReadWrite:
      [[fallthrough]];
    case DiskArrayMode::OverWrite:
      size_p[0] = size_m;
      size_p[1] = sizeof_data;
      break;
    default: {
      SPDLOG_ERROR("Unsupported mode {}", mode);
      assert(0);
    }
    }

    // SPDLOG_DEBUG("DiskArray {} Destroy size: {}",path,size_p[0]);
    if (munmap(static_cast<void *>(size_p), get_file_size()) == -1) {
      perror("munmap");
      exit(1);
    }
    close(fd);
  }
}

template <typename T> void DiskArray<T>::push_back(T &&t) {
  if (capacity < size() + 1) {
    expand_capacity_to_at_least(size() + 1);
  }
  data[size_m] = t;
  size_m++;
}

template <> inline void DiskArray<NoType>::push_back_nt(void *ptr) {
  if (capacity < size() + 1) {
    expand_capacity_to_at_least(size() + 1);
  }
  memcpy(data_at(size_m), ptr, sizeof_data);
  size_m++;
}

template <typename T> size_t DiskArray<T>::push_many(T *t, size_t len) {
  if (capacity < len + size()) {
    // std::cout<<"Capacity: "<<capacity<<" size(): "<<size()<<"
    // len:"<<len<<std::endl;
    expand_capacity_to_at_least(len + size());
  }
  memcpy(data + size_m, t, len * sizeof(T));
  size_m += len;
  return size_m;
}

template <>
inline size_t DiskArray<NoType>::push_many_nt(void *ptr, size_t len) {
  if (capacity < len + size()) {
    // std::cout<<"Capacity: "<<capacity<<" size(): "<<size()<<"
    // len:"<<len<<std::endl;
    expand_capacity_to_at_least(len + size());
  }
  memcpy(data_at(size_m), ptr, sizeof_data * len);
  size_m += len;
  return size_m;
}

template <typename T> inline void *DiskArray<T>::get_nt(size_t offset) {
  return data_at(offset);
}

template <typename T> inline void DiskArray<T>::set_nt(size_t offset, void *t) {
  memcpy(data_at(offset), t, sizeof_data);
}

template <typename T>
inline void DiskArray<T>::set_many(size_t offset, T *t, size_t len) {
  if (capacity < offset + len) {
    SPDLOG_INFO("set many while need to expand to {}", offset + len);
    expand_capacity_to_at_least(offset + len);
  }
  memcpy(reinterpret_cast<char *>(data) + offset * sizeof_data, t,
         len * sizeof_data);
  if (offset + len > size_m)
    size_m = offset + len;
}

template <typename T>
inline void DiskArray<T>::fill_many_nt(size_t offset, size_t len, void *with) {
  if (capacity < offset + len) {
    SPDLOG_INFO("fill many while need to expand to {}", offset + len);
    expand_capacity_to_at_least(offset + len);
  }
  for (size_t i = offset; i < offset + len; i++) {
    memcpy(reinterpret_cast<char *>(data) + i * sizeof_data, with, sizeof_data);
  }

  if (offset + len > size_m)
    size_m = offset + len;
}

template <typename T> size_t DiskArray<T>::push_back_nt_atomic(void *t) {
  return push_back_atomic(reinterpret_cast<T *>(t));
}

template <typename T> size_t DiskArray<T>::push_back_atomic(T *t) {
  return push_many_atomic(t, 1);
}

template <typename T> size_t DiskArray<T>::push_many_atomic(T *t, size_t len) {
  std::atomic_int64_t *size_m_atomic =
      reinterpret_cast<std::atomic_int64_t *>(&size_m);
  std::atomic_size_t *capacity_atomic =
      reinterpret_cast<std::atomic_size_t *>(&capacity);
  int64_t size_start = size_m_atomic->fetch_add(len);
  if (size_start + len <= capacity_atomic->load()) {
    std::shared_lock wlg(wl);
    memcpy(data_at(size_start), t, len * sizeof_data);
  } else {
    {
      SPDLOG_DEBUG("Expand capacity to {}", size_start + len);
      std::unique_lock wlg(wl);
      expand_capacity_to_at_least(size_start + len);
    }
    {
      std::shared_lock wlg(wl);
      memcpy(data_at(size_start), t, len * sizeof_data);
    }
  }
  return size_start;
}

template <typename T> void DiskArray<T>::set_nt_hold(size_t at, void *t) {
  std::shared_lock wlg(wl);
  memcpy(data_at(at), t, sizeof_data);
}

template <typename T> size_t DiskArray<T>::allocate_atomic(size_t len) {
  std::atomic_int64_t *size_m_atomic =
      reinterpret_cast<std::atomic_int64_t *>(&size_m);
  std::atomic_size_t *capacity_atomic =
      reinterpret_cast<std::atomic_size_t *>(&capacity);
  int64_t size_start = size_m_atomic->fetch_add(len);
  if (size_start + len <= capacity_atomic->load()) {
  } else {
    {
      SPDLOG_DEBUG("Expand capacity to {}", size_start + len);
      std::unique_lock wlg(wl);
      expand_capacity_to_at_least(size_start + len);
    }
  }
  return size_start;
}

template <> inline NoType &DiskArray<NoType>::operator[](size_t idx) {
  return *reinterpret_cast<NoType *>(reinterpret_cast<char *>(data) +
                                     idx * sizeof_data);
}

template <typename T> T &DiskArray<T>::operator[](size_t idx) {
  return data[idx];
}

template <typename T> template <typename D> D *DiskArray<T>::as_ptr() {
  return reinterpret_cast<D *>(data);
}

template <typename T>
void DiskArray<T>::expand_capacity_to_at_least(size_t count) {
  if (capacity < count) {
    if (mode == DiskArrayMode::Memory) {
      Timer t("expand_capacity_to_at_least");
      size_t old_data_area_size = get_data_area_total_size();
      if (capacity == 0) {
        capacity = count;
      } else {
        while (capacity < count) {
          capacity *= 2;
        }
      }

      auto new_mem_data =
          std::unique_ptr<char, AlignedDeleter>(reinterpret_cast<char *>(
              aligned_alloc(64, get_data_area_total_size())));
      para_avx_memset0(new_mem_data.get(), get_data_area_total_size());
      // para_avx_memset0(new_mem_data.get(), get_data_area_total_size());

      if (data != nullptr) {
        memcpy(new_mem_data.get(), data, old_data_area_size);
      }

      data = reinterpret_cast<T *>(new_mem_data.get());
      mem_data = std::move(new_mem_data);

      SPDLOG_INFO("Resize memory capacity to {}, data area size {} , new addr "
                  "{}",
                  capacity, get_data_area_total_size(),
                  reinterpret_cast<void *>(data));

    } else {

      size_t old_len = get_file_size();
      while (capacity < count) {
        capacity *= 2;
      }
      if (is_anonymous() == false)
        lseek_to_strech_file(fd, get_file_size());

      void *oldp = static_cast<void *>(size_p);
      void *p = mremap(oldp, old_len, get_file_size(), MREMAP_MAYMOVE);
      if (p == MAP_FAILED) {
        perror("mremap");
        assert(0);
      }
      SPDLOG_TRACE("Extend {} from {} to {}, remap from {} to {}", path,
                   old_len / sizeof_data, capacity, oldp, p);

      size_p = static_cast<int64_t *>(p);
      data = reinterpret_cast<T *>(reinterpret_cast<char *>(size_p) +
                                   HEADER_RESERVED_BYTES);
    }
  }
}

template <typename T> void *DiskArray<T>::data_at(size_t offset) {
  return reinterpret_cast<void *>(reinterpret_cast<char *>(data) +
                                  offset * sizeof_data);
}

template <typename T> void DiskArray<T>::resize(size_t count) {
  if (capacity < count) {
    expand_capacity_to_at_least(count);
  }
  assert(size() <= count);
  size_m = count;
}

template <typename T> void DiskArray<T>::resize(size_t count, T &&t) {
  if (capacity < count) {
    expand_capacity_to_at_least(count);
  }
  assert(size() <= count);
  size_m = count;
  for (size_t i = size(); i < count; i++) {
    data[i] = t;
  }
}

template <typename T> void DiskArray<T>::load_to_memory() {
  if (ever_loaded) {
    return;
  }
  if (is_anonymous() == false) {
    if (madvise(size_p, get_file_size(), MADV_WILLNEED) == -1) {
      perror("madvise");
      exit(1);
    }
  }

  volatile char *ptr = reinterpret_cast<char *>(&data[0]);
  for (size_t i = 0; i < size_m; i += sysconf(_SC_PAGE_SIZE) / sizeof_data) {
    (void)ptr[i];
  }
  (void)ptr[size_m - 1];
  ever_loaded = true;
}

template <typename T> void DiskArray<T>::set_zero() {
  memset(data, 0, sizeof_data * size_m);
}

template <typename T> size_t DiskArray<T>::size_in_bytes() {
  return get_file_size();
}
