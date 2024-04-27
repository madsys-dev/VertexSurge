#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <immintrin.h>
#include <iostream>
#include <map>
#include <memory>
#include <popcntintrin.h>
#include <queue>
#include <random>
#include <set>
#include <string>

#include <fcntl.h>
#include <iomanip>
#include <sys/mman.h>
#include <sys/stat.h>
#include <tbb/concurrent_hash_map.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "common.hh"

// smallest X that is >= x and is a multiple of align
inline size_t align_to(size_t x, size_t align) {
  return (x + align - 1) / align * align;
}

// add 2 uint8, if there is an overflow return {sum,true}, else
// return{sum,false}.
inline std::pair<uint8_t, bool> checked_add(uint8_t a, uint8_t b) {
  uint8_t c = a + b;
  return {c, c < a || c < b};
}

inline std::pair<uint8_t, bool> checked_sub(uint8_t a, uint8_t b) {
  uint8_t c = a - b;
  return {c, a < b};
}

template <typename T>
inline std::string to_string_right(T &&input, size_t len) {
  std::stringstream ss;
  ss << std::right << std::setw(len) << input;
  return ss.str();
}

inline void random_numbers(int total, int m, size_t *output) {
  std::set<int> result;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, total - 1);
  while (result.size() < static_cast<size_t>(m)) {
    result.insert(dis(gen));
  }
  int i = 0;
  for (auto it : result) {
    output[i] = it;
    i++;
  }
}

// Generate **distinct** output_size random numbers in [0, total), and store
// them in output
template <typename T>
void uniform_random_numbers(std::mt19937 &gen, size_t total, T *output,
                            size_t output_size) {
  assert(output_size <= total);
  std::uniform_int_distribution<> dis(0, total - 1);
  if (2 * output_size <= total) {
    std::set<T> ret;
    while (ret.size() < output_size) {
      T x = dis(gen);
      ret.insert(x);
    }
    int i = 0;
    for (auto it : ret) {
      output[i++] = it;
    }
  } else {
    std::vector<T> ret(total);
    for (size_t i = 0; i < total; i++) {
      ret[i] = i;
    }
    std::shuffle(ret.begin(), ret.end(), gen);
    for (size_t i = 0; i < output_size; i++) {
      output[i] = ret[i];
    }
  }
}

// random boolean
inline bool random_bool(std::mt19937 &gen) {
  static std::uniform_int_distribution<> dis(0, 1);
  return dis(gen);
}

inline std::array<std::string, 7> units = {"", "K", "M", "G", "T", "P", "E"};

inline std::string readable_number(size_t size) {
  size_t unit_index = 0;
  double readable_size = size;
  while (readable_size >= 1000 && unit_index < units.size() - 1) {
    readable_size /= 1000;
    unit_index++;
  }
  std::ostringstream ss;
  ss << std::fixed << std::setprecision(2) << readable_size;
  std::string str = ss.str();
  return str + "" + units[unit_index];
}

template <typename T>
void loadArrayWithMmap(const std::string &fileName, T *&array,
                       size_t arraySize = -1) {
  int fd = open(fileName.c_str(), O_RDWR);
  if (fd == -1) {
    std::cerr << "Error opening file for reading" << std::endl;
    return;
  }

  size_t fileSize;

  struct stat statbuf;
  if (fstat(fd, &statbuf) == -1) {
    std::cerr << "Error getting file size" << std::endl;
    close(fd);
    exit(1);
  }

  if (arraySize == static_cast<size_t>(-1)) {
    fileSize = statbuf.st_size;
    if (statbuf.st_size % sizeof(T) != 0) {
      std::cerr << "file size: " << statbuf.st_size << " is not a multiple of "
                << sizeof(T) << std::endl;
    }
  } else {
    fileSize = sizeof(T) * arraySize;
  }

  if ((static_cast<size_t>(statbuf.st_size) >= fileSize) == false) {
    std::cerr << "file size: " << statbuf.st_size
              << " expected size: " << fileSize << std::endl;
    assert(false);
  }

  T *map = (T *)mmap(0, fileSize, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
  if (map == MAP_FAILED) {
    close(fd);
    std::cerr << "Error mmapping the file " << fileName << " in " << __func__
              << std::endl;
    exit(1);
  }

  array = map;
}

template <typename T>
void storeArrayWithMmap(const std::string &fileName, T *array,
                        size_t arraySize) {
  int fd = open(fileName.c_str(), O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);
  if (fd == -1) {
    std::cerr << "Error opening file for writing at " << __FUNCTION__ << " "
              << fileName << std::endl;
    return;
  }

  size_t fileSize = sizeof(T) * arraySize;
  if (fileSize == 0) {
    close(fd);
    return;
  }

  if (lseek(fd, fileSize - 1, SEEK_SET) == -1) {
    close(fd);
    std::cerr << "Error calling lseek() from storeArrayWithMmap to 'stretch' "
                 "the file in "
              << fileName << std::endl;
    return;
  }

  if (write(fd, "", 1) == -1) {
    close(fd);
    std::cerr << "Error writing last byte of the file" << std::endl;
    return;
  }

  T *map = (T *)mmap(0, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (map == MAP_FAILED) {
    close(fd);
    std::cerr << "Error mmapping the file " << fileName << " in " << __func__
              << std::endl;
    return;
  }

  for (size_t i = 0; i < arraySize; i++) {
    if (i % 1000000LL == 0) {
      std::cout << "Storing " << i << "/" << arraySize << " "
                << 100.0 * i / arraySize << "%\r";
    }
    map[i] = array[i];
  }

  if (munmap(map, fileSize) == -1) {
    std::cerr << "Error un-mmapping the file" << std::endl;
  }

  close(fd);
  std::cout << arraySize << " Items Stored to" << fileName << std::endl;
}

template <typename T>
std::pair<T *, int> bigArrayWithMmap(const std::string &fileName,
                                     size_t arraySize, bool overwrite = true) {
  int fd;
  bool is_new;

  if (overwrite || std::filesystem::exists(fileName) == false) {
    fd = open(fileName.c_str(), O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);
    is_new = true;
  } else {
    fd = open(fileName.c_str(), O_RDWR);
    is_new = false;
  }

  if (fd == -1) {
    std::cerr << "Error opening file for writing at " << __FUNCTION__ << " "
              << fileName << std::endl;
    exit(1);
  }

  size_t fileSize = sizeof(T) * arraySize;

  bool extend_file = is_new;

  if (is_new == false) {
    struct stat statbuf;
    if (fstat(fd, &statbuf) == -1) {
      std::cerr << "Error getting file size" << std::endl;
      close(fd);
      exit(1);
    }
    if (static_cast<size_t>(statbuf.st_size) < fileSize) {
      extend_file = true;
    }
  }
  if (extend_file) {
    if (lseek(fd, fileSize - 1, SEEK_SET) == -1) {
      close(fd);
      std::cerr << "Error calling lseek() to 'stretch' the file " << fileName
                << std::endl;
      exit(1);
    }

    if (write(fd, "\0", 1) == -1) {
      close(fd);
      std::cerr << "Error writing last byte of the file" << std::endl;
      exit(1);
    }
  }

  T *map = (T *)mmap(0, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (map == MAP_FAILED) {
    close(fd);
    std::cerr << "Error mmapping the file " << fileName << " in " << __func__
              << std::endl;
    exit(1);
  }
  return {map, fd};
}

template <typename T> void bigArrayUnmap(T *ptr, int fd, size_t arraySize) {
  size_t fileSize = sizeof(T) * arraySize;
  if (munmap(ptr, fileSize) == -1) {
    std::cerr << "Error un-mmapping the file" << std::endl;
  }
  close(fd);
}

template <typename T>
void SortChunk(T *data, size_t data_size,
               std::filesystem::path output_filename) {
  FILE *output_file = fopen(output_filename.c_str(), "w");
  std::sort(data, data + data_size);

  for (size_t i = 0; i < data_size; i++) {
    fwrite(&data[i], sizeof(T), 1, output_file);
  }

  fclose(output_file);
}

template <typename T>
void MergeChunks(const std::vector<std::filesystem::path> &chunk_filenames,
                 const std::filesystem::path output_filename) {
  // Open output file.
  FILE *output_file = fopen(output_filename.c_str(), "w");

  // Create priority queue to hold the current smallest values from each chunk.
  using TI = std::pair<T, size_t>;
  std::priority_queue<TI, std::vector<TI>, std::greater<TI>> pq;

  std::vector<FILE *> chunk_files;

  for (size_t i = 0; i < chunk_filenames.size(); ++i) {
    // Open chunk file.
    FILE *chunk_file = fopen(chunk_filenames[i].c_str(), "r");
    chunk_files.push_back(chunk_file);
    // Read first value from chunk and add to priority queue.
    T x;
    if (fread(&x, sizeof(T), 1, chunk_file) == 1) {
      pq.push({x, i});
    }
  }

  // Merge values from priority queue into output file.
  while (!pq.empty()) {
    T x = pq.top().first;
    size_t i = pq.top().second;
    pq.pop();

    // Write value to output file.
    fwrite(&x, sizeof(T), 1, output_file);

    // Read next value from chunk and add to priority queue.
    if (fread(&x, sizeof(int), 1, chunk_files[i]) == 1) {
      pq.push({x, i});
    }
  }
  // Close output file.
  fclose(output_file);
}

template <typename T>
void ExternalMergeSort(const std::filesystem::path &input_file,
                       const std::filesystem::path &output_file,
                       size_t data_size, size_t memory_size) {
  size_t chunk_data_size = memory_size / sizeof(T);
  std::cout << "External Merge Sort. " << readable_number(data_size)
            << " items in " << input_file << ", "
            << readable_number(memory_size) << "B Memory, Chunk data count: "
            << readable_number(chunk_data_size) << std::endl;
  T *data;
  loadArrayWithMmap(input_file, data, data_size);
  if (data_size <= chunk_data_size) {
    std::cout << "Single Chunk" << std::endl;
    std::sort(data, data + data_size);
    std::cout << "Storing" << std::endl;
    storeArrayWithMmap(output_file, data, data_size);
  } else {
    std::vector<std::filesystem::path> chunk_files;
    size_t chunk_count = (data_size - 1) / chunk_data_size + 1;

    for (size_t i = 0; i < chunk_count; i++) {
      std::cout << "Sorting Chunk " << i << std::endl;
      std::filesystem::path chunk_file =
          output_file.string() + std::to_string(i);
      chunk_files.push_back(chunk_file);
      SortChunk(data + (i * sizeof(T)),
                (i == chunk_count - 1) ? (data_size % chunk_data_size)
                                       : chunk_data_size,
                chunk_file);
    }
    std::cout << "Merging Chunks" << std::endl;
    MergeChunks<T>(chunk_files, output_file);

    for (size_t i = 0; i < chunk_count; i++) {
      std::cout << "Removing " << chunk_files[i] << std::endl;
      std::filesystem::remove(chunk_files[i]);
    }
  }
}

enum class PrintAlign {
  Left,
  Center,
  Right,
};

inline std::string fill_to_length(const std::string &title, size_t width,
                                  char fill_with = ' ',
                                  PrintAlign align = PrintAlign::Center) {
  assert(width >= title.size());
  switch (align) {
  case PrintAlign::Left:
    return title + std::string(width - title.size(), fill_with);
    break;
  case PrintAlign::Center: {
    size_t left = (width - title.size()) / 2;
    size_t right = width - title.size() - left;
    return std::string(left, fill_with) + title + std::string(right, fill_with);
    break;
  }
  case PrintAlign::Right:
    return std::string(width - title.size(), fill_with) + title;
    break;
  default:
    std::cout << "Unkown align type" << std::endl;
    exit(1);
  }
}

template <typename T, typename F> struct BuffDoFunc {
  T *data;
  size_t buff_size, buff_cnt = 0;
  F func;

public:
  BuffDoFunc(size_t buff_size, F func) : buff_size(buff_size), func(func) {
    data = new T[buff_size];
  }

  inline void flush() {
    if (buff_cnt != 0) {
      func(data, buff_cnt);
      buff_cnt = 0;
    }
  }
  inline void push(T x) {
    data[buff_cnt++] = x;
    if (buff_cnt == buff_size) {
      flush();
    }
  }
  ~BuffDoFunc() {
    flush();
    delete[] data;
  }
};

inline std::map<std::string, std::string> read_kv(std::filesystem::path path) {

  std::ifstream ifs(path);
  std::map<std::string, std::string> kv_map;

  std::string line;
  while (std::getline(ifs, line)) {
    size_t pos = line.find(":");
    if (pos != std::string::npos) {
      std::string key = line.substr(0, pos);
      std::string value = line.substr(pos + 1);
      kv_map[key] = value;
    }
  }

  // for (const auto &[key, value] : kv_map)
  // {
  //   std::cout << key << " : " << value << std::endl;
  // }

  return kv_map;
}

struct Config {
public:
  std::filesystem::path path;
  std::map<std::string, std::string> kv_map;

  Config(std::filesystem::path path) : path(path) { kv_map = read_kv(path); }

  inline void print() {
    for (auto &[key, value] : kv_map) {
      std::cout << key << ":" << value << std::endl;
    }
  }

  template <typename T> T get_or_init(std::string key, T default_value) {
    if (kv_map.count(key) == 0) {
      kv_map[key] = std::to_string(default_value);
    }
    return get<T>(key);
  }

  template <typename T> T get(std::string key) {
    if (kv_map.find(key) != kv_map.end()) {
      std::stringstream ss(kv_map[key]);
      T value;
      ss >> value;
      return value;
    } else {
      std::cout << "Key " << key << " not found" << std::endl;
      assert(false);
    }
  }

  template <typename T> void set(std::string key, const T &value) {
    kv_map[key] = std::to_string(value);
  }

  template <typename T> bool contains(std::string key) {
    return kv_map.find(key) != kv_map.end();
  }

  inline void store() { store_to(path); }

  inline void store_to(std::filesystem::path path) {
    std::ofstream ofs(path);
    for (const auto &[key, value] : kv_map) {
      ofs << key << ":" << value << std::endl;
    }
  }
};

struct alignas(64) cache_padded_size_t {
  std::atomic_size_t inner;
};




template <typename T>
void save_vec(std::vector<T> &v, std::filesystem::path f) {
  storeArrayWithMmap(f.string(), v.data(), v.size());
}

template <typename T> std::vector<T> load_vec(std::filesystem::path f) {
  size_t size = std::filesystem::file_size(f) / sizeof(T);
  if (std::filesystem::file_size(f) % sizeof(T) != 0) {
    assert(false && "No Full T");
  }

  T *tmp;
  loadArrayWithMmap(f, tmp, size);
  std::vector<T> re(size);
  for (size_t i = 0; i < size; i++) {
    re[i] = tmp[i];
  }
  return re;
}

// If deletions are perform concurrently, this function might lead to an error.
template <typename Key, typename T>
void modify_or_insert(tbb::concurrent_hash_map<Key, T> &map, Key k, T v,
                      std::function<void(T &)> f) {
  typename tbb::concurrent_hash_map<Key, T>::accessor x;
  if (map.find(x, k)) {
    f(x->second);
  } else {
    if (map.insert(x, k)) {
      x->second = v;
    } else {
      assert(map.find(x, k));
      f(x->second);
    }
  }
}

inline size_t popcnt512(__m512i *p) {
  auto x = _mm512_popcnt_epi64(*p);
  return _mm512_reduce_add_epi64(x);
}
constexpr __mmask8 MASK8ALL1 = 0xff;

inline bool isDisplayable(char c) { return c >= 32 && c <= 126; }

inline void removeNonDisplayableCharacters(std::string &str) {
  for (char &c : str) {
    if (isDisplayable(c) == false) {
      c = '_';
    }
  }
}

// inclusive range
template <typename T> struct Range {
  T start;
  T end;

  Range() = default;

  Range(std::initializer_list<T> list) {
    assert(list.size() == 2);
    start = *list.begin();
    end = *(list.begin() + 1);
  }

  bool in(T x) { return x >= start && x <= end; }
};

template <typename T> struct fmt::formatter<Range<T>> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const Range<T> &p, FormatContext &ctx) const
      -> format_context::iterator {
    return format_to(ctx.out(), "[{}, {}]", p.start, p.end);
  }
};