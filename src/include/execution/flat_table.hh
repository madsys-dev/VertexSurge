#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common.hh"
#include "meta/schema.hh"
#include "meta/type.hh"
#include "storage/bitmap.hh"
#include "storage/concurrent_disk_array.hh"
#include "storage/disk_array.hh"
#include "table.hh"
#include "tuple.hh"

class FlatTable : public Table {
  bool is_concurrent;

  // sequential data in columns
  std::vector<std::shared_ptr<DiskArray<NoType>>> data;

  // sequential data in columns
  std::vector<std::shared_ptr<ConcurrentDiskArray<NoType>>> c_data;

  // For sequential concurrent control
  std::atomic_size_t idx;

  void init_data();
  void init_c_data();

public:
  std::optional<std::string> sorted_on;

  FlatTable(TableSchema schema, bool is_concurrent = false);

  // Meta
  void check_data_consistent_with_schema();
  size_t tuple_count() override;
  size_t atomic_alloc(size_t len);

  void convert_to_sequential();
  void update_c_data_index();

  // Write
  void push_back(Tuple t);
  void push_back_tuple_ref(const DiskTupleRef &t);
  size_t extend_horizontal(
      ValueSchema vs); // extended column will be resized to tuple_count()
  size_t extend_horizontal(ValueSchema vs, void *init); // and init
  // void set_column(size_t i,std::shared_ptr<DiskArray<NoType>> data);

  // must be concurrent
  void push_back_single(size_t col_idx, void *val);

  void resize(size_t new_size);

  void set(size_t row_idx, size_t col_idx, void *val);
  void hold_set(size_t row_idx, size_t col_idx,
                void *val); // hold current map, in concurrent operation where
                            // the map will be extend in parallel
  void hold_set_by_tuple_ref(size_t row_idx, DiskTupleRef r);
  void push_back_single_by_tuple_ref(DiskTupleRef r);

  template <size_t N>
  void hold_set_by_fixed_tuple(size_t i, FixedTuple<N> &ft) {
    assert(is_concurrent == false);
    for (size_t j = 0; j < N; j++) {
      hold_set(i, j, &ft[j]);
    }
  }

  // Read
  void dump_to_json(std::filesystem::path path) override;
  void print(std::optional<size_t> limit) override;

  Tuple get_tuple_at(size_t i);

  template <size_t N>
  FixedTuple<N> get_fixed_tuple_at(size_t i, std::array<size_t, N> j) {
    FixedTuple<N> re;
    for (size_t k = 0; k < N; k++) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
      memcpy(&re[k], get_raw_ptr(i, j[k]), data[j[k]]->get_sizeof_data());
#pragma GCC diagnostic pop
    }
    return re;
  }

  template <typename T> T get_T(size_t row_idx, size_t col_idx) {
    return *reinterpret_cast<T *>(get_raw_ptr(row_idx, col_idx));
  }

  void get_nt(size_t row_idx, size_t col_idx, void *output);

  std::string tuple_to_string(size_t i);
  nlohmann::json tuple_to_json(size_t i);

  // Ref
  void *get_raw_ptr(size_t row_idx, size_t col_idx) override;
  std::shared_ptr<DiskArray<NoType>> &get_col_diskarray(size_t col_idx);
  DiskTupleRef get_start_tuple_ref();
  // If is_concurrent, do not offset at this tuple ref
  DiskTupleRef get_tuple_ref_at(size_t i);

  class Iterator {
    FlatTable &t;
    size_t i;

  public:
    Iterator(FlatTable &t, size_t i) : t(t), i(i){};
    Tuple operator*() { return t.get_tuple_at(i); }
    Iterator &operator++() {
      i++;
      return *this;
    }
    bool operator!=(const Iterator &other) { return i != other.i; }
  };

  Iterator begin();
  Iterator end();

  friend Merge;
};

// depricated
class ConcurrentFlatTable : public Table {
  using DA = ConcurrentDiskArray<NoType>;
  // sequential data in columns
  std::vector<std::shared_ptr<DA>> data;

public:
  std::optional<std::string> sorted_on;

  ConcurrentFlatTable(TableSchema schema);

  // Meta
  size_t tuple_count() override;

  // Write
  void push_back(Tuple t);
  void push_back_tuple_ref(const DiskTupleRef &t);

  void set(size_t row_idx, size_t col_idx, void *val);

  // Read
  void dump_to_json(std::filesystem::path path) override;
  void print(std::optional<size_t> limit) override;

  Tuple get_tuple_at(size_t i);

  template <size_t N>
  FixedTuple<N> get_fixed_tuple_at(size_t i, std::array<size_t, N> j) {
    FixedTuple<N> re;
    for (size_t k = 0; k < N; k++) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
      memcpy(&re[k], &(*data[j[k]])[i], data[j[k]]->get_sizeof_data());
#pragma GCC diagnostic pop
    }
    return re;
  }

  template <typename T> T get_T(size_t row_idx, size_t col_idx) {
    return *reinterpret_cast<T *>(get_raw_ptr(row_idx, col_idx));
  }

  void get_nt(size_t row_idx, size_t col_idx, void *output);

  std::string tuple_to_string(size_t i);
  nlohmann::json tuple_to_json(size_t i);

  // Ref
  void *get_raw_ptr(size_t row_idx, size_t col_idx) override;
  std::shared_ptr<DA> &get_col_diskarray(size_t col_idx);

  // Should use .offset(size_t) Method on this tuple ref
  DiskTupleRef get_tuple_ref_at(size_t i);

  class Iterator {
    ConcurrentFlatTable &t;
    size_t i;

  public:
    Iterator(ConcurrentFlatTable &t, size_t i) : t(t), i(i) {
      // We disable this
      assert(0);
    };
    Tuple operator*() { return t.get_tuple_at(i); }
    Iterator &operator++() {
      i++;
      return *this;
    }
    bool operator!=(const Iterator &other) { return i != other.i; }
  };

  Iterator begin();
  Iterator end();
};
