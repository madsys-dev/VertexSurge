#pragma once
#include "table.hh"

class MatrixTable : public Table {
protected:
  // row_map[physical_row_idx] == real_row_vid
  std::optional<std::shared_ptr<DiskArray<NoType>>> row_map, col_map;

  // row_map[real_row_id] == physical_row_idx
  std::optional<std::unordered_map<VID, VID>> row_hash_map, col_hash_map;

  void set_row_hash_map();
  void set_col_hash_map();

public:
  MatrixTable(TableSchema schema, TableType type);

  size_t col_count, row_count;

  std::optional<VID> find_row_idx(VID row_value);
  std::optional<VID> find_col_idx(VID col_value);

  inline auto &get_row_map() { return row_map; }
  inline auto &get_col_map() { return col_map; }

  void *get_raw_ptr([[maybe_unused]] size_t i,
                    [[maybe_unused]] size_t j) override {
    assert(0);
  };

  void take_maps_from(MatrixTable *other);

  friend TableConvert;
  friend HashJoin;

  template <size_t N> friend class SemiJoin;
};

template <typename F>
concept MatrixTableFilterFn = requires(F f, VID r, VID c) {
  { f(r, c) } -> std::same_as<bool>; // return false if need to be filterd out
};

class VarRowMatrixTable : public MatrixTable {
  std::shared_ptr<DenseVarStackMatrix> data;

public:
  using UnderlyingBitMatrix = DenseVarStackMatrix;

  VarRowMatrixTable(TableSchema schema);

  // Read
  size_t tuple_count() override;
  void dump_to_json(std::filesystem::path path) override;
  void print(std::optional<size_t> limit) override;

  // Write

  void set_matrix(std::shared_ptr<DenseVarStackMatrix> &&);

  // Ref

  DenseVarStackMatrix *get_matrix();

  struct Iterator {
    VarRowMatrixTable &t;

    BitmapIterator bit;
    VID row_offset, col_offset;

    Iterator(VarRowMatrixTable &t, size_t stack_idx, size_t start_col,
             size_t end_col);

    bool next(VID &r, VID &c);
    template <MatrixTableFilterFn F> void filter(F f);
  };

  // cannot guarantee at_least
  std::vector<Iterator> get_iterators(size_t recommended_at_least);
  template <MatrixTableFilterFn F> void filter(F f);

  friend MatrixTableSelect;
};

class ColMainMatrixTable : public MatrixTable {
  std::shared_ptr<DenseColMainMatrix> data;

public:
  using UnderlyingBitMatrix = DenseColMainMatrix;
  ColMainMatrixTable(TableSchema schema);

  // Read
  size_t tuple_count() override;
  void dump_to_json(std::filesystem::path path) override;
  void print(std::optional<size_t> limit) override;

  // Write
  void set_matrix(std::shared_ptr<DenseColMainMatrix> &&);

  // Ref
  DenseColMainMatrix *get_matrix();

  struct Iterator {
    ColMainMatrixTable &t;
    DenseColMainMatrix *mat;
    size_t mat_physical_row_count;

    BitmapIterator bit;
    size_t start_col, end_col;

    // start_col and end_col are physical columns
    Iterator(ColMainMatrixTable &t, size_t start_col, size_t end_col);

    // r and c are stored values, (mapped if any map)
    bool next(VID &r, VID &c);
    template <MatrixTableFilterFn F> void filter(F f);
  };

  std::vector<Iterator> get_iterators(size_t at_least);
  template <MatrixTableFilterFn F> void filter(F f);

  friend MatrixTableSelect;

  friend Mintersect;
};

class FactorizedTable : public Table {};

static_assert(BitMatrixTable<ColMainMatrixTable>);
static_assert(BitMatrixTable<VarRowMatrixTable>);

class MultipleMatrixTable : public Table {

  std::vector<Int32> dis;
  std::vector<std::shared_ptr<Table>> mats;

public:
  MultipleMatrixTable(TableSchema schema, TableType type);

  void add_mat(std::shared_ptr<Table> table, Int32 dis);
  void output_to_flat(FlatTable *flat);

  template <MatrixTableFilterFn F> void filter(F f);

  size_t tuple_count();
  void *get_raw_ptr(size_t i, size_t j);
  void dump_to_json(std::filesystem::path path);
  void print(std::optional<size_t> limit);
};

// impls

template <MatrixTableFilterFn F> void VarRowMatrixTable::Iterator::filter(F f) {
  VID r, c;

  while (bit.next(r)) {
    r %= t.get_matrix()->get_stack_row_count();
    r += row_offset;
    c = bit.now_v_idx / t.get_matrix()->v_per_stack_col;
    c += col_offset;

    if (t.row_map.has_value()) {
      r = t.row_map.value()->as_ptr<VID>()[r];
    }
    if (t.col_map.has_value()) {
      c = t.col_map.value()->as_ptr<VID>()[c];
    }

    if (f(r, c) == false) {
      bit.set_now_false();
    }
  }
}

template <MatrixTableFilterFn F> void VarRowMatrixTable::filter(F f) {
#pragma omp parallel for
  for (auto &it : get_iterators(omp_get_max_threads())) {
    it.filter(f);
  }
}

template <MatrixTableFilterFn F>
void ColMainMatrixTable::Iterator::filter(F f) {
  VID r, c;

  while (bit.next(r)) {
    c = r / mat_physical_row_count + start_col;
    r %= mat_physical_row_count;

    if (t.row_map.has_value()) {
      r = t.row_map.value()->as_ptr<VID>()[r];
    }
    if (t.col_map.has_value()) {
      c = t.col_map.value()->as_ptr<VID>()[c];
    }

    if (f(r, c) == false) {
      bit.set_now_false();
    }
  }
}

template <MatrixTableFilterFn F> void ColMainMatrixTable::filter(F f) {
#pragma omp parallel for
  for (auto &it : get_iterators(omp_get_max_threads())) {
    it.filter(f);
  }
}

template <MatrixTableFilterFn F> void MultipleMatrixTable::filter(F f) {
  for (auto m : mats) {
    switch (type) {
    case TableType::MultipleVarRowMatrix: {
      m->down_to_var_row_matrix_table()->filter(f);
      break;
    }
    case TableType::MultipleColMainMatrix: {
      m->down_to_col_main_matrix_table()->filter(f);
      break;
    }
    default:
      assert(0);
    }
  }
}