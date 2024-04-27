#define TBB_PREVIEW_CONCURRENT_ORDERED_CONTAINERS 1
#include "tbb/concurrent_set.h"

#include "execution/flat_table.hh"
#include "execution/matrix_table.hh"

using namespace std;

MatrixTable::MatrixTable(TableSchema schema, TableType type)
    : Table(schema, type) {}

void MatrixTable::set_row_hash_map() {
  if (row_map.has_value() == false) {
    return;
  }
  if (row_hash_map.has_value()) {
    return;
  }

  row_hash_map = std::make_optional<std::unordered_map<VID, VID>>();
  for (size_t i = 0; i < row_map.value()->size(); i++) {
    row_hash_map.value().insert({row_map.value()->as_ptr<VID>()[i], i});
  }
}

void MatrixTable::set_col_hash_map() {
  if (col_map.has_value() == false) {
    return;
  }
  if (col_hash_map.has_value()) {
    return;
  }
  col_hash_map = std::make_optional<std::unordered_map<VID, VID>>();
  for (size_t i = 0; i < col_map.value()->size(); i++) {
    col_hash_map.value().insert({col_map.value()->as_ptr<VID>()[i], i});
  }
}

optional<VID> MatrixTable::find_row_idx(VID row_value) {
  if (row_map.has_value() == false) {
    return row_value < row_count ? std::optional<VID>(row_value) : nullopt;
  }

  return row_hash_map.value().contains(row_value)
             ? optional<VID>(row_hash_map.value().at(row_value))
             : nullopt;
}
optional<VID> MatrixTable::find_col_idx(VID col_value) {
  if (col_map.has_value() == false) {

    return col_value < col_count ? std::optional<VID>(col_value) : nullopt;
  }

  return col_hash_map.value().contains(col_value)
             ? optional<VID>(col_hash_map.value().at(col_value))
             : nullopt;
}

void MatrixTable::take_maps_from(MatrixTable *other) {
  row_map = std::move(other->row_map);
  col_map = std::move(other->col_map);

  row_hash_map = std::move(other->row_hash_map);
  col_hash_map = std::move(other->col_hash_map);
}

VarRowMatrixTable::VarRowMatrixTable(TableSchema schema)
    : MatrixTable(schema, TableType::VarRowMatrix) {
  if (schema.columns.size() != 2) {
    SPDLOG_ERROR("BitMatrixTable must have 2 columns");
    exit(1);
  }
  if (schema.columns[0].type != Type::VID ||
      schema.columns[1].type != Type::VID) {
    SPDLOG_ERROR("BitMatrixTable must have 2 VID columns");
    exit(1);
  }
}

DenseVarStackMatrix *VarRowMatrixTable::get_matrix() { return data.get(); }

void VarRowMatrixTable::set_matrix(std::shared_ptr<DenseVarStackMatrix> &&mat) {
  data = std::move(mat);
  col_count = data->get_col_count();
  row_count = data->get_row_count();
}

void VarRowMatrixTable::dump_to_json(filesystem::path path) {
  ofstream out(path);
  out << "{";
  out << R"("schema":)";
  nlohmann::json schema = this->schema;
  out << schema << ",";

  out << R"("results":[)";

  auto its = get_iterators(1);
  bool first = true;
  for (auto &it : its) {
    VID r, c;
    while (it.next(r, c)) {
      if (!first) {
        out << ",";
      }
      first = false;
      out << "[" << r << "," << c << "]";
    }
  }
  out << "]}";
}

size_t VarRowMatrixTable::tuple_count() { return data->count(); }

void VarRowMatrixTable::print(std::optional<size_t> limit) {
  set<pair<VID, VID>> edges;
  auto its = get_iterators(1);
  for (auto &it : its) {
    VID r, c;
    while (it.next(r, c)) {
      edges.insert({r, c});
    }
  }
  size_t count = 0;

  for (auto [r, c] : edges) {
    count += 1;
    SPDLOG_INFO("({}, {})", r, c);
    if (limit.has_value() && count == limit.value())
      break;
  }
}

VarRowMatrixTable::Iterator::Iterator(VarRowMatrixTable &t, size_t stack_idx,
                                      size_t start_col, size_t end_col)
    : t(t) {
  auto mat = t.get_matrix();
  row_offset = mat->get_stack_row_count() * stack_idx;
  col_offset = start_col;

  auto v_idx =
      mat->get_v_idx(mat->get_stack_row_count() * stack_idx, start_col);
  bit = BitmapIterator(&mat->bits[v_idx], mat->v_per_stack_col *
                                              (end_col - start_col) *
                                              mat->V_BITSIZE);
}

bool VarRowMatrixTable::Iterator::next(VID &r, VID &c) {
  if (bit.next(r)) {
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
    return true;
  }
  return false;
}

vector<VarRowMatrixTable::Iterator>
VarRowMatrixTable::get_iterators(size_t at_least) {
  vector<Iterator> re;
  auto mat = get_matrix();

  size_t col_count =
      align_to(at_least, mat->get_stack_count()) / mat->get_stack_count();
  size_t col_size = (mat->get_col_count() + col_count - 1) / col_count;
  if (col_size == 0)
    col_size = 1;
  col_count = align_to(mat->get_col_count(), col_size) / col_size;

  for (size_t i = 0; i < mat->get_stack_count(); i++) {
    for (size_t j = 0; j < col_count; j++) {
      re.emplace_back(*this, i, j * col_size,
                      min((j + 1) * col_size, mat->get_col_count()));
      // SPDLOG_INFO("Iterator: stack_idx={}, start_col={}, end_col={}", i,
      //             j * col_size, min((j + 1) * col_size,
      //             mat->get_col_count()));
    }
  }

  return re;
}

ColMainMatrixTable::ColMainMatrixTable(TableSchema schema)
    : MatrixTable(schema, TableType::ColMainMatrix) {
  if (schema.columns.size() != 2) {
    SPDLOG_ERROR("BitMatrixTable must have 2 columns");
    exit(1);
  }
  if (schema.columns[0].type != Type::VID ||
      schema.columns[1].type != Type::VID) {
    SPDLOG_ERROR("BitMatrixTable must have 2 VID columns");
    exit(1);
  }
}

DenseColMainMatrix *ColMainMatrixTable::get_matrix() { return data.get(); }

void ColMainMatrixTable::set_matrix(std::shared_ptr<DenseColMainMatrix> &&mat) {
  data = std::move(mat);
  col_count = data->get_col_count();
  row_count = data->get_row_count();
}

size_t ColMainMatrixTable::tuple_count() { return data->count(); }

void ColMainMatrixTable::print([[maybe_unused]] std::optional<size_t> limit) {
  assert(0);
}

void ColMainMatrixTable::dump_to_json([[maybe_unused]] filesystem::path path) {
  assert(0);
  // ofstream out(path);
  // out << "{";
  // out << R"("schema":)";
  // nlohmann::json schema = this->schema;
  // out << schema << ",";

  // out << R"("results":[)";

  // auto its = get_iterators(1);
  // bool first = true;
  // for (auto &it : its) {
  //   VID r, c;
  //   while (it.next(r, c)) {
  //     if (!first) {
  //       out << ",";
  //     }
  //     first = false;
  //     out << "[" << r << "," << c << "]";
  //   }
  // }
  // out << "]}";
}

ColMainMatrixTable::Iterator::Iterator(ColMainMatrixTable &t, size_t start_col,
                                       size_t end_col)
    : t(t), start_col(start_col), end_col(end_col) {
  mat = t.get_matrix();
  auto v_idx = mat->get_idx(0, start_col);

  mat_physical_row_count = mat->v_per_col * mat->V_BITSIZE;
  // SPDLOG_INFO("physical row count {}",mat_physical_row_count);
  bit = BitmapIterator(&mat->bits[v_idx],
                       mat->v_per_col * (end_col - start_col) * mat->V_BITSIZE);
}

bool ColMainMatrixTable::Iterator::next(VID &r, VID &c) {
  if (bit.next(r)) {
    c = r / mat_physical_row_count + start_col;
    r %= mat_physical_row_count;

    if (t.row_map.has_value()) {
      r = t.row_map.value()->as_ptr<VID>()[r];
    }
    if (t.col_map.has_value()) {
      c = t.col_map.value()->as_ptr<VID>()[c];
    }
    return true;
  }
  return false;
}

vector<ColMainMatrixTable::Iterator>
ColMainMatrixTable::get_iterators(size_t at_least) {
  vector<Iterator> re;
  auto mat = get_matrix();

  size_t col_size = align_to(mat->get_col_count(), at_least) / at_least;
  if (col_size == 0)
    col_size = 1;
  col_count = align_to(mat->get_col_count(), col_size) / col_size;

  for (size_t i = 0; i < col_count; i++) {
    size_t col_start = min(i * col_size, mat->get_col_count());
    size_t col_end = min(col_start + col_size, mat->get_col_count());
    if (col_end > col_start) {
      re.emplace_back(*this, col_start, col_end);
    }
  }

  return re;
}

MultipleMatrixTable::MultipleMatrixTable(TableSchema schema, TableType type)
    : Table(schema, type) {
  assert(isMultipleMatrixTable(type));
}

void MultipleMatrixTable::add_mat(std::shared_ptr<Table> table, Int32 dis) {
  assert(table->type == TypeOfMultipleMatrix(type));
  mats.push_back(table);
  this->dis.push_back(dis);
}

void MultipleMatrixTable::output_to_flat(FlatTable *flat) {
  assert(flat->schema == schema);
  for (size_t i = 0; i < dis.size(); i++) {
    Int32 &d = dis[i];
    auto m = mats[i];
    switch (type) {
    case TableType::MultipleVarRowMatrix: {
      auto data = m->down_to_var_row_matrix_table();
#pragma omp parallel for
      for (auto it : data->get_iterators(omp_get_max_threads())) {
        VID r, c;
        while (it.next(r, c)) {
          flat->push_back_single(0, &r);
          flat->push_back_single(1, &c);
          flat->push_back_single(2, &d);
        }
      }

      break;
    }
    case TableType::MultipleColMainMatrix: {
      auto data = m->down_to_col_main_matrix_table();
#pragma omp parallel for
      for (auto it : data->get_iterators(omp_get_max_threads())) {
        VID r, c;
        while (it.next(r, c)) {
          flat->push_back_single(0, &r);
          flat->push_back_single(1, &c);
          flat->push_back_single(2, &d);
        }
      }
      break;
    }
    default: {
      assert(0);
    }
    }
  }
}

size_t MultipleMatrixTable::tuple_count() {
  size_t re = 0;
  for (auto &m : mats) {
    re += m->tuple_count();
  }
  return re;
}

void *MultipleMatrixTable::get_raw_ptr([[maybe_unused]] size_t i,
                                       [[maybe_unused]] size_t j) {
  assert(0);
}
void MultipleMatrixTable::dump_to_json(
    [[maybe_unused]] std::filesystem::path path) {
  assert(0);
}

void MultipleMatrixTable::print([[maybe_unused]] std::optional<size_t> limit) {
  assert(0);
}

TEST(Table, VarRowMatrixTableIterator) {
  std::mt19937 gen(123);
  auto table = unique_ptr<Table>(
      new VarRowMatrixTable(TableSchema{{{"a", Type::VID}, {"b", Type::VID}}}));

  auto mat = make_shared<DenseVarStackMatrix>(2000, 10000, 512);

  for (size_t i = 0; i < mat->get_row_count(); i++) {
    for (size_t j = 0; j < mat->get_col_count(); j++) {
      mat->set(i, j, random_bool(gen));
    }
  }

  table->down_to_var_row_matrix_table()->set_matrix(std::move(mat));
  auto iters = table->down_to_var_row_matrix_table()->get_iterators(
      omp_get_max_threads());
  tbb::concurrent_set<std::pair<VID, VID>> s;

#pragma omp parallel for
  for (auto &iter : iters) {
    VID r, c;
    while (iter.next(r, c)) {
      auto [_, ok] = s.insert(make_pair(r, c));
      if (!ok) {
        SPDLOG_ERROR(
            "r:{},c:{}, insert error, row_offset {}, col_offset {},iter_len {}",
            r, c, iter.row_offset, iter.col_offset, iter.bit.v_idx_max);
        assert(0);
      }
    }
  }
  EXPECT_EQ(table->tuple_count(), s.size());
  SPDLOG_INFO("map size: {} set size: {}", table->tuple_count(), s.size());

  for (auto &[r, c] : s) {
    if (table->down_to_var_row_matrix_table()->get_matrix()->get(r, c) ==
        false) {
      SPDLOG_ERROR("r:{},c:{}", r, c);
      assert(0);
    }
  }

#pragma omp parallel for collapse(2)
  for (size_t r = 0;
       r < table->down_to_var_row_matrix_table()->get_matrix()->get_row_count();
       r++) {
    for (size_t c = 0;
         c <
         table->down_to_var_row_matrix_table()->get_matrix()->get_col_count();
         c++) {
      if (table->down_to_var_row_matrix_table()->get_matrix()->get(r, c)) {
        if (s.find(make_pair(r, c)) == s.end()) {
          SPDLOG_ERROR("r:{},c:{}", r, c);
          assert(false);
        }
      }
    }
  }
}

TEST(Table, ColMainMatrixTableIterator) {
  std::mt19937 gen(123);
  auto table = unique_ptr<Table>(new ColMainMatrixTable(
      TableSchema{{{"a", Type::VID}, {"b", Type::VID}}}));

  auto mat = make_shared<DenseColMainMatrix>(2000, 10000);

  for (size_t i = 0; i < mat->get_row_count(); i++) {
    for (size_t j = 0; j < mat->get_col_count(); j++) {
      mat->set(i, j, random_bool(gen));
    }
  }

  table->down_to_col_main_matrix_table()->set_matrix(std::move(mat));
  auto iters = table->down_to_col_main_matrix_table()->get_iterators(
      omp_get_max_threads());
  tbb::concurrent_set<std::pair<VID, VID>> s;

#pragma omp parallel for
  for (auto &iter : iters) {
    VID r, c;
    while (iter.next(r, c)) {
      auto [_, ok] = s.insert(make_pair(r, c));
      if (!ok) {
        SPDLOG_ERROR("r:{},c:{}, insert error, start col {},iter_len {}", r, c,
                     iter.start_col, iter.bit.v_idx_max);
        assert(0);
      } else {
        // SPDLOG_INFO("r:{},c:{}, insert ok, start col {},iter_len {}", r, c,
        //              iter.start_col, iter.bit.len);
      }
    }
  }
  EXPECT_EQ(table->tuple_count(), s.size());
  SPDLOG_INFO("map size: {} set size: {}", table->tuple_count(), s.size());

  for (auto &[r, c] : s) {
    if (table->down_to_col_main_matrix_table()->get_matrix()->get(r, c) ==
        false) {
      SPDLOG_ERROR("r:{},c:{}", r, c);
      assert(0);
    }
  }

#pragma omp parallel for collapse(2)
  for (size_t r = 0;
       r <
       table->down_to_col_main_matrix_table()->get_matrix()->get_row_count();
       r++) {
    for (size_t c = 0;
         c <
         table->down_to_col_main_matrix_table()->get_matrix()->get_col_count();
         c++) {
      if (table->down_to_col_main_matrix_table()->get_matrix()->get(r, c)) {
        if (s.find(make_pair(r, c)) == s.end()) {
          SPDLOG_ERROR("r:{},c:{}", r, c);
          assert(false);
        }
      }
    }
  }
}