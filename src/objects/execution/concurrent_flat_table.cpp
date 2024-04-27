#include "execution/flat_table.hh"

using namespace std;

ConcurrentFlatTable::ConcurrentFlatTable(TableSchema schema)
    : Table(schema, TableType::Flat) {
  for (const auto &[name, type] : schema.columns) {
    auto array = make_shared<DA>();
    SPDLOG_TRACE("Set size of data to {}", sizeof_type(type));
    array->set_sizeof_data(sizeof_type(type));
    data.push_back(array);
  }
}

size_t ConcurrentFlatTable::tuple_count() { return data[0]->size(); }

void ConcurrentFlatTable::dump_to_json(std::filesystem::path path) {
  ofstream out(path);
  out << "{";
  out << R"("schema":)";
  nlohmann::json schema = this->schema;
  out << schema << ",";

  out << R"("results":[)";

  for (size_t i = 0; i < tuple_count(); i++) {
    out << tuple_to_json(i) << (i == tuple_count() - 1 ? "" : ",");
  }

  out << "]}";
}

void ConcurrentFlatTable::print(std::optional<size_t> limit) {
  if (limit.has_value() == false) {
    limit = tuple_count();
  }
  for (size_t i = 0; i < limit.value(); i++) {
    SPDLOG_INFO("{}", tuple_to_string(i));
  }
}

void ConcurrentFlatTable::push_back(Tuple t) {
  for (size_t j = 0; j < schema.columns.size(); j++) {
    data[j]->push_back_nt(&t[j]);
  }
}

void ConcurrentFlatTable::push_back_tuple_ref(const DiskTupleRef &t) {
  assert(schema.columns.size() <= DiskTupleRef::InlinedRefCount);
  for (size_t j = 0; j < schema.columns.size(); j++) {
    data[j]->push_back_nt(t.data[j]);
  }
}

void *ConcurrentFlatTable::get_raw_ptr(size_t row_idx, size_t col_idx) {
  return &(*data[col_idx])[row_idx];
}

auto ConcurrentFlatTable::get_col_diskarray(size_t col_idx)
    -> shared_ptr<DA> & {
  return data[col_idx];
}

auto ConcurrentFlatTable::get_tuple_ref_at(size_t i) -> DiskTupleRef {
  DiskTupleRef re;
  for (size_t j = 0; j < schema.columns.size(); j++) {
    re.data[j] = get_raw_ptr(i, j);
  }
  return re;
}

void ConcurrentFlatTable::get_nt(size_t row_idx, size_t col_idx, void *output) {
  memcpy(output, get_raw_ptr(row_idx, col_idx),
         schema.columns[col_idx].type_size());
}

string ConcurrentFlatTable::tuple_to_string(size_t i) {
  string re = "";
  for (size_t j = 0; j < schema.columns.size(); j++) {
    re += schema.columns[j].to_string(get_raw_ptr(i, j));
    if (j != schema.columns.size() - 1)
      re += ",";
  }
  re += "";
  return re;
}

nlohmann::json ConcurrentFlatTable::tuple_to_json(size_t i) {
  nlohmann::json re = nlohmann::json::array();
  for (size_t j = 0; j < schema.columns.size(); j++) {

    re.push_back(schema.columns[j].to_json(get_raw_ptr(i, j)));
  }
  return re;
}

Tuple ConcurrentFlatTable::get_tuple_at(size_t i) {
  Tuple re(schema.columns.size());
  for (size_t j = 0; j < schema.columns.size(); j++) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
    memcpy(&re[j], &(*data[j])[i], data[j]->get_sizeof_data());
#pragma GCC diagnostic pop
  }
  return re;
}

ConcurrentFlatTable::Iterator ConcurrentFlatTable::begin() {
  return Iterator(*this, 0);
}

ConcurrentFlatTable::Iterator ConcurrentFlatTable::end() {
  return Iterator(*this, tuple_count());
}

TEST(TableConcurrent, flat) {
  // we do not use ConcurrentFlatTable anymore
  GTEST_SKIP();
  TableSchema schema = {{{"a", Type::Int32}, {"b", Type::Int32}}};
  ConcurrentFlatTable t(schema);
  t.push_back(Tuple{1, 3});
  t.push_back(Tuple{2, 4});
  t.push_back(Tuple{3, 5});
  Table *table = &t;
  ConcurrentFlatTable *flat_table = dynamic_cast<ConcurrentFlatTable *>(table);

  ConcurrentFlatTable::Iterator x = flat_table->begin();
  EXPECT_EQ((*x)[0].i32, 1);
  EXPECT_EQ((*x)[1].i32, 3);
  ++x;
  EXPECT_EQ((*x)[0].i32, 2);
  EXPECT_EQ((*x)[1].i32, 4);
  ++x;
  EXPECT_EQ((*x)[0].i32, 3);
  EXPECT_EQ((*x)[1].i32, 5);
}
