#include "execution/flat_table.hh"

using namespace std;

void FlatTable::init_data() {
  for (const auto &[name, type] : schema.columns) {
    auto array = make_shared<DiskArray<NoType>>();
    SPDLOG_TRACE("Set size of data to {}", sizeof_type(type));
    array->set_sizeof_data(sizeof_type(type));
    data.push_back(array);
  }
}

void FlatTable::init_c_data() {
  for (const auto &[name, type] : schema.columns) {
    auto array = make_shared<ConcurrentDiskArray<NoType>>();
    SPDLOG_TRACE("Set size of data to {}", sizeof_type(type));
    array->set_sizeof_data(sizeof_type(type));
    c_data.push_back(array);
  }
}

FlatTable::FlatTable(TableSchema schema, bool is_concurrent)
    : Table(schema, TableType::Flat), is_concurrent(is_concurrent), idx(0) {
  if (is_concurrent)
    init_c_data();
  else
    init_data();
}

void FlatTable::check_data_consistent_with_schema() {
  if (is_concurrent) {
    assert(schema.columns.size() == c_data.size());
    for (size_t i = 0; i < schema.columns.size(); i++) {
      assert(schema.columns[i].type_size() == c_data[i]->get_sizeof_data());
      assert(tuple_count() == c_data.size());
    }
  } else {
    assert(schema.columns.size() == data.size());
    for (size_t i = 0; i < schema.columns.size(); i++) {
      assert(tuple_count() == data[i]->size());
      assert(schema.columns[i].type_size() == data[i]->get_sizeof_data());
    }
  }
}

size_t FlatTable::tuple_count() {
  return is_concurrent ? c_data[0]->size() : data[0]->size();
}

size_t FlatTable::atomic_alloc(size_t len) {
  assert(is_concurrent == false);
  for (auto &t : data) {
    t->allocate_atomic(len);
  }
  return idx.fetch_add(len);
}

void FlatTable::convert_to_sequential() {
  update_c_data_index();
  if (is_concurrent == false)
    return;
  is_concurrent = false;
  init_data();

  for (size_t i = 0; i < c_data.size(); i++) {
    c_data[i]->merge_to_provided_array(*data[i]);
  }
}

void FlatTable::update_c_data_index() {
  if (is_concurrent == false)
    return;
  for (size_t i = 0; i < c_data.size(); i++) {
    c_data[i]->update_index();
  }
  size_t size = c_data[0]->size();
  for (size_t i = 1; i < c_data.size(); i++) {
    if (c_data[i]->size() != size) {
      SPDLOG_ERROR("column {} size {} not equal to column {}", i,
                   c_data[i]->size(), 0);
      assert(0);
    }
  }
}

void FlatTable::push_back(Tuple t) {
  if (is_concurrent) {
    for (size_t j = 0; j < schema.columns.size(); j++) {
      c_data[j]->push_back_nt(&t[j]);
    }
  } else {
    for (size_t j = 0; j < schema.columns.size(); j++) {
      data[j]->push_back_nt(&t[j]);
    }
  }
}

void FlatTable::push_back_tuple_ref(const DiskTupleRef &t) {
  assert(schema.columns.size() <= DiskTupleRef::InlinedRefCount);
  if (is_concurrent) {
    for (size_t j = 0; j < schema.columns.size(); j++) {
      c_data[j]->push_back_nt(t.data[j]);
    }
  } else {
    for (size_t j = 0; j < schema.columns.size(); j++) {
      data[j]->push_back_nt(t.data[j]);
    }
  }
}

size_t FlatTable::extend_horizontal(ValueSchema vs) {
  schema.columns.push_back(vs);
  assert(is_concurrent == false);
  auto array = make_shared<DiskArray<NoType>>();
  SPDLOG_TRACE("Set size of data to {}", sizeof_type(vs.type));
  array->set_sizeof_data(sizeof_type(vs.type));
  array->resize(this->tuple_count());
  data.push_back(array);
  return schema.columns.size() - 1;
}

size_t FlatTable::extend_horizontal(ValueSchema vs, void *init) {
  auto re = extend_horizontal(vs);
  data[re]->fill_many_nt(0, tuple_count(), init);
  return re;
}

// void FlatTable::set_column(size_t i,std::shared_ptr<DiskArray<NoType>> data){
//   assert(is_concurrent == false);
//   this->data[i] = data;
// }

void FlatTable::push_back_single(size_t col_idx, void *val) {
  assert(is_concurrent);
  c_data[col_idx]->push_back_nt(val);
}

void FlatTable::resize(size_t new_size) {
  assert(is_concurrent == false);
  for (size_t j = 0; j < schema.columns.size(); j++) {
    data[j]->resize(new_size);
  }
}

void FlatTable::set(size_t row_idx, size_t col_idx, void *val) {
  if (is_concurrent) {
    c_data[col_idx]->set_nt(row_idx, val);
  } else {
    data[col_idx]->set_nt(row_idx, val);
  }
}

void FlatTable::hold_set(size_t row_idx, size_t col_idx, void *val) {
  assert(is_concurrent == false);
  data[col_idx]->set_nt_hold(row_idx, val);
}

void FlatTable::hold_set_by_tuple_ref(size_t row_idx, DiskTupleRef r) {
  assert(is_concurrent == false);
  for (size_t j = 0; j < schema.columns.size(); j++) {
    data[j]->set_nt_hold(row_idx, r.data[j]);
  }
}

void FlatTable::push_back_single_by_tuple_ref(DiskTupleRef r) {
  assert(is_concurrent);
  for (size_t j = 0; j < schema.columns.size(); j++) {
    c_data[j]->push_back_nt(r.data[j]);
  }
}

void FlatTable::dump_to_json(std::filesystem::path path) {
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

void FlatTable::print(std::optional<size_t> limit) {
  update_c_data_index();
  if (limit.has_value() == false || limit.value() > tuple_count()) {
    limit = tuple_count();
  }
  for (size_t i = 0; i < limit.value(); i++) {
    SPDLOG_INFO("{}", tuple_to_string(i));
  }
}

Tuple FlatTable::get_tuple_at(size_t i) {
  Tuple re(schema.columns.size());
  for (size_t j = 0; j < schema.columns.size(); j++) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
    memcpy(&re[j], get_raw_ptr(i, j), data[j]->get_sizeof_data());
#pragma GCC diagnostic pop
  }
  return re;
}

void FlatTable::get_nt(size_t row_idx, size_t col_idx, void *output) {
  memcpy(output, get_raw_ptr(row_idx, col_idx),
         schema.columns[col_idx].type_size());
}

string FlatTable::tuple_to_string(size_t i) {
  string re = "";
  for (size_t j = 0; j < schema.columns.size(); j++) {
    re += schema.columns[j].to_string(get_raw_ptr(i, j));
    if (j != schema.columns.size() - 1)
      re += ",";
  }
  re += "";
  return re;
}

nlohmann::json FlatTable::tuple_to_json(size_t i) {
  nlohmann::json re = nlohmann::json::array();
  for (size_t j = 0; j < schema.columns.size(); j++) {

    re.push_back(schema.columns[j].to_json(get_raw_ptr(i, j)));
  }
  return re;
}

void *FlatTable::get_raw_ptr(size_t row_idx, size_t col_idx) {
  return is_concurrent ? &(*c_data[col_idx])[row_idx]
                       : &(*data[col_idx])[row_idx];
}

auto FlatTable::get_col_diskarray(size_t col_idx)
    -> shared_ptr<DiskArray<NoType>> & {
  assert(!is_concurrent);
  return data[col_idx];
}
auto FlatTable::get_start_tuple_ref() -> DiskTupleRef {
  assert(!is_concurrent);
  DiskTupleRef re;
  for (size_t j = 0; j < schema.columns.size(); j++) {
    re.data[j] = data[j]->as_ptr<void>();
  }
  return re;
}

auto FlatTable::get_tuple_ref_at(size_t i) -> DiskTupleRef {
  DiskTupleRef re;
  for (size_t j = 0; j < schema.columns.size(); j++) {
    re.data[j] = get_raw_ptr(i, j);
  }
  return re;
}

FlatTable::Iterator FlatTable::begin() { return Iterator(*this, 0); }

FlatTable::Iterator FlatTable::end() { return Iterator(*this, tuple_count()); }

TEST(Table, flat) {
  TableSchema schema = {{{"a", Type::Int32}, {"b", Type::Int32}}};
  FlatTable t(schema);
  t.push_back(Tuple{1, 3});
  t.push_back(Tuple{2, 4});
  t.push_back(Tuple{3, 5});
  Table *table = &t;
  FlatTable *flat_table = dynamic_cast<FlatTable *>(table);

  FlatTable::Iterator x = flat_table->begin();
  EXPECT_EQ((*x)[0].i32, 1);
  EXPECT_EQ((*x)[1].i32, 3);
  ++x;
  EXPECT_EQ((*x)[0].i32, 2);
  EXPECT_EQ((*x)[1].i32, 4);
  ++x;
  EXPECT_EQ((*x)[0].i32, 3);
  EXPECT_EQ((*x)[1].i32, 5);
}
