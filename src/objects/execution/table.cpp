#include "execution/flat_table.hh"
#include "execution/matrix_table.hh"

using namespace std;

Table::Table(TableSchema schema, TableType type) : type(type), schema(schema) {}

FlatTable *Table::down_to_flat_table() {
  assert(type == TableType::Flat);
  return dynamic_cast<FlatTable *>(this);
}

VarRowMatrixTable *Table::down_to_var_row_matrix_table() {
  assert(type == TableType::VarRowMatrix);
  return dynamic_cast<VarRowMatrixTable *>(this);
}

ColMainMatrixTable *Table::down_to_col_main_matrix_table() {
  assert(type == TableType::ColMainMatrix);
  return dynamic_cast<ColMainMatrixTable *>(this);
}

MultipleTable::MultipleTable(TableSchema schema, VariableName key_name)
    : Table(schema, TableType::Multiple),
      key_schema(schema.get_schema(key_name)->first) {
  subtable_schema = schema;
  subtable_schema.remove_variable(key_schema);
}

void MultipleTable::add_table(void *key, std::shared_ptr<Table> table) {
  assert(subtable_schema == table->schema);
  keys.push_back(TupleValue::construct_tuple_value(key_schema.type, key));
  tables.push_back(table);
}

std::shared_ptr<Table> MultipleTable::new_empty_subflat(bool is_concurrent) {
  return std::shared_ptr<Table>(new FlatTable(subtable_schema, is_concurrent));
}

size_t MultipleTable::tuple_count() {
  size_t re = 0;
  for (auto &t : tables) {
    re += t->tuple_count();
  }
  return re;
}

void *MultipleTable::get_raw_ptr(size_t, size_t) {
  // TODO. Maybe sometimes we will impl this
  assert(0);
}

void MultipleTable::dump_to_json(std::filesystem::path) {}

void MultipleTable::print(std::optional<size_t> limit) {
  if (limit.has_value() == false) {
    for (auto &t : tables) {
      t->print(nullopt);
    }
  } else {
    size_t count = limit.value();
    for (size_t i = 0; i < keys.size(); i++) {
      auto &k = keys[i];
      auto &t = tables[i];
      SPDLOG_INFO("Key: {}", k.to_string(key_schema.type));
      t->print(std::min(count, t->tuple_count()));
      count -= std::min(count, t->tuple_count());
      if (count == 0) {
        break;
      }
    }
  }
}
