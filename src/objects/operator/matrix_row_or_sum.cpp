#include "operator/big_operators2.hh"

using namespace std;

MatrixRowOrSum::MatrixRowOrSum() {}

void MatrixRowOrSum::finishAddingDependency() {
  assert(deps[0]->results_schema.columns.size() == 2);
  results_schema.columns.push_back(deps[0]->results_schema.columns[1]);
  assert(results_schema.columns[0].type == Type::VID);
  results = unique_ptr<Table>(new FlatTable(results_schema, true));
}

void MatrixRowOrSum::run_internal() {
  switch (dep_results[0]->type) {
  case TableType::ColMainMatrix: {
    auto table = dep_results[0]->down_to_col_main_matrix_table();
    auto mat = table->get_matrix();
    auto map = table->get_col_map();

#pragma omp parallel for
    for (size_t i = 0; i < mat->get_col_count(); i++) {
      VID col_id = i;
      if (map.has_value()) {
        col_id = map.value()->as_ptr<VID>()[col_id];
      }
      if (mat->has_bits_in_col(i)) {
        results->down_to_flat_table()->push_back_single(0, &col_id);
      }
    }

    break;
  }
  case TableType::VarRowMatrix: {
    auto table = dep_results[0]->down_to_var_row_matrix_table();
    auto mat = table->get_matrix();
    auto map = table->get_col_map();

#pragma omp parallel for
    for (size_t i = 0; i < mat->get_col_count(); i++) {
      VID col_id = i;
      if (map.has_value()) {
        col_id = map.value()->as_ptr<VID>()[col_id];
      }
      if (mat->has_bits_in_col(i)) {
        results->down_to_flat_table()->push_back_single(0, &col_id);
      }
    }
    break;
  }
  default:
    assert(false);
  }
  results->down_to_flat_table()->convert_to_sequential();
}
