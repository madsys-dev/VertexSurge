#include "operator/big_operators2.hh"
#include <tbb/parallel_sort.h>

using namespace std;

MatrixColumnCount::MatrixColumnCount(VariableName count_name,
                                     std::optional<SortOrder> ord)
    : count_name(count_name), ord(ord) {}

void MatrixColumnCount::finishAddingDependency() {
  assert(deps.size() == 1);
  assert(deps[0]->results_schema.columns.size() == 2);
  assert(deps[0]->results_schema.columns[1].type == Type::VID);
  results_schema.columns.push_back({count_name, Type::VID});
  results_schema.columns.push_back({"count", Type::Int32});
  results = unique_ptr<Table>(new FlatTable(results_schema, true));
}

void MatrixColumnCount::run_internal() {
  assert(dep_results[0]->type == TableType::ColMainMatrix ||
         dep_results[0]->type == TableType::VarRowMatrix);

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
      Int32 count = mat->count_bits_in_col(i);
      if (ord.has_value()) {
        temp.push_back({count, col_id});
      } else {
        results->down_to_flat_table()->push_back_single(0, &col_id);
        results->down_to_flat_table()->push_back_single(1, &count);
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
      Int32 count = mat->count_bits_in_col(i);
      if (ord.has_value()) {
        temp.push_back({count, col_id});
      } else {
        results->down_to_flat_table()->push_back_single(0, &col_id);
        results->down_to_flat_table()->push_back_single(1, &count);
      }
    }

    break;
  }
  default:
    assert(false);
  }

  if (ord.has_value()) {
    if (ord.value() == SortOrder::ASC) {
      tbb::parallel_sort(temp.begin(), temp.end(), std::less<TempPair>());
    } else {
      tbb::parallel_sort(temp.begin(), temp.end(), std::greater<TempPair>());
    }

#pragma omp parallel for
    for (size_t i = 0; i < temp.size(); i++) {
      results->down_to_flat_table()->push_back_single(0, &temp[i].second);
      results->down_to_flat_table()->push_back_single(1, &temp[i].first);
    }
  }

  results->down_to_flat_table()->convert_to_sequential();
}
