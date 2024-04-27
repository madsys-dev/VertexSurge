#pragma once

#include "common.hh"
#include "physical_operator.hh"
#include <tbb/concurrent_vector.h>

/*

  This file declares some auxiliary operators that are used in the system.

*/

class InlinedData : public BigOperator {
  inline void run_internal() override{};

public:
  InlinedData() = default;
};

class Count : public BigOperator {
  void run_internal();

public:
  Count();
};

class TableConvert : public BigOperator {

  TableType convert_to;
  bool transpose;

  void finishAddingDependency() override;
  void run_internal() override;

public:
  TableConvert(TableType type, bool transpose = false);
};

class Projection : public BigOperator {
  std::vector<std::pair<VariableName, VariableName>> projection_map;
  std::vector<size_t> old_variable_idx;

  void finishAddingDependency() override;
  void run_internal() override;

public:
  using MapPair = std::pair<VariableName, VariableName>;
  using MapPairs = std::vector<MapPair>;

  Projection(MapPairs projection_map);
};

template <MatrixTableFilterFn F> class MatrixTableFilter : public BigOperator {
  F f;

  void finishAddingDependency() override;
  void run_internal() override;

public:
  MatrixTableFilter(F f);
};

/*

  Select some columns in the matrix

*/

// TODO: Select None Empty Columns
class MatrixTableSelect : public BigOperator {
  VariableName key_name;
  std::vector<size_t> key_idx;
  ValueSchema ks;

  void finishAddingDependency() override;
  void run_internal() override;

public:
  MatrixTableSelect(VariableName key_name);
};

/*
  Reorder Tuples in Flat Table
*/
class ReOrderFlat : public BigOperator {

  FlatTupleCompareFn cmp_fn;
  FlatTupleOrderFn order_fn;
  bool pre_order;
  std::vector<size_t> pre_orders;

  void finishAddingDependency() override;
  void run_internal() override;

public:
  ReOrderFlat(FlatTupleCompareFn cmp_fn);
  ReOrderFlat(FlatTupleOrderFn order_fn);
};

enum class SortOrder {
  ASC,
  DESC,
};

/*

  Turn to a flat table, first column is the column name of matrix, second column
  is the count

*/

class MatrixColumnCount : public BigOperator {
  VariableName count_name;

  using TempPair = std::pair<Int32, VID>;
  std::optional<SortOrder> ord;
  tbb::concurrent_vector<TempPair> temp; // temperary sort here

  void finishAddingDependency() override;
  void run_internal() override;

public:
  MatrixColumnCount(VariableName count_name,
                    std::optional<SortOrder> ord = std::nullopt);
};

/*
  Turn to a flat table with one column, the column is the visited vertex ids of
  all start vertices
*/
class MatrixRowOrSum : public BigOperator {

  void finishAddingDependency() override;
  void run_internal() override;

public:
  MatrixRowOrSum();
};

// impls

template <MatrixTableFilterFn F>
MatrixTableFilter<F>::MatrixTableFilter(F f) : f(f) {}

template <MatrixTableFilterFn F>
void MatrixTableFilter<F>::finishAddingDependency() {
  assert(deps.size() == 1);
  results_schema = deps[0]->results_schema;
}

template <MatrixTableFilterFn F> void MatrixTableFilter<F>::run_internal() {
  results = dep_results[0];
  assert(isMatrixTable(results->type) || isMultipleMatrixTable(results->type));
  switch (results->type) {
  case TableType::VarRowMatrix: {
    auto m = results->down_to_table<VarRowMatrixTable>();
    m->filter(f);
    break;
  }
  case TableType::ColMainMatrix: {
    auto m = results->down_to_table<ColMainMatrixTable>();
    m->filter(f);
    break;
  }
  case TableType::MultipleVarRowMatrix: {
    auto ms = results->down_to_table<MultipleMatrixTable>();
    ms->filter(f);
    break;
  }
  case TableType::MultipleColMainMatrix: {
    auto ms = results->down_to_table<MultipleMatrixTable>();
    ms->filter(f);
    break;
  }
  default: {
    assert(0);
  }
  }
}
