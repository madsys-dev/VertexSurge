#pragma once
#include <tbb/concurrent_unordered_set.h>

#include "aggregation.hh"
#include "common.hh"
#include "physical_operator.hh"
/*

    This file declares some auxiliary operators that are used in the system.

*/

/*
   Two table A,B as input.
   Filter out tuples whose values not in B.
   N equals to the number of key variables


   Support Only when A,B are Flat or VarRowMatrix
*/
template <size_t N> class SemiJoin : public BigOperator {
  using T = FixedTuple<N>;

  VariableName key_variables[N];
  std::array<size_t, N> a_idx, b_idx;

  TableSchema key_variable_schema;

  bool only_count;

  tbb::concurrent_unordered_set<T, typename T::Hash, typename T::Equal> b_set;

  bool probe_once(T &t);

  void build();
  void probe();

  void finishAddingDependency() override;
  void run_internal() override;

public:
  SemiJoin(std::array<VariableName, N> key_variables, bool only_count);
};

class FetchVertexProperty : public BigOperator {

  TypeName vertex_type;
  VariableName vid_variable;
  VariableName property_name;
  VariableName property_alias;

  ValueSchema property_schema;
  ValueSchema property_alias_schema;

  void run_internal() override;

public:
  FetchVertexProperty(
      TypeName vertex_type, VariableName vid_variable,
      VariableName property_name,
      std::optional<VariableName> property_alias = std::nullopt);
  void finishAddingDependency() override;
};

class GroupByAggregation : public BigOperator {
  VariableName group_by_what, aggregate_what, aggregate_as;

  AggregationType agg_type;
  std::shared_ptr<Aggregator> agg;

  size_t key_at, value_at;

  void run_internal() override;
  void finishAddingDependency() override;

public:
  GroupByAggregation(VariableName group_by_what, VariableName aggregate_what,
                     AggregationType agg_type,
                     std::optional<VariableName> aggregate_as = std::nullopt);
};

class SplitFlatBy : public BigOperator {
  VariableName split_by_what;
  Type split_type;
  size_t split_idx;

  using MapType = std::unordered_map<TupleValue, std::shared_ptr<Table>,
                                     TupleValueRuntimeHash, TupleValueKeyEqual>;

  std::shared_ptr<MapType> split_table;

  void run_internal() override;
  void finishAddingDependency() override;

public:
  SplitFlatBy(VariableName split_by_what);
};

class Merge : public BigOperator {

  void run_internal() override;
  void finishAddingDependency() override;

public:
  Merge();
};