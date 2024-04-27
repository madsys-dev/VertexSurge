#pragma once
#include <tbb/concurrent_hash_map.h>
#include <vector>

#include "common.hh"
#include "expand_impl.hh"
#include "physical_operator.hh"

/*

  This file declares 4 most core operators in the system:
  - Scan
  - VExpand
  - HashJoin
  - Mintersect

*/

struct VertexSelector {};

class Scan : public BigOperator {
  std::string vertex_type;                    // Person
  std::vector<VariableName> fetch_properties; // name
  std::vector<VariableFilter> filters;        // Person.name

  VertexSchema vs;
  std::vector<void *> filter_field_ptr;
  std::vector<void *> fetch_field_ptr;

  void run_internal() override;

  void set_table_schema();

public:
  Scan(std::string vertex_type);
  Scan(std::string vertex_type, std::vector<VariableName> fetch_properties,
       std::vector<VariableFilter> filters = {});
};

class VExpand : public BigOperator {

#ifdef USE_STACK_COLUMN_MAJOR

  using MTT = VarRowMatrixTable;
#ifdef USE_BLOCK_HILBERT
  const ExpandDenseStrategy dense_strategy = ExpandDenseStrategy::BlockHilbert;
#else
  const ExpandDenseStrategy dense_strategy = ExpandDenseStrategy::Hilbert;
#endif
#else
  using MT = DenseColMainMatrix;
  const ExpandDenseStrategy dense_strategy = ExpandDenseStrategy::Naive;

#endif
  using MT = MTT::UnderlyingBitMatrix;
  // config
  size_t k_min, k_max, k_actual_max;

  std::optional<TypeName> start_type, target_type;
  // TypeName start_type, target_type, edge_type;
  // Direction dir;
  MultipleStepExpandParams multi_step_edge_params;

  VariableName start_variable_name;
  std::optional<VariableName> target_variable_name;
  // ExpandPathType path_type;
  VExpandPathParameters path_params;
  ExpandStrategy strategy;

public:
  // derived
  // EdgeSchema es;
  std::optional<VertexSchema> ss, ts; // start schema, target schema

  FlatVariableRef start_variable;

  // find shortest
  FlatVariableRef target_variable;
  FlatVariableRef distance_variable;
  size_t target_found_count;

private:
  // data
  ExpandStatus<MT> status;
  size_t last_visit_count = 0;

  void merge_to_MT();
  void copy_to_multi_MT();
  void merge_to_Flat();

  void finishAddingDependency() override;
  void run_internal() override;
  bool after_child(size_t i) override;
  bool before_execution() override;

public:
  inline ~VExpand() override {
    SPDLOG_DEBUG("destroy {}",
                 PhysicalOperator::get_physical_operator_name(this));
  };
  VExpand(TypeName src_type, TypeName dst_type, TypeName edge_type,
          Direction dir, size_t k_min, size_t k_max, ExpandPathType path_type,
          ExpandStrategy strategy, VariableName variable_name,
          std::optional<VariableName> target_variable_name);

  // For FindShortest
  VExpand(TypeName src_type, TypeName dst_type, TypeName edge_type,
          Direction dir, VExpandPathParameters path_params,
          ExpandStrategy strategy, VariableName variable_name,
          VariableName target_variable_name);

  // The constructor for only one edge type
  VExpand(TypeName src_type, TypeName dst_type, TypeName edge_type,
          Direction dir, size_t k_min, size_t k_max,
          VExpandPathParameters path_params, ExpandStrategy strategy,
          VariableName variable_name,
          std::optional<VariableName> target_variable_name);

  // The total for same edge for multiple constructor
  VExpand(ExpandEdgeParams edge_params, size_t k_min, size_t k_max,
          VExpandPathParameters path_params, ExpandStrategy strategy,
          VariableName variable_name,
          std::optional<VariableName> target_variable_name);

  // The total constructor
  VExpand(MultipleStepExpandParams multi_step_edge_params,
          VExpandPathParameters path_params, ExpandStrategy strategy,
          VariableName variable_name,
          std::optional<VariableName> target_variable_name);
};

class HashJoin : public BigOperator {
  VariableName key_variable_name;
  bool only_count;

  tbb::concurrent_hash_map<TupleValue, std::vector<size_t>, TupleValueHash>
      flat_hash_map;

  tbb::concurrent_hash_map<VID, VID> col_main_hash_map;

  tbb::concurrent_hash_map<TupleValue, size_t, TupleValueHash>
      count_only_hash_map;

  Table *build_table, *probe_table;

  void run_internal() override;

  inline std::string short_description() override {
    return only_count ? "CountOnly" : "";
  }

  void build();
  void probe();

public:
  HashJoin(std::string key_variable_name, bool only_count); // Hash Join

  void finishAddingDependency() override;
};

class Mintersect : public BigOperator {
  VariableName new_vertex_var_name;
  TypeName new_vertex_type;
  bool only_count;
  VertexSchema vs;
  ValueSchema ks;

  std::vector<size_t> var_base_idx, edges_u_idx, edges_nv_idx;

  std::atomic_int64_t total_num = 0;

  void run_internal() override;

  inline std::string short_description() override {
    return only_count ? "CountOnly" : "";
  }

public:
  Mintersect(VariableName new_vertex_var_name, TypeName new_vertex_type,
             bool only_count);

  void finishAddingDependency() override;
};
