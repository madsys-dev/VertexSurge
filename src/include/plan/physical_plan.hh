#pragma once

#include "common.hh"
#include "operator/big_operators.hh"
#include "operator/big_operators2.hh"
#include "operator/big_operators3.hh"
#include "operator/physical_operator.hh"
struct PhysicalPlan {
  SharedBigOperator root;

  bool quite = false;
  std::optional<size_t> output_limit = 10;

  PhysicalPlan();
  PhysicalPlan(SharedBigOperator);

  // Build Plan

  PhysicalPlan inline_vid(VariableName name, std::vector<VID> vids);
  PhysicalPlan inline_vid_range(VariableName name, Range<VID> vid_range);

  PhysicalPlan merge_flat(PhysicalPlan other);

  PhysicalPlan csr_expand_to_neighbor(VariableName start_name,
                                      VariableName target_name,
                                      TypeName src_type, TypeName dst_type,
                                      TypeName edge_type,
                                      Direction dir = Direction::Undecide);

  PhysicalPlan vexpand(VariableName start_name, VariableName target_name,
                       TypeName src_type, TypeName dst_type, TypeName edge_type,
                       VExpandPathParameters path_params, size_t k_min,
                       size_t k_max, Direction dir = Direction::Undecide);

  PhysicalPlan vexpand_multiple_edges(VariableName start_name,
                                      VariableName target_name,
                                      std::vector<ExpandEdgeParam> edge_params,
                                      VExpandPathParameters path_params,
                                      size_t k_min, size_t k_max);

  PhysicalPlan vexpand_different_edges_by_step(
      VariableName start_name, VariableName target_name,
      MultipleStepExpandParams multi_step_edge_params,
      VExpandPathParameters path_params);

  PhysicalPlan find_shortest(VariableName start_name, VariableName target_name,
                             TypeName src_type, TypeName dst_type,
                             TypeName edge_type,
                             Direction dir = Direction::Undecide);

  template <MatrixTableFilterFn F> PhysicalPlan filter_matrix(F f);

  PhysicalPlan convert_to_flat();

  PhysicalPlan fetch_vertex_property(
      VariableName vid_name, TypeName vertex_type, VariableName property_name,
      std::optional<VariableName> property_alias = std::nullopt);

  PhysicalPlan projection(Projection::MapPairs map_pairs);

  PhysicalPlan remove_variable(VariableName name);

  PhysicalPlan sum_group_by(VariableName by_what, VariableName sum_what);

  PhysicalPlan split_flat_by(VariableName by_what);

  PhysicalPlan sort_by(VariableName by_what);

  // Set Run

  PhysicalPlan &set_quite();

  void run();
  void run_print_all();
  void run_quite();
};

// impls
template <MatrixTableFilterFn F> PhysicalPlan PhysicalPlan::filter_matrix(F f) {
  auto mf = std::shared_ptr<BigOperator>(new MatrixTableFilter<F>(f));
  mf->addLastDependency(root);
  return PhysicalPlan(mf);
}