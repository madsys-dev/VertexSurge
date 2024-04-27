#include "plan/physical_plan.hh"
#include "plan/usual_pattern.hh"
using namespace std;

PhysicalPlan::PhysicalPlan() : PhysicalPlan(nullptr){};
PhysicalPlan::PhysicalPlan(SharedBigOperator op) : root(op), quite(false) {}

PhysicalPlan PhysicalPlan::inline_vid(VariableName name,
                                      std::vector<VID> vids) {
  return PhysicalPlan(::inlined_vid(name, vids));
}

PhysicalPlan PhysicalPlan::inline_vid_range(VariableName name,
                                            Range<VID> vids) {
  return PhysicalPlan(::inlined_vid_range(name, vids));
}

PhysicalPlan PhysicalPlan::csr_expand_to_neighbor(
    VariableName start_name, VariableName target_name, TypeName src_type,
    TypeName dst_type, TypeName edge_type, Direction dir) {
  return PhysicalPlan(::csr_expand_to_neighbor(
      root, start_name, target_name, src_type, dst_type, edge_type, dir));
}

PhysicalPlan PhysicalPlan::merge_flat(PhysicalPlan other) {
  auto merge = std::shared_ptr<BigOperator>(new Merge());
  merge->addDependency(root);
  merge->addLastDependency(other.root);
  return PhysicalPlan(merge);
}

PhysicalPlan PhysicalPlan::find_shortest(VariableName start_name,
                                         VariableName target_name,
                                         TypeName src_type, TypeName dst_type,
                                         TypeName edge_type, Direction dir) {
  auto shortest_vexpand = std::shared_ptr<BigOperator>(new VExpand(
      src_type, dst_type, edge_type, dir, VExpandPathParameters::FindShortest(),
      ExpandStrategy::Dense, start_name, target_name));
  shortest_vexpand->addLastDependency(root);

  return PhysicalPlan(shortest_vexpand);
}

PhysicalPlan PhysicalPlan::vexpand(VariableName start_name,
                                   VariableName target_name, TypeName src_type,
                                   TypeName dst_type, TypeName edge_type,
                                   VExpandPathParameters path_params,
                                   size_t k_min, size_t k_max, Direction dir) {

  return PhysicalPlan(::vexpand(root, start_name, target_name, src_type,
                                dst_type, edge_type, path_params, k_min, k_max,
                                dir));
}

PhysicalPlan PhysicalPlan::vexpand_multiple_edges(
    VariableName start_name, VariableName target_name,
    vector<ExpandEdgeParam> edge_params, VExpandPathParameters path_params,
    size_t k_min, size_t k_max) {
  auto ve = shared_ptr<BigOperator>(
      new VExpand(edge_params, k_min, k_max, path_params, ExpandStrategy::Dense,
                  start_name, target_name));
  ve->addLastDependency(root);
  return PhysicalPlan(ve);
}

PhysicalPlan PhysicalPlan::vexpand_different_edges_by_step(
    VariableName start_name, VariableName target_name,
    MultipleStepExpandParams multi_step_edge_params,
    VExpandPathParameters path_params) {
  auto ve = shared_ptr<BigOperator>(
      new VExpand(multi_step_edge_params, path_params, ExpandStrategy::Dense,
                  start_name, target_name));
  ve->addLastDependency(root);
  return PhysicalPlan(ve);
}

PhysicalPlan PhysicalPlan::convert_to_flat() {
  return PhysicalPlan(::convert_to_flat(root));
}

PhysicalPlan PhysicalPlan::fetch_vertex_property(
    VariableName vid_name, TypeName vertex_type, VariableName property_name,
    std::optional<VariableName> property_alias) {
  return PhysicalPlan(::fetch_vertex_property(root, vid_name, vertex_type,
                                              property_name, property_alias));
}

PhysicalPlan PhysicalPlan::projection(Projection::MapPairs map_pairs) {

  return PhysicalPlan(::projection(root, map_pairs));
}

PhysicalPlan PhysicalPlan::remove_variable(VariableName name) {
  return PhysicalPlan(::remove_variable(root, name));
}

PhysicalPlan PhysicalPlan::sum_group_by(VariableName by_what,
                                        VariableName sum_what) {
  return PhysicalPlan(::sum_group_by(root, by_what, sum_what));
}

PhysicalPlan PhysicalPlan::split_flat_by(VariableName by_what) {
  auto split_by = std::shared_ptr<BigOperator>(new SplitFlatBy(by_what));
  split_by->addLastDependency(root);
  return PhysicalPlan(split_by);
}

PhysicalPlan PhysicalPlan::sort_by([[maybe_unused]] VariableName by_what) {
  assert(0);
}

void PhysicalPlan::run_quite() {
  quite = true;
  run();
}

void PhysicalPlan::run_print_all() {
  output_limit = nullopt;
  run();
}

void PhysicalPlan::run() {
  if (quite == false)
    root->set_total_time_on();
  root->run();

  if (quite == false) {

    root->get_merged_prof();
    root->get_prof().report();

    SPDLOG_INFO(
        "QueryUsedTime: {}",
        root->get_prof()
            .get_global_timer(
                PhysicalOperator::get_physical_operator_name(root.get()) +
                "[Total]")
            .elapsedMs());
    SPDLOG_INFO("{}, {} tuples", root->results_schema.columns,
                root->results->tuple_count());
    root->results->print(output_limit);
  }
}
