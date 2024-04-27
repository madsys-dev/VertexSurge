#include "common.hh"
#include "operator/big_operators.hh"
#include "operator/big_operators2.hh"
#include "operator/big_operators3.hh"
#include "storage/graph_store.hh"

// Scan Person and filter on community, and projection to "name.id"

inline SharedBigOperator
scan_person_id_filter_on_community_no_filter(std::string name) {
  using namespace std;
  auto name_id = fmt::format("{}.id", name);
  auto scan_person1 = shared_ptr<BigOperator>(
      new Scan("Person", {"id"}, {{{"Person.community", Type::TagValue8}}}));

  auto p1_projection = shared_ptr<BigOperator>(
      new Projection({make_pair("Person.id", name_id)}));
  p1_projection->addLastDependency((scan_person1));
  return p1_projection;
}

inline SharedBigOperator scan_person_id_filter_on_community(std::string name,
                                                            ValueFilterFn f) {
  using namespace std;
  auto name_id = fmt::format("{}.id", name);
  auto scan_person1 = shared_ptr<BigOperator>(
      new Scan("Person", {"id"}, {{{"Person.community", Type::TagValue8}, f}}));

  auto p1_projection = shared_ptr<BigOperator>(
      new Projection({make_pair("Person.id", name_id)}));
  p1_projection->addLastDependency((scan_person1));
  return p1_projection;
}

inline SharedBigOperator
scan_person_id_filter_on_community_range(std::string name, Range<TagValue8> f) {
  using namespace std;
  auto name_id = fmt::format("{}.id", name);
  auto scan_person1 = shared_ptr<BigOperator>(
      new Scan("Person", {"id"}, {{{"Person.community", Type::TagValue8}, f}}));

  auto p1_projection = shared_ptr<BigOperator>(
      new Projection({make_pair("Person.id", name_id)}));
  p1_projection->addLastDependency((scan_person1));
  return p1_projection;
}

inline SharedBigOperator
scan_vertex_id_with_filters(TypeName vertex_type_name,
                            VariableName variable_name,
                            std::vector<VariableFilter> filters) {
  using namespace std;
  auto name_id = fmt::format("{}.id", variable_name);
  auto ids =
      shared_ptr<BigOperator>(new Scan(vertex_type_name, {"id"}, filters));
  auto projection = shared_ptr<BigOperator>(new Projection(
      {make_pair(fmt::format("{}.id", vertex_type_name), name_id)}));
  projection->addLastDependency(ids);
  return projection;
}

// Scan Account and filter on community, and projection to "name.id"
inline SharedBigOperator scan_account_id(std::string name) {
  using namespace std;
  auto name_id = fmt::format("{}.id", name);
  auto scan_account = shared_ptr<BigOperator>(new Scan("Account", {"id"}));

  auto p1_projection = shared_ptr<BigOperator>(
      new Projection({make_pair("Account.id", name_id)}));
  p1_projection->addLastDependency((scan_account));
  return p1_projection;
}

inline SharedBigOperator inlined_account_id(std::string name,
                                            std::vector<VID> &inlined_data) {
  using namespace std;
  // SPDLOG_WARN("inlined_data count {}",inlined_data.size());
  auto name_id = fmt::format("{}.id", name);
  auto inlined = shared_ptr<BigOperator>(new InlinedData);
  inlined->results_schema.columns = {{name_id, Type::VID}};
  inlined->results = unique_ptr<Table>(new FlatTable(inlined->results_schema));
  inlined->results->down_to_flat_table()->get_col_diskarray(0)->push_many_nt(
      inlined_data.data(), inlined_data.size());

  // for(auto &x:inlined_data){
  //   inlined->results->down_to_flat_table()->push_back_single(0, &x);
  // }

  return inlined;
}

// Expand from start to target and select target
inline SharedBigOperator
vexpand_knows(SharedBigOperator start_list, SharedBigOperator target_list,
              std::string start_name, std::string target_name, size_t k_min,
              size_t k_max, Direction dir = Direction::Both,
              ExpandPathType path_type = ExpandPathType::Shortest) {
  using namespace std;
  auto ve12 = shared_ptr<BigOperator>(
      new VExpand("Person", "Person", "knows", dir, k_min, k_max, path_type,
                  ExpandStrategy::Dense, start_name, target_name));
  ve12->addLastDependency(start_list);
  ve12->record_output_size = true;
  // return ve12;

  auto ve12_S = shared_ptr<BigOperator>(new MatrixTableSelect(target_name));
  ve12_S->addDependency(ve12);
  ve12_S->addLastDependency(target_list);
  return ve12_S;
}

// Expand from start to target and select target
inline SharedBigOperator inlined_vid(VariableName name, std::vector<VID> vids) {
  using namespace std;
  auto data = unique_ptr<BigOperator>(new InlinedData);

  data->results_schema.columns.push_back(ValueSchema{name, Type::VID});

  auto table = unique_ptr<Table>(new FlatTable(data->results_schema));

  auto &array = table->down_to_flat_table()->get_col_diskarray(0);
  array->push_many_nt(vids.data(), vids.size());

  data->results = std::move(table);
  return data;
}

inline SharedBigOperator inlined_vid_range(VariableName name,
                                           Range<VID> vid_range) {
  std::vector<VID> vids;
  for (VID v = vid_range.start; v <= vid_range.end; v++) {
    vids.push_back(v);
  }
  return inlined_vid(name, vids);
}

// Expand from start to target and select target
inline SharedBigOperator vexpand_transfer(SharedBigOperator start_list,
                                          SharedBigOperator target_list,
                                          std::string start_name,
                                          std::string target_name, size_t k_min,
                                          size_t k_max) {
  using namespace std;
  auto ve12 = shared_ptr<BigOperator>(
      new VExpand("Account", "Account", "transfer", Direction::Forward, k_min,
                  k_max, ExpandPathType::Shortest, ExpandStrategy::Dense,
                  start_name, target_name));
  ve12->addLastDependency(start_list);
  ve12->record_output_size = true;

  auto ve12_S = shared_ptr<BigOperator>(new MatrixTableSelect(target_name));
  ve12_S->addDependency(ve12);
  ve12_S->addLastDependency(target_list);
  return ve12_S;
}

inline void query_end(SharedBigOperator re) {
  re->set_total_time_on();
  re->run();

  re->get_merged_prof();
  re->get_prof().report();

  SPDLOG_INFO("QueryUsedTime: {}",
              re->get_prof()
                  .get_global_timer(
                      PhysicalOperator::get_physical_operator_name(re.get()) +
                      "[Total]")
                  .elapsedMs());
  SPDLOG_INFO("{}, {} tuples", re->results_schema.columns,
              re->results->tuple_count());
  re->results->print(std::nullopt);
}

inline SharedBigOperator
csr_expand_to_neighbor(SharedBigOperator start_op, VariableName start_name,
                       VariableName target_name, TypeName src_type,
                       TypeName dst_type, TypeName edge_type,
                       Direction dir = Direction::Undecide) {
  using namespace std;
  auto expand = shared_ptr<BigOperator>(
      new VExpand(src_type, dst_type, edge_type, dir, 1, 1, ExpandPathType::Any,
                  ExpandStrategy::CSR, start_name, target_name));
  expand->addLastDependency(start_op);
  return expand;
}

inline SharedBigOperator csr_expand_to_neighbor_with_distance(
    SharedBigOperator start_op, VariableName start_name,
    VariableName target_name, TypeName src_type, TypeName dst_type,
    TypeName edge_type, Direction dir = Direction::Undecide) {
  using namespace std;
  VExpandPathParameters path_params = VExpandPathParameters::Any();
  path_params.with_distance = true;
  auto expand = shared_ptr<BigOperator>(
      new VExpand(src_type, dst_type, edge_type, dir, 1, 1, path_params,
                  ExpandStrategy::CSR, start_name, target_name));
  expand->addLastDependency(start_op);
  return expand;
}

// Expand from start to target and select target
inline SharedBigOperator
vexpand(SharedBigOperator start_op, VariableName start_name,
        VariableName target_name, TypeName src_type, TypeName dst_type,
        TypeName edge_type, VExpandPathParameters path_params, size_t k_min,
        size_t k_max, Direction dir = Direction::Undecide) {
  using namespace std;
  auto ve12 = shared_ptr<BigOperator>(
      new VExpand(src_type, dst_type, edge_type, dir, k_min, k_max, path_params,
                  ExpandStrategy::Dense, start_name, target_name));
  ve12->addLastDependency(start_op);
  ve12->record_output_size = true;
  return ve12;
}

inline SharedBigOperator convert_to_flat(SharedBigOperator bt) {
  using namespace std;
  auto convert = shared_ptr<BigOperator>(new TableConvert(TableType::Flat));
  convert->addLastDependency(bt);
  return convert;
}

inline SharedBigOperator fetch_vertex_property(
    SharedBigOperator op, VariableName vid_name, TypeName vertex_type,
    VariableName property_name,
    std::optional<VariableName> property_alias = std::nullopt) {
  using namespace std;
  auto fetch = shared_ptr<BigOperator>(new FetchVertexProperty(
      vertex_type, vid_name, property_name, property_alias));
  fetch->addLastDependency(op);
  return fetch;
}

inline SharedBigOperator projection(SharedBigOperator op,
                                    Projection::MapPairs map_pairs) {
  using namespace std;
  auto drop = shared_ptr<BigOperator>(new Projection(map_pairs));
  drop->addLastDependency(op);
  return drop;
}

inline SharedBigOperator remove_variable(SharedBigOperator op,
                                         VariableName name) {
  using namespace std;
  Projection::MapPairs pairs;
  for (auto vs : op->results_schema.columns) {
    if (vs.name != name) {
      pairs.push_back(make_pair(vs.name, vs.name));
    }
  }
  auto drop = shared_ptr<BigOperator>(new Projection(pairs));
  drop->addLastDependency(op);
  return drop;
}

inline SharedBigOperator sum_group_by(SharedBigOperator op,
                                      VariableName by_what,
                                      VariableName sum_what) {
  using namespace std;
  auto group_by = shared_ptr<BigOperator>(
      new GroupByAggregation(by_what, sum_what, AggregationType::SUM));
  group_by->addLastDependency(op);
  return group_by;
}