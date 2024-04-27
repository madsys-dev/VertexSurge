#include "operator/big_operators.hh"

using namespace std;

Scan::Scan(std::string vertex_type) : vertex_type(vertex_type) {
  vs = graph_store->schema.get_vertex_schema(vertex_type);
  for (const auto &[name, type] : vs.properties) {
    fetch_properties.push_back(name);
  }
  filters = {};
  set_table_schema();
}

Scan::Scan(std::string vertex_type, std::vector<std::string> fetch_properties,
           std::vector<VariableFilter> filters)
    : vertex_type(vertex_type), fetch_properties(fetch_properties),
      filters(filters) {
  vs = graph_store->schema.get_vertex_schema(vertex_type);
  set_table_schema();
}

void Scan::set_table_schema() {
  for (const auto &name : fetch_properties) {
    auto vas = vs.properties[name];
    results_schema.columns.push_back(
        {fmt::format("{}.{}", vertex_type, name), vas.type});
  }
  results = unique_ptr<Table>(new FlatTable(results_schema));
}

void Scan::run_internal() {
  for (const auto &f : filters) {
    filter_field_ptr.push_back(
        graph_store->get_variable_start<void>(f.vas.name));
  }

  for (const auto &[name, type] : results_schema.columns) {
    fetch_field_ptr.push_back(graph_store->get_variable_start<void>(name));
  }
  FlatTable *re = dynamic_cast<FlatTable *>(results.get());

  DiskTupleRef dt;
  for (size_t i = 0; i < results_schema.columns.size(); i++) {
    dt[i] = fetch_field_ptr[i];
  }

  for (size_t i = 0; i < vs.get_count(); i++) {
    bool filter_out = false;
    for (size_t j = 0; j < filters.size(); j++) {
      if (!filters[j].filter_at(filter_field_ptr[j], i)) {
        filter_out = true;
        break;
      }
    }
    if (filter_out)
      continue;
    re->push_back_tuple_ref(dt.offset(results_schema, i));
  }
}

TEST(Operator, Scan) {
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  Scan s("Person");
  s.run();
  auto flat = s.results->down_to_flat_table();
  SPDLOG_INFO("{}", flat->schema.columns);

  // for (size_t i = 0; i < flat->tuple_count(); i++) {
  // SPDLOG_INFO("{}", flat->tuple_to_string(i));
  // }

  s.get_merged_prof().report();
}
