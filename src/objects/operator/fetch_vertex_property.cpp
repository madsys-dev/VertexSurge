#include "operator/big_operators3.hh"

using namespace std;

FetchVertexProperty::FetchVertexProperty(
    TypeName vertex_type, VariableName vid_variable, VariableName property_name,
    std::optional<VariableName> property_alias)
    : vertex_type(vertex_type), vid_variable(vid_variable),
      property_name(property_name) {
  if (property_alias.has_value()) {
    this->property_alias = property_alias.value();
  } else {
    this->property_alias = property_name;
  }
}

void FetchVertexProperty::finishAddingDependency() {
  assert(deps.size() == 1);
  assert(deps[0]->results_schema.get_schema(vid_variable).has_value());

  property_schema = graph_store->schema.get_vertex_schema(vertex_type)
                        .properties[property_name];
  property_alias_schema = property_schema;
  property_alias_schema.name = property_alias;

  results_schema = deps[0]->results_schema;
  results_schema.add_non_duplicated_variable(property_alias_schema);
}

void FetchVertexProperty::run_internal() {
  assert(dep_results[0]->type == TableType::Flat);
  results = dep_results[0];

  auto flat = results->down_to_flat_table();
  auto [vid_schema, vid_at] =
      dep_results[0]->schema.get_schema(vid_variable).value();

  auto c = flat->extend_horizontal(property_alias_schema);
  VariableName vertex_property =
      fmt::format("{}.{}", vertex_type, property_name);
  auto &p = graph_store->get_variable(vertex_property);

  // #pragma omp parallel for //enable this will cause clangd to crash, I
  // do not understand
  for (size_t r = 0; r < flat->tuple_count(); r++) {
    VID vid = flat->get_T<VID>(r, vid_at);
    flat->set(r, c, p.get_nt(vid));
  }
}
