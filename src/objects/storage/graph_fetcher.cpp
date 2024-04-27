#include "storage/graph_fetcher.hh"
#include "operator/physical_operator.hh"

using namespace std;

VertexPropertyFetcher::VertexPropertyFetcher(TypeName vertex_type,
                                             ValueSchema property)
    : ref(PhysicalOperator::graph_store->get_variable(
          vertex_property_name(vertex_type, property.name))),
      vertex_type(vertex_type), property(property) {}
void *VertexPropertyFetcher::get_as_ptr(VID id) { return ref.get_nt(id); }

TupleValue VertexPropertyFetcher::get_as_tv(VID id) {
  return TupleValue::construct_tuple_value(property.type, get_as_ptr(id));
}

VertexNeighborFetcher::VertexNeighborFetcher(TypeName start_type,
                                             TypeName neighbor_type,
                                             TypeName edge_type, Direction dir)
    : start_type(start_type), neighbor_type(neighbor_type),
      edge_type(edge_type), dir(dir) {
  string edge_name =
      fmt::format("{}_{}_{}", start_type, edge_type, neighbor_type);
  auto schema =
      PhysicalOperator::graph_store->schema.get_edge_schema(edge_name);
  if (this->dir == Direction::Undecide) {
    this->dir = schema.give_direction(start_type);
  }
  switch (this->dir) {
  case Direction::Forward: {
    data = PhysicalOperator::graph_store->get_variable_start<VID>(
        schema.get_src_dst_csr_data_name());
    index = PhysicalOperator::graph_store->get_variable_start<Offset>(
        schema.get_src_dst_csr_index_name());
    break;
  }
  case Direction::Backward: {
    data = PhysicalOperator::graph_store->get_variable_start<VID>(
        schema.get_dst_src_csr_data_name());
    index = PhysicalOperator::graph_store->get_variable_start<Offset>(
        schema.get_dst_src_csr_index_name());
    break;
  }
  default: {
    assert(0);
  }
  }
}

VertexNeighborFetcher::Iterator::Iterator(VertexNeighborFetcher &fetcher,
                                          Offset start, Offset end)
    : fetcher(fetcher), now(start), end(end) {}

bool VertexNeighborFetcher::Iterator::next(VID &x) {
  if (now < end) {
    x = fetcher.data[now];
    now++;
    return true;
  } else {
    return false;
  }
}

VertexNeighborFetcher::Iterator VertexNeighborFetcher::get_iterator(VID id) {
  return Iterator(*this, index[id], index[id + 1]);
}
