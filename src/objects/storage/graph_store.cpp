#include "storage/graph_store.hh"
#include "operator/physical_operator.hh"
#include "utils/csv.hh"
#include "utils/timer.hh"
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;

MultipleStepExpandParams::MultipleStepExpandParams(size_t k_min, size_t k_max)
    : k_min(k_min), k_max(k_max) {
  multi_params.resize(k_max);
}

MultipleStepExpandParams
MultipleStepExpandParams::append_expand_edge_param_for_every(
    ExpandEdgeParam param) {
  for (auto &p : multi_params) {
    p.push_back(param);
  }
  return *this;
}

MultipleStepExpandParams
MultipleStepExpandParams::set_expand_edge_params_at(ExpandEdgeParams params,
                                                    size_t at) {
  multi_params[at - 1] = params;
  return *this;
}

MultipleStepExpandParams
MultipleStepExpandParams::set_expand_edge_params_all(ExpandEdgeParams params) {
  for (auto &x : multi_params) {
    x = params;
  }
  return *this;
}

MultipleStepExpandParams
MultipleStepExpandParams::set_expand_edge_param_at(ExpandEdgeParam param,
                                                   size_t at) {
  multi_params[at - 1] = {param};
  return *this;
}

Graph::Graph(filesystem::path dir) : dir(dir) {}

void Graph::load() {
  SPDLOG_INFO("Loading Graph from {}", dir);
  // Load schema.json
  json data = json::parse(ifstream(dir / "schema.json"));
  // cout << setw(4) << data;
  // schema.load_from(data);
  schema = data;

  variable_table = schema.generate_variable_table();

  variables.resize(variable_table.size());
  for (const auto &[name, type_loc] : variable_table) {
    const auto &[type, loc] = type_loc;
    variables[loc] = DiskArray<NoType>(dir / name, DiskArrayMode::ReadOnly);
    variables[loc].set_sizeof_data(sizeof_type(type));
    variables[loc].load_to_memory();
  }

  if (PhysicalOperator::graph_store != nullptr) {
    SPDLOG_WARN("Override Existing Graph!!!");
  }

  PhysicalOperator::graph_store = this;

  SPDLOG_INFO("Graph load complete");
}

vector<EdgesRef> Graph::get_csr_edge_refs(EdgeSchema es, Direction dir) {
  vector<EdgesRef> re;
  auto [add_src_dst, add_dst_src] = es.check_src_or_dst_for_edge_ref(dir);

  if (add_src_dst) {
    SPDLOG_INFO("Getting Forward CSR");
    EdgesRef ref;
    ref.format = EdgeStoreFormat::CSR;
    ref.csr.index.first =
        get_variable_start<Offset>(es.get_src_dst_csr_index_name());
    ref.csr.index.second = schema.get_vertex_schema(es.dst).get_count() + 1;
    ref.csr.edges.first =
        get_variable_start<VID>(es.get_src_dst_csr_data_name());
    ref.csr.edges.second = es.get_count();
    re.push_back(ref);
  }

  if (add_dst_src) {
    SPDLOG_INFO("Getting Backward CSR");
    EdgesRef ref;
    ref.format = EdgeStoreFormat::CSR;
    ref.csr.index.first =
        get_variable_start<Offset>(es.get_dst_src_csr_index_name());
    ref.csr.index.second = schema.get_vertex_schema(es.src).get_count() + 1;
    ref.csr.edges.first =
        get_variable_start<VID>(es.get_dst_src_csr_data_name());
    ref.csr.edges.second = es.get_count();
    re.push_back(ref);
  }
  return re;
}

vector<EdgesRef> Graph::get_flat_edge_refs(EdgeSchema es, Direction dir,
                                           FlatOrder order,
                                           optional<size_t> pcnt) {
  vector<EdgesRef> re;
  auto [add_src_dst, add_dst_src] = es.check_src_or_dst_for_edge_ref(dir);

  std::vector<Direction> dirs;
  if (add_src_dst)
    dirs.push_back(Direction::Forward);
  if (add_dst_src)
    dirs.push_back(Direction::Backward);

  for (auto dir : dirs) {
    if (pcnt.has_value()) {
      for (size_t i = 0; i < pcnt.value(); i++) {
        EdgesRef ref;
        ref.format = EdgeStoreFormat::Flat;
        auto name = es.get_flat_edges_name(dir, order, i);
        ref.flat.start = get_variable_start<VIDPair>(name);
        ref.flat.len = get_variable(name).size();
        re.push_back(ref);
      }
    } else {
      EdgesRef ref;
      ref.format = EdgeStoreFormat::Flat;
      ref.flat.start = get_variable_start<VIDPair>(
          es.get_flat_edges_name(dir, order, nullopt));
      ref.flat.len = es.get_count();
      re.push_back(ref);
    }
  }

  return re;
}

vector<EdgesRef> Graph::get_edges_refs(vector<ExpandEdgeParam> edge_params,
                                       EdgesRefRequest req) {
  vector<EdgesRef> re;
  std::string debug_output = "";
  for (auto &edge_param : edge_params) {
    debug_output +=
        fmt::format("{} {}, ", edge_param.edge_type, edge_param.dir);
    auto new_refs = get_edges_refs(edge_param.es, req, edge_param.dir);
    re.insert(re.end(), new_refs.begin(), new_refs.end());
  }
  SPDLOG_INFO("Get Edge Refs: {}", debug_output);
  return re;
}

vector<EdgesRef> Graph::get_edges_refs(EdgeSchema es, EdgesRefRequest req,
                                       Direction dir) {
  // Timer t("get edges refs");
  vector<EdgesRef> re;

  switch (req.store_format) {

  case EdgeStoreFormat::CSR: {
    auto refs = get_csr_edge_refs(es, dir);
    re.insert(re.end(), refs.begin(), refs.end());
    break;
  }
  case EdgeStoreFormat::Flat: {
    optional<size_t> pcnt;
    if (req.partitioned) {
      pcnt = es.store_info.get_partition_count(req.flat_order);
    } else {
      pcnt = nullopt;
    }
    auto refs = get_flat_edge_refs(es, dir, req.flat_order, pcnt);
    re.insert(re.end(), refs.begin(), refs.end());
    break;
  }
  default: {
    assert(0);
  }
  }

  // for (auto &r : re) {
  //   SPDLOG_INFO("{}", r);
  // }

  return re;
}

TEST(GraphStore, load) {
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();
}
