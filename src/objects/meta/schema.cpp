#include "meta/schema.hh"
#include <gtest/gtest.h>

using namespace std;

using json = nlohmann::json;

// void VertexSchema::load_from_json(nlohmann::json json) {
//   for (const auto &[name, type] : json["properties"].items()) {
//     properties[name] = {name, type_from_string(type)};
//   }
//   statistics.count = json["count"];
// }

// void EdgeSchema::load_from_json(nlohmann::json json) {
//   src = json["src"];
//   dst = json["dst"];

//   for (const auto &[name, type] : json["properties"].items()) {
//     properties[name] = {name, type_from_string(type)};
//   }

//   statistics.count = json["count"];
//   directed = json["directed"];
//   store_info.hilbert = json["hilbert"];
//   store_info.hilbert_partition = json["block_by_threads"];
// }

string EdgeSchema::get_forward_edge_simple_name() const {
  return fmt::format("{}_{}_{}", src, type, dst);
}
string EdgeSchema::get_backward_edge_simple_name() const {
  return fmt::format("{}_{}_{}", dst, type, src);
}

string EdgeSchema::get_forward_edge_name() const {
  if (directed) {
    return fmt::format("{}-{}->{}", src, type, dst);

  } else {
    return fmt::format("{}-{}-{}", src, type, dst);
  }
}
string EdgeSchema::get_src_dst_csr_data_name() const {
  return fmt::format("{}_csr_src_dst", get_forward_edge_name());
}

string EdgeSchema::get_src_dst_csr_index_name() const {
  return fmt::format("{}_index", get_src_dst_csr_data_name());
}

string EdgeSchema::get_backward_edge_name() const {
  if (directed) {
    return fmt::format("{}<-{}-{}", dst, type, src);
  } else {
    return fmt::format("{}-{}-{}", dst, type, src);
  }
}

string EdgeSchema::get_dst_src_csr_data_name() const {
  return fmt::format("{}_csr_dst_src", get_backward_edge_name());
}

string EdgeSchema::get_dst_src_csr_index_name() const {
  return fmt::format("{}_index", get_dst_src_csr_data_name());
}

string
EdgeSchema::get_flat_edges_name(Direction dir,
                                EdgeSchema::StorageInfo::FlatEdgeOrder order,
                                std::optional<size_t> index) const {
  string edge_name, order_suffix, index_suffix;
  switch (dir) {

  case Direction::Forward:
    edge_name = get_forward_edge_name();
    break;
  case Direction::Backward:
    edge_name = get_backward_edge_name();
    break;
  case Direction::Both:
    assert(0);
    break;
  default:
    assert(0);
  }

  order_suffix = fmt::format("_{}", EdgeSchema::StorageInfo::to_string(order));

  if (index.has_value()) {
    index_suffix = fmt::format("_{}", index.value());
  } else {
    index_suffix = "";
  }

  return edge_name + order_suffix + index_suffix;
}

string EdgeSchema::get_forward_hilbert_name() const {
  return get_flat_edges_name(Direction::Forward,
                             EdgeSchema::StorageInfo::FlatEdgeOrder::Hilbert,
                             nullopt);
}

string EdgeSchema::get_backward_hilbert_name() const {
  return get_flat_edges_name(Direction::Backward,
                             EdgeSchema::StorageInfo::FlatEdgeOrder::Hilbert,
                             nullopt);
}

string EdgeSchema::get_forward_flat_name() const {
  return fmt::format("{}_flat", get_forward_edge_name());
}

string EdgeSchema::get_backward_flat_name() const {
  return fmt::format("{}_flat", get_backward_edge_name());
}

string EdgeSchema::get_forward_hilbert_partition_name(size_t i) const {
  return fmt::format("{}_{}", get_forward_hilbert_name(), i);
}

string EdgeSchema::get_backward_hilbert_partition_name(size_t i) const {
  return fmt::format("{}_{}", get_backward_hilbert_name(), i);
}

string EdgeSchema::get_forward_flat_partition_name(size_t i) const {
  return fmt::format("{}_{}", get_forward_flat_name(), i);
}

VertexSchema::VertexSchema(std::string type) : type(type) {}
EdgeSchema::EdgeSchema(std::string type) : type(type) {}

// void GraphSchema::load_from(json schema) {
//   graph_name = schema["graph_name"];
//   graph_type = schema["graph_type"];

//   for (const auto &[type, info] : schema["Vertex"].items()) {
//     VertexSchema vs(type);
//     vs.load_from_json(info);
//     vertex.push_back(vs);
//     SPDLOG_INFO("Vertex {} loaded, properties {}", vs.type, vs.properties);
//   }

//   for (const auto &[type, info] : schema["Edge"].items()) {
//     EdgeSchema es(type);
//     es.load_from_json(info);
//     edge.push_back(es);
//     SPDLOG_INFO("Edge ({})-[{}]-({}) loaded, directed: {}, properties {}, has
//     "
//                 "hilbert: {}, block hilbert: {}",
//                 es.src, es.type, es.dst, es.directed, es.properties,
//                 es.store_info.hilbert,
//                 es.store_info.hilbert_partition.has_value()
//                     ? es.store_info.hilbert_partition.value()
//                     : -1);
//   }
// }

VariableTable GraphSchema::generate_variable_table() {
  VariableTable table;
  size_t location = 0;
  for (const auto &vs : vertex) {
    for (const auto &[name, vas] : vs.properties) {
      auto variable_name = fmt::format("{}.{}", vs.type, name);
      table[variable_name] = {vas.type, location++};
    }
  }

  for (const auto &es : edge) {
    auto [src_dst, dst_src] = es.src_or_dst_for_gen();
    if (src_dst) {
      table[es.get_src_dst_csr_index_name()] = {Type::Offset, location++};
      table[es.get_src_dst_csr_data_name()] = {Type::VID, location++};
      for (auto order : es.store_info.flats) {
        table[es.get_flat_edges_name(Direction::Forward, order, nullopt)] = {
            Type::VIDPair, location++};
      }
      for (size_t oi = 0; oi < es.store_info.partitioned_flats.size(); oi++) {
        auto order = es.store_info.partitioned_flats[oi];
        auto cnt = es.store_info.partition_counts[oi];
        for (size_t i = 0; i < cnt; i++) {
          table[es.get_flat_edges_name(Direction::Forward, order, i)] = {
              Type::VIDPair, location++};
        }
      }
    }

    if (dst_src) {
      table[es.get_dst_src_csr_index_name()] = {Type::Offset, location++};
      table[es.get_dst_src_csr_data_name()] = {Type::VID, location++};
      for (auto order : es.store_info.flats) {
        table[es.get_flat_edges_name(Direction::Backward, order, nullopt)] = {
            Type::VIDPair, location++};
      }
      for (size_t oi = 0; oi < es.store_info.partitioned_flats.size(); oi++) {
        auto order = es.store_info.partitioned_flats[oi];
        auto cnt = es.store_info.partition_counts[oi];
        for (size_t i = 0; i < cnt; i++) {
          table[es.get_flat_edges_name(Direction::Backward, order, i)] = {
              Type::VIDPair, location++};
        }
      }
    }
  }
  // SPDLOG_INFO("Variable Table: {}", table);
  return table;
}

VertexSchema &GraphSchema::get_vertex_schema(std::string name) {
  for (auto &vs : vertex) {
    if (vs.type == name) {
      return vs;
    }
  }
  throw std::runtime_error(fmt::format("Vertex schema {} not found", name));
}

EdgeSchema &GraphSchema::get_edge_schema(std::string edge_simple_name) {
  for (auto &es : edge) {
    if (es.get_forward_edge_simple_name() == edge_simple_name ||
        es.get_backward_edge_simple_name() == edge_simple_name) {
      return es;
    }
  }
  throw std::runtime_error(
      fmt::format("Edge schema {} not found", edge_simple_name));
}

void EdgeSchema::StorageInfo::add_flat(FlatEdgeOrder order,
                                       std::optional<size_t> pcnt) {
  if (pcnt.has_value()) {
    for (auto &f : partitioned_flats) {
      if (f == order) {
        assert(0);
      }
    }
    partitioned_flats.push_back(order);
    partition_counts.push_back(pcnt.value());

  } else {
    for (auto &f : flats) {
      if (f == order) {
        SPDLOG_WARN("Flat {} already exists", to_string(order));
        return;
      }
    }
    flats.push_back(order);
  }
}

void EdgeSchema::StorageInfo::add_hilbert(std::optional<size_t> pcnt) {
  add_flat(EdgeSchema::StorageInfo::FlatEdgeOrder::Hilbert, pcnt);
}

void EdgeSchema::StorageInfo::add_src_dst(std::optional<size_t> pcnt) {
  add_flat(EdgeSchema::StorageInfo::FlatEdgeOrder::SrcDst, pcnt);
}

std::pair<bool, bool>
EdgeSchema::check_src_or_dst_for_edge_ref(Direction dir) const {
  pair<bool, bool> re = {false, false};
  auto &[add_src_dst, add_dst_src] = re;
  if (directed) {
    if (is_self_edge()) {
      if (dir == Direction::Forward || dir == Direction::Both) {
        // Account - transfer -> Account
        add_src_dst = true;
      }
      if (dir == Direction::Backward || dir == Direction::Both) {
        // Account <- transfer - Account
        add_dst_src = true;
      }
    } else {
      if (dir == Direction::Forward) {
        // Loan - deposit -> Account
        add_src_dst = true;
      }
      if (dir == Direction::Backward) {
        // Account <- deposit - Loan
        add_dst_src = true;
      }
      if (dir == Direction::Both) {
        SPDLOG_ERROR("Directed non-self edge should not be Both");
        assert(0);
      }
    }
  } else {
    // Direction no effect
    if (is_self_edge()) {
      // Person - knows - Person
      add_src_dst = true;
    } else {
      // Rarely Used
      // Boy - relationship - Girl
      add_src_dst = true;
      SPDLOG_WARN("Undirected Not Self Edge Is Rarely Used!");
      assert(0);
    }
  }
  return re;
}

std::pair<bool, bool> EdgeSchema::src_or_dst_for_gen() const {
  std::pair<bool, bool> re = {false, false};
  auto &[src_dst, dst_src] = re;

  if (is_self_edge()) {
    if (directed) {
      // Account - transfer -> Account
      src_dst = true;

      // Account <- transfer - Account
      dst_src = true;
    } else {
      // Person - knows - Person
      src_dst = true;
    }
  } else {
    if (directed) {
      // Loan - deposit -> Account
      src_dst = true;

      // Account <- deposit - Loan
      dst_src = true;
    } else {
      // Rare
      src_dst = true;
      assert(0);
    }
  }
  return re;
}

Direction EdgeSchema::give_direction(TypeName from_where) const {
  if (directed) {
    if (is_self_edge()) {
      // Need a direction
      SPDLOG_ERROR(
          "Cannot give direction for self edge, please specify directions");
      assert(0);
    } else {
      if (from_where == src) {
        return Direction::Forward;
      } else if (from_where == dst) {
        return Direction::Backward;
      } else {
        assert(0);
      }
    }
  } else {
    return Direction::Both;
  }
}

void EdgeSchema::check_direction(Direction dir, TypeName from_where) const {
  switch (dir) {

  case Direction::Forward: {

    break;
  }
  case Direction::Backward: {
    assert(from_where == dst);
    break;
  }
  case Direction::Both: {
    assert(from_where == src);
    assert(from_where == dst);
    break;
  }
  case Direction::Undecide: {
    assert(0);
    break;
  }
  default: {
    assert(0);
  }
  }
}

size_t EdgeSchema::StorageInfo::get_partition_count(FlatOrder order) {
  for (size_t i = 0; i < partition_counts.size(); i++) {
    if (partitioned_flats[i] == order) {
      return partition_counts[i];
    }
  }
  assert(0);
}

std::string EdgeSchema::StorageInfo::to_string(const FlatEdgeOrder &order) {
  switch (order) {
  case FlatEdgeOrder::Hilbert:
    return "hilbert";
    break;
  case FlatEdgeOrder::SrcDst:
    return "src_dst";
    break;
  case FlatEdgeOrder::DstSrc:
    return "dst_src";
    break;
  case FlatEdgeOrder::Random:
    return "random";
    break;
  default:
    assert(0);
  }
}

void EdgeSchema::StorageInfo::to_json(nlohmann::json &j,
                                      const FlatEdgeOrder &order) {
  j = to_string(order);
}

void EdgeSchema::StorageInfo::from_json(const nlohmann::json &j,
                                        FlatEdgeOrder &order) {
  std::string s = j.get<std::string>();
  if (s == "hilbert") {
    order = FlatEdgeOrder::Hilbert;
  } else if (s == "src_dst") {
    order = FlatEdgeOrder::SrcDst;
  } else if (s == "dst_src") {
    order = FlatEdgeOrder::DstSrc;
  } else if (s == "random") {
    order = FlatEdgeOrder::Random;
  } else {
    assert(0);
  }
}

TEST(Schema, Load) {
  GTEST_SKIP_("Schema Changed, Test Obselete");
  GraphSchema s;
  json schema = json::parse(
      R"(
   {
    "Edge": {
        "knows": {
            "block_by_threads": 24,
            "count": 1017674,
            "directed": false,
            "dst": "Person",
            "hilbert": true,
            "properties": {},
            "src": "Person"
        }
    },
    "Vertex": {
        "Person": {
            "count": 75879,
            "properties": {
                "community": "TagValue8",
                "id": "VID",
                "name": "ShortString"
            }
        }
    },
    "name": "LDBC SNB",
    "type": "Social Network"
}
        )");
  SPDLOG_INFO("Test Load");
  s = schema;

  auto table = s.generate_variable_table();
  SPDLOG_INFO("Variable Table: {}", table);

  GTEST_ASSERT_EQ(table.size(), 24 * 2 + 2 + 2 + 3);
}

TEST(Schema, LDBC_FINBENCH) {
  GTEST_SKIP_("Schema Changed, Test Obselete");
  GraphSchema s;
  json schema = json::parse(
      R"(
   {
    "Edge": {
        "transfer": {
            "block_by_threads": 1,
            "count": 1017674,
            "directed": true,
            "dst": "Account",
            "hilbert": false,
            "properties": {},
            "src": "Account"
        },
        "signIn": {
            "block_by_threads": 1,
            "count": 1017674,
            "directed": true,
            "dst": "Account",
            "hilbert": false,
            "properties": {},
            "src": "Account"
        }
    },
    "Vertex": {
        "Account": {
            "count": 75879,
            "properties": {
                "community": "TagValue8",
                "id": "VID"
            }
        },
        "Medium": {
          "count": 12345,
           "properties": {
                "isBlocked": "TagValue8",
                "id": "VID",
                "type": "ShortString"
            }
        }
    },
    "name": "LDBC SNB",
    "type": "Social Network"
}
        )");
  s = schema;

  auto table = s.generate_variable_table();
  SPDLOG_INFO("Variable Table: {}", table);
}
