#pragma once

#include <map>
#include <nlohmann/json.hpp>
#include <vector>

#include "common.hh"
#include "meta/type.hh"

struct VertexSchema {
  // static infomation
  TypeName type;
  std::map<VariableName, ValueSchema> properties;

  struct Statistics {
    size_t count;
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(Statistics, count);
  } statistics;

  VertexSchema() = default;
  VertexSchema(std::string type);
  // void load_from_json(nlohmann::json json);
  inline size_t get_count() const { return statistics.count; }
  inline void add_property(VariableName property_name, Type property_type) {
    assert(properties.find(property_name) == properties.end());
    properties[property_name] = ValueSchema{property_name, property_type};
  }

  inline bool operator==(const VertexSchema &rhs) const {
    return type == rhs.type && properties == rhs.properties;
  }

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(VertexSchema, type, statistics, properties);
};

struct EdgeSchema {
  // Static Infomation
  TypeName type;
  bool directed;
  bool allow_multiple_edge;
  TypeName src, dst;
  std::map<std::string, ValueSchema> properties;

  struct Statistics {
    size_t count;
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(Statistics, count);
  } statistics;

  struct StorageInfo {
    enum class FlatEdgeOrder {
      Hilbert,
      SrcDst,
      DstSrc,
      Random,
    };

    static std::string to_string(const FlatEdgeOrder &order);
    void to_json(nlohmann::json &j, const FlatEdgeOrder &order);
    void from_json(const nlohmann::json &j, FlatEdgeOrder &order);

    std::vector<FlatEdgeOrder> flats;
    std::vector<FlatEdgeOrder> partitioned_flats;
    std::vector<size_t> partition_counts;

    void add_flat(FlatEdgeOrder order, std::optional<size_t> pcnt);
    void add_hilbert(std::optional<size_t> pcnt);
    void add_src_dst(std::optional<size_t> pcnt);

    size_t get_partition_count(FlatEdgeOrder order);

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(StorageInfo, flats, partitioned_flats,
                                   partition_counts);
  } store_info;

  EdgeSchema() = default;
  EdgeSchema(std::string type);
  // void load_from_json(nlohmann::json json);
  inline size_t get_count() const { return statistics.count; }
  inline bool is_self_edge() const { return src == dst; }
  inline void add_property(VariableName property_name, Type property_type) {
    assert(properties.find(property_name) == properties.end());
    properties[property_name] = ValueSchema{property_name, property_type};
  }
  std::pair<bool, bool> check_src_or_dst_for_edge_ref(Direction dir) const;
  std::pair<bool, bool> src_or_dst_for_gen() const;
  Direction give_direction(TypeName from_where) const;
  void check_direction(Direction dir, TypeName from_where) const;

  std::string get_forward_edge_simple_name() const;
  std::string get_backward_edge_simple_name() const;

  std::string get_forward_edge_name() const;
  std::string get_backward_edge_name() const;

  std::string get_src_dst_csr_index_name() const;
  std::string get_src_dst_csr_data_name()
      const; // this is dst, because the data from src is dst

  std::string get_dst_src_csr_index_name() const;
  std::string get_dst_src_csr_data_name()
      const; // this is src, because the data from dst is src

  std::string get_flat_edges_name(Direction dir,
                                  StorageInfo::FlatEdgeOrder order,
                                  std::optional<size_t> index) const;

  std::string get_forward_hilbert_name() const;
  std::string get_backward_hilbert_name() const;
  std::string get_forward_hilbert_partition_name(size_t index) const;
  std::string get_backward_hilbert_partition_name(size_t index) const;

  std::string get_forward_flat_name() const;
  std::string get_backward_flat_name() const;
  std::string get_forward_flat_partition_name(size_t index) const;
  std::string get_backward_flat_partition_name(size_t index) const;

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(EdgeSchema, type, directed,
                                 allow_multiple_edge, src, dst, properties,
                                 statistics, store_info);
};

using VariableTable = std::map<std::string, TypeLocation>;
using FlatOrder = EdgeSchema::StorageInfo::FlatEdgeOrder;
class GraphSchema {
public:
  std::string graph_name;
  std::string graph_type;

  std::vector<VertexSchema> vertex;
  std::vector<EdgeSchema> edge;

  // void load_from(nlohmann::json schema);
  VariableTable generate_variable_table();

  VertexSchema &get_vertex_schema(std::string name);
  EdgeSchema &get_edge_schema(std::string full_edge_name);

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(GraphSchema, graph_name, graph_type, vertex,
                                 edge);
};

class ImportConfig {
public:
  std::string input_dir;
  std::string output_dir;

  std::string input_dataset_name;
  std::string output_graph_type;

  std::string target_database_name;
};
