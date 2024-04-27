#pragma once

#include <filesystem>

#include "common.hh"
#include "disk_array.hh"
#include "meta/schema.hh"
#include "meta/type.hh"
#include "utils.hh"

enum class EdgeStoreFormat {
  CSR,
  Flat,
};

using OffsetArrayRef = std::pair<Offset *, size_t>;
struct CSREdgeListRef {
  OffsetArrayRef index;
  VIDArrayRef edges;
};
struct FlatEdgeListRef {
  VIDPair *start;
  size_t len;
};

struct ExpandEdgeParam {
  TypeName start_type, target_type, edge_type;
  Direction dir = Direction::Undecide;
  EdgeSchema es;

  ExpandEdgeParam() = default;
  inline ExpandEdgeParam(TypeName start_type, TypeName target_type,
                         TypeName edge_type,
                         Direction dir = Direction::Undecide)
      : start_type(start_type), target_type(target_type), edge_type(edge_type),
        dir(dir), es(EdgeSchema()) {}
};
using ExpandEdgeParams = std::vector<ExpandEdgeParam>;

struct MultipleStepExpandParams {
  std::vector<ExpandEdgeParams> multi_params;
  size_t k_min, k_max;

  // MultipleStepExpandParams() = default;
  MultipleStepExpandParams(size_t k_min, size_t k_max);

  MultipleStepExpandParams
  append_expand_edge_param_for_every(ExpandEdgeParam param);

  MultipleStepExpandParams
  set_expand_edge_params_all(ExpandEdgeParams params); // at start from 1

  MultipleStepExpandParams
  set_expand_edge_params_at(ExpandEdgeParams params,
                            size_t at); // at start from 1

  MultipleStepExpandParams
  set_expand_edge_param_at(ExpandEdgeParam param,
                           size_t at); // at start from 1
};

struct EdgesRefRequest {
  EdgeStoreFormat store_format;
  bool partitioned;
  FlatOrder flat_order;
  EdgesRefRequest() = default;
  inline static EdgesRefRequest CSRRequest() {
    EdgesRefRequest re;
    re.store_format = EdgeStoreFormat::CSR;
    return re;
  };
  inline static EdgesRefRequest FlatRequest(FlatOrder order, bool partitioned) {
    EdgesRefRequest re;
    re.store_format = EdgeStoreFormat::Flat;
    re.flat_order = order;
    re.partitioned = partitioned;
    return re;
  };
};

struct EdgesRef {
  EdgeStoreFormat format;
  CSREdgeListRef csr;
  FlatEdgeListRef flat;

  inline Offset *index() const { return csr.index.first; }
  inline VID *csr_data() const { return csr.edges.first; }
  inline VIDPair *flat_data() const { return flat.start; }
  inline size_t edge_count() const {
    switch (format) {
    case EdgeStoreFormat::CSR:
      return csr.edges.second;
    case EdgeStoreFormat::Flat:
      return flat.len;
    default: {
      assert(0);
    }
    }
  }
};

// impl libfmt for EdgesRef
template <> struct fmt::formatter<EdgesRef> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const EdgesRef &p, FormatContext &ctx) {
    if (p.format == EdgeStoreFormat::CSR) {
      return format_to(ctx.out(), "CSR EdgesRef({})", p.edge_count());
    }
    if (p.format == EdgeStoreFormat::Flat) {
      return format_to(ctx.out(), "Flat EdgesRef({})", p.edge_count());
    }
    return format_to(ctx.out(), "EdgesRef({})", p.edge_count());
  }
};

class Graph {
  std::filesystem::path dir;

  std::vector<EdgesRef> get_csr_edge_refs(EdgeSchema es, Direction dir);
  std::vector<EdgesRef> get_flat_edge_refs(EdgeSchema es, Direction dir,
                                           FlatOrder order,
                                           std::optional<size_t> pcnt);

public:
  GraphSchema schema;
  VariableTable variable_table;
  std::vector<DiskArray<NoType>> variables;

  std::mt19937 gen = std::mt19937(123);

  Graph(std::filesystem::path dir);
  void load();

  std::vector<EdgesRef> get_edges_refs(std::vector<ExpandEdgeParam> edge_params,
                                       EdgesRefRequest format);

  std::vector<EdgesRef> get_edges_refs(EdgeSchema es, EdgesRefRequest format,
                                       Direction dir);

  inline DiskArray<NoType> &get_variable(std::string name) {
    size_t idx = variable_table.at(name).location;
    return variables.at(idx);
  }

  template <typename T> T *get_variable_start(std::string name) {
    size_t idx = variable_table.at(name).location;
    SPDLOG_TRACE("Get variable {} start ptr at {}", name,
                 (void *)(&variables[idx][0]));
    return reinterpret_cast<T *>(&variables.at(idx)[0]);
  }

  template <typename T> const T &get_variable_at(std::string name, size_t i) {
    size_t idx = variable_table.at(name).location;
    return variables.at(idx)[i];
  }
};
