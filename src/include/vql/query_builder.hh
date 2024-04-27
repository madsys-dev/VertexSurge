#pragma once

#include "antlr4-runtime.h"

#include "common.hh"
#include "meta/type.hh"
#include "operator/expand.hh"
#include "operator/physical_operator.hh"
#include <string>

#include "grammar/vqlBaseListener.h"
#include "grammar/vqlLexer.h"
#include "grammar/vqlParser.h"

namespace query {

enum class VertexFilterType {
  ID,
  TAG,
  ALL,
};

struct Vertex {
  VariableName name;

  VertexFilterType filter_type;
  VID id;
  Range<TagValue8> tag;

  void merge(const Vertex &v);

  std::function<bool(void *v)> property_filter();
};

struct Edge {
  VariableName src, dst;
  Direction dir;
  ExpandPathType path_type;
  Range<size_t> variable_length;
};

struct Variable {
  VariableName vname;
  std::optional<VariableName> property;
};

class Builder {
  std::string query;

  // for parse
  std::vector<Vertex> vertices;
  std::vector<Edge> edges;
  std::vector<Variable> return_vertices;

public:
  bool only_count;

private:
  // for build
  using JoinOrder = std::vector<VariableName>;
  std::vector<std::shared_ptr<BigOperator>> vertex_scan;
  std::map<std::tuple<VariableName, VariableName, TableType>,
           std::shared_ptr<BigOperator>>
      ves;

  std::shared_ptr<BigOperator> get_vertex(VariableName name);
  std::optional<std::shared_ptr<BigOperator>>
  get_ve(VariableName src, VariableName dst, std::optional<TableType> type);

public:
  Builder(std::string query);
  void parse();
  JoinOrder variable_apperance_order();

  std::shared_ptr<BigOperator> build(JoinOrder join_order);
  std::vector<std::shared_ptr<BigOperator>> all_possible_plans();
};

class vqlListener : public vqlBaseListener {
public:
  VariableName last_vertex_name;
  std::optional<Edge> last_edge;

  // output
  std::map<VariableName, Vertex> vertices;
  std::vector<Edge> edges;
  std::vector<Variable> return_vertices;
  bool only_count = false;

public:
  void exitVertex(vqlParser::VertexContext *ctx) override;
  void exitEdge(vqlParser::EdgeContext *ctx) override;
  void exitQuery(vqlParser::QueryContext *ctx) override;

  void debug();
};
} // namespace query

// impl libfmt for VertexFilterType
template <> struct fmt::formatter<query::VertexFilterType> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const query::VertexFilterType &p, FormatContext &ctx) const
      -> format_context::iterator {
    switch (p) {
    case query::VertexFilterType::ID:
      return format_to(ctx.out(), "ID");
    case query::VertexFilterType::TAG:
      return format_to(ctx.out(), "TAG");
    case query::VertexFilterType::ALL:
      return format_to(ctx.out(), "ALL");
    default:
      assert(0);
    }
  }
};
