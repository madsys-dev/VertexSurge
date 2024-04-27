#pragma once
#include "common.hh"
#include "meta/type.hh"
#include "operator/expand.hh"
#include "operator/physical_operator.hh"
#include <string>

namespace logical {

struct Pattern;

struct Vertex {
private:
  Pattern *pattern;
  Vertex(VariableName name, TypeName label, Pattern *p);
  friend Pattern;

public:
  VariableName name;
  TypeName label;
  std::vector<VariableFilter> filters;

  Vertex *filter();

  Vertex *vexpand_any(VariableName v, TypeName edge_label,
                      Range<size_t> variable_length, Direction dir);

  Vertex *vexpand_shortest(VariableName v, TypeName edge_label,
                           Range<size_t> variable_length, Direction dir);

private:
  Vertex *vexpand_simple(VariableName v, TypeName edge_label,
                         Range<size_t> variable_length, Direction dir,
                         ExpandPathType path_type);
};

struct EdgeStep {
  Direction dir;
  std::set<TypeName> labels;
};

// impl libfmt for Ed

struct Edge {
  Vertex *src, *dst;
  ExpandPathType path_type;
  Range<size_t> variable_length;
  std::vector<EdgeStep> steps; // steps.size() == max(variable_length)
};

struct Pattern {
private:
  friend Vertex;

public:
  std::vector<std::shared_ptr<Vertex>> vertices;
  std::vector<Edge> edges;

  Pattern *create_vertex(VariableName name, TypeName label);

  Vertex *get_vertex(VariableName name);
  Vertex *get_vertex_or_create(VariableName name, TypeName label);

private:
  std::optional<Edge *> get_edge(Vertex *src, Vertex *dst);

  void vexpand_in_detail(VariableName src, VariableName dst,
                         ExpandPathType path_type,
                         Range<size_t> variable_length,
                         std::vector<EdgeStep> steps);
};

} // namespace logical

// impl libfmt for Vertex
template <> struct fmt::formatter<logical::Vertex> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const logical::Vertex &v, FormatContext &ctx) const
      -> format_context::iterator {
    return format_to(ctx.out(), "({}:{})", v.name, v.label);
  }
};

// impl libfmt for EdgeStep
template <> struct fmt::formatter<logical::EdgeStep> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const logical::EdgeStep &es, FormatContext &ctx) const
      -> format_context::iterator {
    std::string labels, re;
    for (auto &l : es.labels) {
      labels += fmt::format(":{}", l);
    }

    switch (es.dir) {
    case Direction::Forward: {
      re = fmt::format("-[{}]->", labels);
      break;
    }
    case Direction::Backward: {
      re = fmt::format("<-[{}]-", labels);
      break;
    }
    case Direction::Both: {
      re = fmt::format("-[{}]-", labels);
      break;
    }
    default: {
      assert(0);
    }
    }
    return format_to(ctx.out(), "{}", re);
  }
};

// impl libfmt for Edge
template <> struct fmt::formatter<logical::Edge> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const logical::Edge &e, FormatContext &ctx) const
      -> format_context::iterator {
    std::string path;
    for (auto &es : e.steps) {
      path += fmt::format("{}", es);
      if (&es != &e.steps.back()) {
        path += "()";
      }
    }
    return format_to(ctx.out(), "({}){}({})", e.src->name, path, e.dst->name);
  }
};

// impl libfmt for Pattern
template <> struct fmt::formatter<logical::Pattern> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const logical::Pattern &p, FormatContext &ctx) const
      -> format_context::iterator {
    std::string vertices, edges;
    for (auto &v : p.vertices) {
      vertices += fmt::format("{}\n", *v);
    }
    for (auto &e : p.edges) {
      edges += fmt::format("{}\n", e);
    }
    return format_to(ctx.out(), "Vertices: {}\nEdges: {}", vertices, edges);
  }
};