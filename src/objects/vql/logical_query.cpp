#include "vql/logical_query.hh"

namespace logical {

Vertex::Vertex(VariableName name, TypeName label, Pattern *pattern)
    : pattern(pattern), name(name), label(label) {}

Vertex *Vertex::vexpand_any(VariableName v, TypeName edge_label,
                            Range<size_t> variable_length, Direction dir) {
  return vexpand_simple(v, edge_label, variable_length, dir,
                        ExpandPathType::Any);
}

Vertex *Vertex::vexpand_shortest(VariableName v, TypeName edge_label,
                                 Range<size_t> variable_length, Direction dir) {
  return vexpand_simple(v, edge_label, variable_length, dir,
                        ExpandPathType::Shortest);
}

Vertex *Vertex::vexpand_simple(VariableName v, TypeName edge_label,
                               Range<size_t> variable_length, Direction dir,
                               ExpandPathType path_type) {
  std::vector<EdgeStep> steps;
  for (size_t i = 0; i < variable_length.end; i++) {
    steps.push_back({dir, {edge_label}});
  }
  pattern->vexpand_in_detail(name, v, path_type, variable_length, steps);
  return this;
}

Pattern *Pattern::create_vertex(VariableName name, TypeName label) {
  get_vertex_or_create(name, label);
  return this;
}

Vertex *Pattern::get_vertex(VariableName name) {
  for (auto &v : vertices) {
    if (v->name == name) {
      return v.get();
    }
  }
  throw std::runtime_error(fmt::format("vertex: {} not found", name));
}

Vertex *Pattern::get_vertex_or_create(VariableName name, TypeName label) {
  for (auto &v : vertices) {
    if (v->name == name) {
      return v.get();
    }
  }
  auto v = std::shared_ptr<Vertex>(new Vertex(name, label, this));
  vertices.push_back(v);
  return v.get();
}

std::optional<Edge *> Pattern::get_edge(Vertex *src, Vertex *dst) {
  for (auto &e : edges) {
    if (e.src == src && e.dst == dst) {
      return &e;
    }
    if (e.dst == src && e.src == dst) {
      return &e;
    }
  }
  return std::nullopt;
}

void Pattern::vexpand_in_detail(VariableName src, VariableName dst,
                                ExpandPathType path_type,
                                Range<size_t> variable_length,
                                std::vector<EdgeStep> steps) {
  auto u = get_vertex(src);
  auto v = get_vertex(dst);

  if (get_edge(u, v).has_value()) {
    throw std::runtime_error(
        fmt::format("edge from {} to {} already exists", src, dst));
  }

  Edge e;
  e.src = u;
  e.dst = v;
  e.path_type = path_type;
  e.variable_length = variable_length;
  e.steps = steps;
  edges.push_back(e);
}

} // namespace logical

TEST(Logical, Create) {
  using namespace logical;
  Pattern p;
  p.create_vertex("a", "Person")
      ->create_vertex("b", "Person")
      ->get_vertex("a")
      ->vexpand_any("b", "knows", {1, 3}, Direction::Both);
  fmt::print("{}", p);
}
