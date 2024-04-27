#include "vql/query_builder.hh"
#include "operator/big_operators.hh"
#include "plan/usual_pattern.hh"

namespace query {

Builder::Builder(std::string query) : query(query){};

std::vector<VariableName> Builder::variable_apperance_order() {
  std::vector<VariableName> join_order;
  for (auto v : vertices) {
    join_order.push_back(v.name);
  }
  return join_order;
}

std::vector<std::shared_ptr<BigOperator>> Builder::all_possible_plans() {
  std::vector<std::shared_ptr<BigOperator>> plans;
  std::vector<VariableName> join_order = variable_apperance_order();
  std::sort(join_order.begin(), join_order.end());
  do {
    plans.push_back(build(join_order));
  } while (std::next_permutation(join_order.begin(), join_order.end()));
  return plans;
}

std::shared_ptr<BigOperator>
Builder::build(std::vector<VariableName> join_order) {

  vertex_scan.clear();
  ves.clear();

  for (auto v : vertices) {
    std::shared_ptr<BigOperator> vs;
    switch (v.filter_type) {
    case VertexFilterType::ID:
      assert(0);
    case VertexFilterType::TAG:
      vs = scan_person_id_filter_on_community_range(v.name, v.tag);
      break;
    case VertexFilterType::ALL:
      vs = scan_person_id_filter_on_community_no_filter(v.name);
      break;
    default:
      assert(0);
    }
    vertex_scan.push_back(vs);
  }

  for (auto e : edges) {
    std::shared_ptr<BigOperator> ve;
    ve = vexpand_knows(get_vertex(e.src), get_vertex(e.dst),
                       fmt::format("{}.id", e.src), fmt::format("{}.id", e.dst),
                       e.variable_length.start, e.variable_length.end, e.dir,
                       e.path_type);

    ves[{e.src, e.dst, TableType::VarRowMatrix}] = (ve);
  }
  if (join_order.size() >= 2) {
    std::shared_ptr<BigOperator> last =
        get_ve(join_order[0], join_order[1], std::nullopt).value();
    SPDLOG_INFO("start {} {}", join_order[0], join_order[1]);

    for (size_t i = 2; i < join_order.size(); i++) {
      bool this_only_count = true;
      if (i == join_order.size() - 1) {
        this_only_count = only_count;
      }

      auto mint = std::shared_ptr<BigOperator>(new Mintersect(
          fmt::format("{}.id", join_order[i]), "Person", this_only_count));
      mint->addDependency(last);
      for (size_t j = 0; j < i; j++) {
        auto ve =
            get_ve(join_order[i], join_order[j], TableType::ColMainMatrix);
        if (ve.has_value()) {
          mint->addDependency(ve.value());
          SPDLOG_INFO("intersect {} {}", join_order[i], join_order[j]);
        }
      }
      mint->finishAddingDependency();
      last = mint;
    }
    return last;
  } else {
    return get_vertex(join_order[0]);
  }

  return nullptr;
}

std::shared_ptr<BigOperator> Builder::get_vertex(VariableName name) {
  for (auto &vs : vertex_scan) {
    if (vs->results_schema.columns[0].name == fmt::format("{}.id", name)) {
      return vs;
    }
  }
  assert(0);
}

std::optional<std::shared_ptr<BigOperator>>
Builder::get_ve(VariableName src, VariableName dst,
                std::optional<TableType> type) {
  if (type.has_value()) {
    if (ves.contains({src, dst, type.value()})) {
      return ves[{src, dst, type.value()}];
    }
  } else {
    if (ves.contains({src, dst, TableType::VarRowMatrix})) {
      return ves[{src, dst, TableType::VarRowMatrix}];
    }
    if (ves.contains({src, dst, TableType::ColMainMatrix})) {
      return ves[{src, dst, TableType::ColMainMatrix}];
    }
  }
  // not found
  std::shared_ptr<BigOperator> ve = nullptr;
  bool transpose = false;
  if (ves.contains({src, dst, TableType::VarRowMatrix})) {
    ve = ves[{src, dst, TableType::VarRowMatrix}];
  }
  if (ves.contains({dst, src, TableType::VarRowMatrix})) {
    ve = ves[{dst, src, TableType::VarRowMatrix}];
    transpose = true;
  }
  if (ve != nullptr) {
    auto ve_T =
        std::shared_ptr<BigOperator>(new TableConvert(type.value(), transpose));
    ves[{src, dst, type.value()}] = ve_T;
    ve_T->addLastDependency(ve);
    return ve_T;
  }

  return std::nullopt;
}

void Builder::parse() {
  using namespace antlr4;
  ANTLRInputStream input(query);
  vqlLexer lexer(&input);
  CommonTokenStream tokens(&lexer);
  vqlParser parser(&tokens);

  tree::ParseTree *tree = parser.query();
  vqlListener listener;
  tree::ParseTreeWalker::DEFAULT.walk(&listener, tree);

  //   listener.debug();

  // get parse data
  for (auto [k, v] : listener.vertices) {
    vertices.push_back(v);
  }
  edges = listener.edges;
  return_vertices = listener.return_vertices;
  only_count = listener.only_count;

  std::cout << "Vertices:" << std::endl;
  for (auto &v : vertices) {
    std::cout << fmt::format("{} {} {} {}", v.name, v.filter_type, v.id, v.tag)
              << std::endl;
  }
  std::cout << "Edges:" << std::endl;
  for (auto &e : edges) {
    std::cout << fmt::format(
                     "{} {} {} {} {}", e.src, e.dst, e.dir, e.variable_length,
                     e.path_type == ExpandPathType::Shortest ? "shortest"
                                                             : "any")
              << std::endl;
  }

  std::cout << "Return:" << std::endl;
  for (auto &v : return_vertices) {
    std::cout << fmt::format("{} {}", v.vname, v.property.value_or(""))
              << std::endl;
  }
}

void Vertex::merge(const Vertex &v) {
  if (filter_type == VertexFilterType::ALL) {
    filter_type = v.filter_type;
    id = v.id;
    tag = v.tag;
  } else {
    if (v.filter_type == VertexFilterType::ALL) {
      return;
    } else {
      assert(0);
    }
  }
}

void vqlListener::exitVertex(vqlParser::VertexContext *ctx) {
  Vertex v;

  v.name = ctx->VAR()->getSymbol()->getText();
  if (ctx->vertex_filter() == nullptr) {
    v.filter_type = VertexFilterType::ALL;
  } else {
    auto filter = ctx->vertex_filter()->property_filter();
    if (filter->INT() != nullptr) {
      v.filter_type = VertexFilterType::ID;
      v.id = std::stoull(filter->INT()->getSymbol()->getText());
    } else {
      v.filter_type = VertexFilterType::TAG;
      auto range = filter->int_range();
      v.tag.start = std::stoull(range->INT(0)->getSymbol()->getText());
      v.tag.end = std::stoull(range->INT(1)->getSymbol()->getText());
    }
  }

  if (last_edge.has_value()) {
    last_edge->dst = v.name;
    edges.push_back(last_edge.value());
    last_edge = std::nullopt;
  }
  last_vertex_name = v.name;

  // std::cout<<fmt::format("{} {} {}
  // {}",v.name,v.filter_type,v.id,v.tag)<<std::endl;
  if (vertices.contains(v.name) == false) {
    vertices[v.name] = v;
  } else {
    vertices[v.name].merge(v);
  }
}

void vqlListener::exitEdge(vqlParser::EdgeContext *ctx) {
  Edge e;

  vqlParser::LengthContext *length = nullptr;
  vqlParser::ShortestContext *shortest = nullptr;

  if (ctx->backward_edge() != nullptr) {
    e.dir = Direction::Backward;
    length = ctx->backward_edge()->length();
    shortest = ctx->backward_edge()->shortest();
  }
  if (ctx->forward_edge() != nullptr) {
    e.dir = Direction::Forward;
    length = ctx->forward_edge()->length();
    shortest = ctx->forward_edge()->shortest();
  }
  if (ctx->undirected_edge() != nullptr) {
    e.dir = Direction::Both;
    length = ctx->undirected_edge()->length();
    shortest = ctx->undirected_edge()->shortest();
  }
  if (shortest != nullptr)
    e.path_type = ExpandPathType::Shortest;
  else
    e.path_type = ExpandPathType::Any;

  if (length != nullptr) {
    if (length->fixed_length() != nullptr) {
      size_t start;
      if (length->fixed_length()->INT() == nullptr) {
        start = 1;
      } else {
        start =
            std::stoull(length->fixed_length()->INT()->getSymbol()->getText());
      }
      e.variable_length = {start, start};
    }
    if (length->variable_length() != nullptr) {
      size_t start = std::stoull(
          length->variable_length()->INT(0)->getSymbol()->getText());
      size_t end = std::stoull(
          length->variable_length()->INT(1)->getSymbol()->getText());
      e.variable_length = {start, end};
    }

  } else {
    e.variable_length = {1, 1};
  }

  e.src = last_vertex_name;
  last_edge = e;
}

void vqlListener::exitQuery(vqlParser::QueryContext *ctx) {
  for (auto v : ctx->var_props()->var_prop()) {
    Variable vp;
    vp.vname = v->VAR(0)->getSymbol()->getText();
    if (v->VAR(1) != nullptr) {
      vp.property = v->VAR(1)->getSymbol()->getText();
    }
    return_vertices.push_back(vp);
  }
  if (ctx->count_only() != nullptr) {
    only_count = true;
  }
}

} // namespace query

TEST(VQL, QueryBuilder) {

  std::string query =
      "vmatch (a{tag:1-1})->(b), "
      "(b{tag:1-100})<-[2..4]-(c{id:1})-[]s-(a) return a.id,b.community,c;";
  std::cout << query << std::endl;
  query::Builder builder(query);
  builder.parse();
}