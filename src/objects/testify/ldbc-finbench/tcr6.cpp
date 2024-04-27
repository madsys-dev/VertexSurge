#include "common/ptest_common.hh"
#include "csv.hh"
#include "operator/big_operators2.hh"
#include "plan/physical_plan.hh"
#include "storage/graph_fetcher.hh"
#include "testify/fin_parameters.hh"

struct Param {
  VID id;
  inline static Param load_from_tvs(const std::vector<std::string> &titles,
                                    std::vector<std::string> &&values) {
    Param p;
    assert(titles[0] == "id");
    p.id = std::stoll(values[0]);
    return p;
  };
};

int main([[maybe_unused]] int argc, [[maybe_unused]] char *argv[]) {
  Graph g(FIN_GRAPH_PATH_DEFAULT);
  g.load();

  auto params =
      load_from_csv<Param>(FIN_GRAPH_PATH_DEFAULT / "params" / "tcr6.csv", ',');
  std::vector<VID> params_vid;
  for (auto &p : params) {
    params_vid.push_back(p.id);
  }

  PhysicalPlan p =
      PhysicalPlan()
          .inline_vid("dst", params_vid)
          .vexpand_different_edges_by_step(
              "dst", "src",
              MultipleStepExpandParams(1, 2)
                  .set_expand_edge_param_at(
                      ExpandEdgeParam("Account", "Account", "withdraw",
                                      Direction::Backward),
                      1)
                  .set_expand_edge_param_at(
                      ExpandEdgeParam("Account", "Account", "transfer",
                                      Direction::Backward),
                      2),
              VExpandPathParameters::Any())
          .convert_to_flat();

  p.run();

  return 0;
}
