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
          .inline_vid("loan.id", params_vid)
          .csr_expand_to_neighbor("loan.id", "src.id", "Loan", "Account",
                                  "deposit")
          .vexpand_different_edges_by_step(
              "src.id", "dst.id",
              MultipleStepExpandParams(1, 3)
                  .append_expand_edge_param_for_every(ExpandEdgeParam(
                      "Account", "Account", "withdraw", Direction::Forward))
                  .append_expand_edge_param_for_every(ExpandEdgeParam(
                      "Account", "Account", "transfer", Direction::Forward)),
              VExpandPathParameters::Any())
          .convert_to_flat();

  p.run();

  return 0;
}
