#include "common/ptest_common.hh"
#include "csv.hh"
#include "operator/big_operators2.hh"
#include "plan/physical_plan.hh"
#include "plan/usual_pattern.hh"
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
      load_from_csv<Param>(FIN_GRAPH_PATH_DEFAULT / "params" / "tcr1.csv", ',');
  std::vector<VID> params_vid;
  for (auto &p : params) {
    params_vid.push_back(p.id);
  }

  PhysicalPlan p =
      PhysicalPlan()
          .inline_vid("person.id", params_vid)
          .csr_expand_to_neighbor("person.id", "account.id", "Person",
                                  "Account", "own")
          .vexpand("account.id", "other.id", "Account", "Account", "transfer",
                   VExpandPathParameters::Any(), 1, 3, Direction::Backward)
          .convert_to_flat()
          .csr_expand_to_neighbor("other.id", "loan.id", "Account", "Loan",
                                  "deposit")
          .fetch_vertex_property("loan.id", "Loan", "balance")
          .remove_variable("loan.id")
          .sum_group_by("other.id", "balance");
  // p.root->export_plan_png(fmt::format("resource/{}/plan.png", __FILE__));
  p.run();

  return 0;
}