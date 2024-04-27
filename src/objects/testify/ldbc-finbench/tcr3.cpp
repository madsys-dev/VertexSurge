#include "common/ptest_common.hh"
#include "csv.hh"
#include "operator/big_operators2.hh"
#include "plan/physical_plan.hh"
#include "storage/graph_fetcher.hh"
#include "testify/fin_parameters.hh"

struct Param {
  VID id1, id2;
  inline static Param load_from_tvs(const std::vector<std::string> &titles,
                                    std::vector<std::string> &&values) {
    Param p;
    assert(titles[0] == "id1");
    assert(titles[1] == "id2");
    p.id1 = std::stoll(values[0]);
    p.id2 = std::stoll(values[1]);
    return p;
  };
};

int main() {
  Graph g(FIN_GRAPH_PATH_DEFAULT);
  g.load();

  auto params =
      load_from_csv<Param>(FIN_GRAPH_PATH_DEFAULT / "params" / "tcr3.csv", ',');
  std::vector<VID> start, taget;
  for (auto &p : params) {
    start.push_back(p.id1);
    taget.push_back(p.id2);
  }

  PhysicalPlan p = PhysicalPlan()
                       .inline_vid("start", start)
                       .merge_flat(PhysicalPlan().inline_vid("target", taget))
                       .find_shortest("start", "target", "Account", "Account",
                                      "transfer", Direction::Forward);
  p.run();

  return 0;
}
