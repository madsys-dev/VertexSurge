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

int main() {
  Graph g(FIN_GRAPH_PATH_DEFAULT);
  g.load();

  auto params =
      load_from_csv<Param>(FIN_GRAPH_PATH_DEFAULT / "params" / "tcr1.csv", ',');
  std::vector<VID> params_vid;
  for (auto &p : params) {
    params_vid.push_back(p.id);
  }

  // [[maybe_unused]] VID id = 1;

  VertexNeighborFetcher neighbor_fetcher("Account", "Medium", "signIn");
  VertexPropertyFetcher is_blocked_fetcher(
      "Medium", ValueSchema{"isBlocked", Type::TagValue8});
  TupleValue blocked;
  blocked.tagvalue8 = 1;

  PhysicalPlan p =
      PhysicalPlan()
          .inline_vid("person.id", params_vid)
          .csr_expand_to_neighbor("person.id", "account.id", "Person",
                                  "Account", "own")
          .vexpand("account.id", "other.id", "Account", "Account", "transfer",
                   VExpandPathParameters::Shortest().set_with_distance(), 1, 3,
                   Direction::Forward)
          .filter_matrix([&neighbor_fetcher, &is_blocked_fetcher,
                          &blocked](VID, VID other) {
            auto mediums = neighbor_fetcher.get_iterator(other);
            VID medium_id;
            while (mediums.next(medium_id)) {
              if (TupleValue::eq(is_blocked_fetcher.get_as_tv(medium_id),
                                 blocked, Type::TagValue8)) {
                return true;
              }
            }
            return false;
          })
          .convert_to_flat()
          .split_flat_by("account.id");
  p.run();
  return 0;
}
