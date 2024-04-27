#include <nlohmann/json.hpp>

#include "common.hh"
#include "operator/big_operators.hh"

#include "common/ptest_common.hh"
#include "operator/big_operators2.hh"
#include "plan/usual_pattern.hh"
#include "storage/graph_store.hh"

using namespace std;
using json = nlohmann::json;

/*
Evaluation Case 2
3 hop reverse count

MATCH (p:GroupA)-[:knows*..3]-(q:Person) RETURN COUNT(p) as count_of_p,q ORDER
by count_of_p DESC;

*/

const size_t community_count = 1;

std::set<size_t> p_communities[community_count] = {DEFAULT_COMMUNITY(0)};

bool only_count = ONLY_COUNT_DEFAULT;

int main(int argc, char *argv[]) {
  string graph_path = SN_GRAPH_PATH_DEFAULT;
  size_t k_min = K_MIN_DEFAULT, k_max = K_MAX_DEFAULT;

  if (argc != 2) {
    cout << "Usage: k_max" << endl;
    return 0;
  }

  if (string(argv[1]) != "debug") {
    k_max = std::stoi(argv[1]);
  }

  // if (string(argv[1]) != "debug") {
  //   auto args = json::parse(argv[1]);
  //   graph_path = args["graph_path"];

  //   for (size_t i = 0; i < community_count; i++) {
  //     p_communities[i].clear();
  //     for (size_t x = args[fmt::format("p{}_communities", i + 1)][0];
  //          x < args[fmt::format("p{}_communities", i + 1)][1]; x++) {
  //       p_communities[i].insert(x);
  //     }
  //   }
  //   k_min = args["k_min"];
  //   k_max = args["k_max"];
  // }

  spdlog::set_level(spdlog::level::debug);

  SPDLOG_INFO("Parallelism: {}", omp_get_max_threads());
  Graph g(graph_path);
  g.load();

  auto scan_person1 = scan_person_id_filter_on_community("a", [](void *p) {
    auto community = reinterpret_cast<TagValue8 *>(p);
    return p_communities[0].contains(*community);
  });
  scan_person1->record_output_size = true;

  auto ve12 = shared_ptr<BigOperator>(new VExpand(
      "Person", "Person", "knows", Direction::Forward, k_min, k_max,
      ExpandPathType::Shortest, ExpandStrategy::Dense, "a.id", "b.id"));
  ve12->addLastDependency(scan_person1);
  ve12->record_output_size = true;

  auto cc12 =
      shared_ptr<BigOperator>(new MatrixColumnCount("count", SortOrder::DESC));
  cc12->addLastDependency(ve12);

  auto &root = cc12;
  //   auto &root = m3intersect;

  root->export_plan_png(
      fmt::format("resource/{}/plan.png",
                  std::filesystem::path(__FILE__).filename().string()));

  root->set_total_time_on();
  root->run();

  root->get_merged_prof();
  root->get_prof().report();

  [[maybe_unused]] size_t end_vertices_size = 0, touched_vertices_size = 0,
                          person_count =
                              g.schema.get_vertex_schema("Person").get_count();

  for (auto [k, v] : root->get_prof().get_global_counters()) {
    if (k.find("Projection") != std::string::npos)
      end_vertices_size += v;
    if (k.find("VExpand") != std::string::npos)
      touched_vertices_size += v;
  }

  SPDLOG_INFO("Selectivity: {} ({}/{})",
              (double)end_vertices_size / person_count, end_vertices_size,
              person_count);

  SPDLOG_INFO("{}, {} tuples", root->results_schema.columns,
              root->results->tuple_count());

  SPDLOG_INFO("QueryUsedTime: {}",
              root->get_prof()
                  .get_global_timer(
                      PhysicalOperator::get_physical_operator_name(root.get()) +
                      "[Total]")
                  .elapsedMs());

  root->results->print(100);

  return 0;
}