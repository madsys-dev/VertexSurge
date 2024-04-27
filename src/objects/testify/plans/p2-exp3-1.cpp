#include <nlohmann/json.hpp>

#include "common.hh"
#include "common/ptest_common.hh"
#include "operator/big_operators.hh"
#include "operator/big_operators2.hh"
#include "plan/usual_pattern.hh"
#include "storage/graph_store.hh"

using namespace std;
using json = nlohmann::json;

const size_t community_count = 3;
std::set<size_t> p_communities[community_count] = {
    DEFAULT_COMMUNITY(0), DEFAULT_COMMUNITY(1), DEFAULT_COMMUNITY(2)};

bool only_count = ONLY_COUNT_DEFAULT;

int main(int argc, char *argv[]) {
  string graph_path = SN_GRAPH_PATH_DEFAULT;
  size_t k_min = 2, k_max = 2;

  if (argc != 2) {
    cout << "Usage: kmax" << endl;
    return 0;
  }
  if (string(argv[1]) != "debug") {
    size_t k = std::stoul(argv[1]);
    k_max = k;
  }

  spdlog::set_level(spdlog::level::debug);

  SPDLOG_INFO("Parallelism: {}", omp_get_max_threads());
  Graph g(graph_path);
  g.load();

  auto scan_person1 = scan_person_id_filter_on_community("a", [](void *p) {
    auto community = reinterpret_cast<TagValue8 *>(p);
    return p_communities[0].contains(*community);
  });
  scan_person1->record_output_size = true;

  auto scan_person2 = scan_person_id_filter_on_community("b", [](void *p) {
    auto community = reinterpret_cast<TagValue8 *>(p);
    return p_communities[1].contains(*community);
  });
  scan_person2->record_output_size = true;

  auto scan_person3 = scan_person_id_filter_on_community("c", [](void *p) {
    auto community = reinterpret_cast<TagValue8 *>(p);
    return p_communities[2].contains(*community);
  });
  scan_person3->record_output_size = true;

  // 1->2->3

  auto p12 =
      vexpand_knows(scan_person1, scan_person2, "a.id", "b.id", k_min, k_max);
  auto p12C = shared_ptr<BigOperator>(
      new TableConvert(TableType::ColMainMatrix, false));
  p12C->addLastDependency(p12);

  auto p23 =
      vexpand_knows(scan_person2, scan_person3, "b.id", "c.id", k_min, k_max);
  auto p23C =
      shared_ptr<BigOperator>(new TableConvert(TableType::ColMainMatrix, true));
  p23C->addLastDependency(p23);

  auto bj123 = shared_ptr<BigOperator>(new HashJoin("b.id", only_count));
  bj123->addDependency(p12C);
  bj123->addLastDependency(p23C);

  auto &root = bj123;

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

  // if (PRINT_OUTPUT)
  //   root->results->print(2000);

  SPDLOG_INFO("{}, {} tuples", root->results_schema.columns,
              root->results->tuple_count());

  SPDLOG_INFO("QueryUsedTime: {}",
              root->get_prof()
                  .get_global_timer(
                      PhysicalOperator::get_physical_operator_name(root.get()) +
                      "[Total]")
                  .elapsedMs());

  if (only_count) {
    SPDLOG_INFO("QueryResults: {}",
                root->results->down_to_flat_table()->tuple_to_string(0));
  }

  return 0;
}