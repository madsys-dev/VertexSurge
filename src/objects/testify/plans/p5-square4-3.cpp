#include <nlohmann/json.hpp>

#include "common.hh"
#include "operator/big_operators.hh"

#include "common/ptest_common.hh"
#include "operator/big_operators2.hh"
#include "plan/usual_pattern.hh"
#include "storage/graph_store.hh"

using namespace std;
using json = nlohmann::json;

const size_t community_count = 4;

std::set<size_t> p_communities[community_count] = {
    DEFAULT_COMMUNITY(0), DEFAULT_COMMUNITY(1), DEFAULT_COMMUNITY(2),
    DEFAULT_COMMUNITY(3)};

bool only_count = ONLY_COUNT_DEFAULT;

int main(int argc, char *argv[]) {
  string graph_path = SN_GRAPH_PATH_DEFAULT;
  size_t k_min = K_MIN_DEFAULT, k_max = K_MAX_DEFAULT;
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

  auto scan_person4 = scan_person_id_filter_on_community("d", [](void *p) {
    auto community = reinterpret_cast<TagValue8 *>(p);
    return p_communities[3].contains(*community);
  });
  scan_person4->record_output_size = true;

  auto p21 =
      vexpand_knows(scan_person2, scan_person1, "b.id", "a.id", k_min, k_max);
  auto p21C = shared_ptr<BigOperator>(
      new TableConvert(TableType::ColMainMatrix, false));
  p21C->addLastDependency(p21);

  auto p14 =
      vexpand_knows(scan_person1, scan_person4, "a.id", "d.id", k_min, k_max);
  auto p14C =
      shared_ptr<BigOperator>(new TableConvert(TableType::ColMainMatrix, true));
  p14C->addLastDependency(p14);

  auto bj412 = shared_ptr<BigOperator>(new HashJoin("a.id", false));
  bj412->addDependency(p21C);
  bj412->addLastDependency(p14C);

  auto p23 =
      vexpand_knows(scan_person2, scan_person3, "b.id", "c.id", k_min, k_max);
  auto p23C =
      shared_ptr<BigOperator>(new TableConvert(TableType::ColMainMatrix, true));
  p23C->addLastDependency(p23);

  auto p34 =
      vexpand_knows(scan_person3, scan_person4, "c.id", "d.id", k_min, k_max);
  auto p34C = shared_ptr<BigOperator>(
      new TableConvert(TableType::ColMainMatrix, false));
  p34C->addLastDependency(p34);

  auto m3intersect =
      shared_ptr<BigOperator>(new Mintersect("c.id", "Person", only_count));

  m3intersect->addDependency(bj412);
  m3intersect->addDependency(p23C);
  m3intersect->addLastDependency(p34C);

  auto &root = m3intersect;
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
  if (only_count) {
    SPDLOG_INFO("QueryResults: {}",
                root->results->down_to_flat_table()->tuple_to_string(0));
  }

  return 0;
}