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

  auto p12 =
      vexpand_knows(scan_person1, scan_person2, "a.id", "b.id", k_min, k_max);

  auto p31 =
      vexpand_knows(scan_person3, scan_person1, "c.id", "a.id", k_min, k_max);

  auto p32 =
      vexpand_knows(scan_person3, scan_person2, "c.id", "b.id", k_min, k_max);

  auto p14 =
      vexpand_knows(scan_person1, scan_person4, "a.id", "d.id", k_min, k_max);
  auto p24 =
      vexpand_knows(scan_person2, scan_person4, "b.id", "d.id", k_min, k_max);
  auto p34 =
      vexpand_knows(scan_person3, scan_person4, "c.id", "d.id", k_min, k_max);

  auto p32C = shared_ptr<BigOperator>(
      new TableConvert(TableType::ColMainMatrix, false));
  p32C->addLastDependency((p32));

  auto p31C = shared_ptr<BigOperator>(
      new TableConvert(TableType::ColMainMatrix, false));
  p31C->addLastDependency((p31));

  auto m123 = shared_ptr<BigOperator>(new Mintersect("c.id", "Person", false));

  m123->addDependency((p12));
  m123->addDependency((p31C));
  m123->addLastDependency((p32C));

  auto p41C =
      shared_ptr<BigOperator>(new TableConvert(TableType::ColMainMatrix, true));
  p41C->addLastDependency((p14));

  auto p42C =
      shared_ptr<BigOperator>(new TableConvert(TableType::ColMainMatrix, true));
  p42C->addLastDependency((p24));

  auto p43C =
      shared_ptr<BigOperator>(new TableConvert(TableType::ColMainMatrix, true));
  p43C->addLastDependency((p34));

  auto m1234 =
      shared_ptr<BigOperator>(new Mintersect("d.id", "Person", only_count));
  m1234->addDependency(m123);
  m1234->addDependency((p41C));
  m1234->addDependency((p42C));
  m1234->addLastDependency((p43C));

  auto &root = m1234;
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