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
    Triangle 1-2-3-1

    This plan uses intersect starting from 1-2 1-3 and 2-3
*/

const size_t tag_count = 1;
std::vector<size_t> risk_ids_input[tag_count] = {DEFAULT_COMMUNITY_VEC(0)};
std::vector<VID> risk_ids[tag_count];

bool only_count = ONLY_COUNT_DEFAULT;

int main(int argc, char *argv[]) {
  string graph_path = BT_GRAPH_PATH_DEFAULT;
  size_t k_min = K_MIN_DEFAULT, k_max = E6_K_MAX_DEFAULT;

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

  //   for (size_t i = 0; i < tag_count; i++) {
  //     risk_ids_input[i].clear();
  //     for (size_t x = args[fmt::format("p{}_communities", i + 1)][0];
  //          x < args[fmt::format("p{}_communities", i + 1)][1]; x++) {
  //       risk_ids_input[i].push_back(x);
  //     }
  //   }

  //   k_min = args["k_min"];
  //   k_max = args["k_max"];
  // }
  for (size_t i = 0; i < tag_count; i++)
    for (const auto x : risk_ids_input[i])
      risk_ids[i].push_back(x);

  spdlog::set_level(spdlog::level::debug);

  SPDLOG_INFO("Parallelism: {}", omp_get_max_threads());
  Graph g(graph_path);
  g.load();

  auto scan_account1 = inlined_account_id("a", risk_ids[0]);
  scan_account1->record_output_size = true;

  auto projection_account1 = shared_ptr<BigOperator>(new Projection(
      Projection::MapPairs({Projection::MapPair({"a.id", "b.id"})})));
  projection_account1->addLastDependency(scan_account1);

  auto ts12 = vexpand_transfer(scan_account1, projection_account1, "a.id",
                               "b.id", k_min, k_max);

  auto &root = ts12;
  // auto &root = scan_account2;

  root->export_plan_png(
      fmt::format("resource/{}/plan.png",
                  std::filesystem::path(__FILE__).filename().string()));

  root->set_total_time_on();
  root->run();

  root->get_merged_prof();
  root->get_prof().report();

  [[maybe_unused]] size_t end_vertices_size = 0, touched_vertices_size = 0,
                          account_count =
                              g.schema.get_vertex_schema("Account").get_count();

  for (auto [k, v] : root->get_prof().get_global_counters()) {
    if (k.find("Projection") != std::string::npos)
      end_vertices_size += v;
    if (k.find("VExpand") != std::string::npos)
      touched_vertices_size += v;
  }

  SPDLOG_INFO("Selectivity: {} ({}/{})",
              (double)end_vertices_size / account_count, end_vertices_size,
              account_count);
  // if(PRINT_OUTPUT)
  // root->results->print(2000);

  SPDLOG_INFO("{}, {} tuples", root->results_schema.columns,
              root->results->tuple_count());

  SPDLOG_INFO("QueryUsedTime: {}",
              root->get_prof()
                  .get_global_timer(
                      PhysicalOperator::get_physical_operator_name(root.get()) +
                      "[Total]")
                  .elapsedMs());

  return 0;
}