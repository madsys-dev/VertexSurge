#include "common/ptest_common.hh"
#include "vql/query_builder.hh"

int main() {
  std::string graph_path = SN_GRAPH_PATH_DEFAULT;
  Graph g(graph_path);
  g.load();

  std::string query =
      "vmatch (a{tag:1-1})->(b), "
      "(b{tag:3-3})<-[2..4]-(c{tag:2-2})-[]s-(a) return a.id,b.community,c;";
  std::cout << query << std::endl;
  query::Builder builder(query);
  builder.parse();

  auto root = builder.build(builder.variable_apperance_order());
  root->test_run(std::filesystem::path(__FILE__).filename().string(),
                 builder.only_count);

  return 0;
}