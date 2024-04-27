#include "operator/big_operators3.hh"

using namespace std;

Merge::Merge(){};

void Merge::finishAddingDependency() {
  for (auto &dep : deps) {
    results_schema.merge_schema(dep->results_schema);
  }
  assert(results_schema.has_duplicate_variable() == false);
}

void Merge::run_internal() {
  optional<size_t> count = nullopt;
  for (auto &r : dep_results) {
    if (count.has_value()) {
      assert(r->tuple_count() == count.value());
    } else {
      count = r->tuple_count();
    }
  }
  results = std::shared_ptr<Table>(new FlatTable(results_schema));
  auto flat = results->down_to_flat_table();
  flat->data.clear();

  for (auto &r : dep_results) {
    flat->data.insert(flat->data.end(), r->down_to_flat_table()->data.begin(),
                      r->down_to_flat_table()->data.end());
  }
  flat->check_data_consistent_with_schema();
}
