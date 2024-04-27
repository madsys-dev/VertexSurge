#include "operator/big_operators3.hh"
using namespace std;

SplitFlatBy::SplitFlatBy(VariableName split_by_what)
    : split_by_what(split_by_what) {}

void SplitFlatBy::finishAddingDependency() {
  results_schema = deps[0]->results_schema;
  split_type = results_schema.get_schema(split_by_what)->first.type;
  split_idx = results_schema.get_schema(split_by_what)->second;

  results =
      std::shared_ptr<Table>(new MultipleTable(results_schema, split_by_what));
  split_table = std::shared_ptr<MapType>(new MapType(
      TupleValueRuntimeHash::BUCKET_SIZE, TupleValueRuntimeHash(split_type),
      TupleValueKeyEqual(split_type)));
}

void SplitFlatBy::run_internal() {
  assert(dep_results[0]->type == TableType::Flat);
  auto flat = dep_results[0]->down_to_flat_table();
  auto multi_flat = results->down_to_table<MultipleTable>();

  auto map_idx = multi_flat->schema.map_to(multi_flat->subtable_schema);

  for (size_t r = 0; r < flat->tuple_count(); r++) {
    auto tv = TupleValue::construct_tuple_value(
        split_type, flat->get_raw_ptr(r, split_idx));
    if (split_table->contains(tv) == false) {
      (*split_table)[tv] = multi_flat->new_empty_subflat(true);
    }
  }

#pragma omp parallel for
  for (size_t r = 0; r < flat->tuple_count(); r++) {
    auto tv = TupleValue::construct_tuple_value(
        split_type, flat->get_raw_ptr(r, split_idx));
    auto subflat = (*split_table)[tv]->down_to_table<FlatTable>();
    for (size_t c = 0; c < flat->schema.columns.size(); c++) {
      if (c == split_idx)
        continue;
      size_t nc = map_idx[c];
      subflat->push_back_single(nc, flat->get_raw_ptr(r, c));
    }
  }

  for (auto &[key, t] : *split_table) {
    multi_flat->add_table(const_cast<TupleValue &>(key).value_ptr(split_type),
                          t);
  }
}
