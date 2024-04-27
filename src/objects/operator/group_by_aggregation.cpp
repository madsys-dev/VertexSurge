#include "operator/big_operators3.hh"

using namespace std;

Aggregator::Aggregator(AggregationType agg_type, Type key_type, Type value_type,
                       Type agg_value_type)
    : agg_type(agg_type), key_type(key_type), value_type(value_type),
      agg_value_type(agg_value_type),
      agg_map(TupleValueRuntimeHash::BUCKET_SIZE,
              TupleValueRuntimeHash(key_type), TupleValueKeyEqual(key_type)) {}

void Aggregator::end() {}

size_t Aggregator::size() { return agg_map.size(); }

void Aggregator::output_to_empty_flat(FlatTable *flat, size_t key_at,
                                      size_t agg_at) {
  assert(flat->tuple_count() == 0);

  for (auto [key, aggv] : agg_map) {
    // SPDLOG_INFO("output {} {}", key.to_string(key_type)
    // ,aggv.to_string(value_type));
    flat->push_back_single(key_at,
                           const_cast<TupleValue &>(key).value_ptr(key_type));
    flat->push_back_single(agg_at, aggv.value_ptr(agg_value_type));
  }
  flat->convert_to_sequential();
}

void Aggregator::merge_st(void *key, void *value) {
  TupleValue key_tv = TupleValue::construct_tuple_value(key_type, key);
  TupleValue value_tv = TupleValue::construct_tuple_value(value_type, value);
  merge_internal(key_tv, value_tv);
}

SumAggregator::SumAggregator(Type key_type, Type value_type,
                             Type agg_value_type)
    : Aggregator(AggregationType::SUM, key_type, value_type, agg_value_type) {
  assert(value_type == agg_value_type);
};

void SumAggregator::merge_internal(TupleValue key, TupleValue value) {
  if (agg_map.contains(key) == false) {
    agg_map[key] = TupleValue::zero(agg_value_type);
  }
  auto it = agg_map.find(key);
  it->second = TupleValue::add(it->second, value, agg_value_type);
}

std::shared_ptr<Aggregator> create_aggregator(AggregationType type,
                                              Type key_type, Type value_type,
                                              Type agg_value_type) {
  switch (type) {
  case AggregationType::SUM:
    return std::make_shared<SumAggregator>(key_type, value_type,
                                           agg_value_type);
  default:
    assert(0);
  }
}

GroupByAggregation::GroupByAggregation(VariableName group_by_what,
                                       VariableName aggregate_what,
                                       AggregationType agg_type,
                                       std::optional<VariableName> aggregate_as)
    : group_by_what(group_by_what), aggregate_what(aggregate_what),
      agg_type(agg_type) {
  if (aggregate_as.has_value()) {
    this->aggregate_as = aggregate_as.value();
  } else {
    this->aggregate_as = fmt::format("{}({})", agg_type, aggregate_what);
  }
}

void GroupByAggregation::finishAddingDependency() {
  assert(deps.size() == 1);
  assert(deps[0]->results_schema.columns.size() == 2);

  auto [key_vs, key_at] =
      deps[0]->results_schema.get_schema(group_by_what).value();
  auto [value_vs, value_at] =
      deps[0]->results_schema.get_schema(aggregate_what).value();
  this->key_at = key_at;
  this->value_at = value_at;

  Type agg_value_type = type_after_aggregation(agg_type, value_vs.type);

  results_schema.add_non_duplicated_variable(key_vs);
  results_schema.add_non_duplicated_variable(
      ValueSchema{aggregate_as, agg_value_type});

  agg = create_aggregator(agg_type, key_vs.type, value_vs.type, agg_value_type);
}

void GroupByAggregation::run_internal() {
  results = std::shared_ptr<Table>(new FlatTable(results_schema, true));
  auto output = results->down_to_flat_table();
  auto input = dep_results[0]->down_to_flat_table();

  for (size_t r = 0; r < input->tuple_count(); r++) {
    // SPDLOG_INFO("Merge {}",input->tuple_to_string(r));
    agg->merge_st(input->get_raw_ptr(r, key_at),
                  input->get_raw_ptr(r, value_at));
  }
  agg->end();
  agg->output_to_empty_flat(output, 0, 1);
}
