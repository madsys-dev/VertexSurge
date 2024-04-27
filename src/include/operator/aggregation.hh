#pragma once
#include "common.hh"
#include "execution/tuple.hh"
#include "meta/type.hh"

enum class AggregationType { SUM, AVG, COUNT, MAX, MIN };

inline Type type_after_aggregation(AggregationType agg_type, Type input_type) {
  switch (agg_type) {
  case AggregationType::SUM: {
    switch (input_type) {
    case Type::Int32:
      return input_type;
    case Type::Int64:
      return input_type;
    default:
      assert(0);
    }
  }
  case AggregationType::AVG: {
    assert(0);
  }
  case AggregationType::COUNT: {
    return Type::Int64;
  }
  case AggregationType::MAX:
    [[fallthrough]];
  case AggregationType::MIN: {
    switch (input_type) {
    case Type::VID:
    case Type::VIDPair:
    case Type::Offset:
    case Type::Int32:
    case Type::Int64:
    case Type::ShortString:
    case Type::TagValue8:
    case Type::TagValue16:
      return input_type;
    }
  }
  }
  assert(0);
}

struct Aggregator {
protected:
  AggregationType agg_type;
  Type key_type, value_type, agg_value_type;

  std::unordered_map<TupleValue, TupleValue, TupleValueRuntimeHash,
                     TupleValueKeyEqual>
      agg_map;

  Aggregator(AggregationType agg_type, Type key_type, Type value_type,
             Type agg_value_type);

  // init or merge

  virtual void merge_internal(TupleValue key, TupleValue value) = 0;

public:
  // single thread
  void merge_st(void *key, void *value);
  virtual void end();

  size_t size();

  // Flat must be empty and concurrent
  void output_to_empty_flat(FlatTable *flat, size_t key_at, size_t agg_at);
};

struct SumAggregator : Aggregator {
  SumAggregator(Type key_type, Type value_type, Type agg_value_type);
  void merge_internal(TupleValue key, TupleValue value) override;
};

std::shared_ptr<Aggregator> create_aggregator(AggregationType type,
                                              Type key_type, Type value_type,
                                              Type agg_value_type);

// impl libfmt for AggregationType
template <> struct fmt::formatter<AggregationType> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(AggregationType t, FormatContext &ctx) {
    switch (t) {
    case AggregationType::SUM:
      return format_to(ctx.out(), "SUM");
    case AggregationType::AVG:
      return format_to(ctx.out(), "AVG");
    case AggregationType::COUNT:
      return format_to(ctx.out(), "COUNT");
    case AggregationType::MAX:
      return format_to(ctx.out(), "MAX");
    case AggregationType::MIN:
      return format_to(ctx.out(), "MIN");
    default:
      assert(0);
    }
  }
};