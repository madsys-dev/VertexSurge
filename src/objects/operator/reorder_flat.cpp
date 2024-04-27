#include "multi_column_sort.hh"
#include "operator/big_operators2.hh"
#include "tbb/parallel_sort.h"

using namespace std;

ReOrderFlat::ReOrderFlat(FlatTupleCompareFn cmp_fn)
    : cmp_fn(cmp_fn), pre_order(false) {}

ReOrderFlat::ReOrderFlat(FlatTupleOrderFn order_fn)
    : order_fn(order_fn), pre_order(true) {}

void ReOrderFlat::finishAddingDependency() {
  assert(deps.size() == 1);
  results_schema = deps[0]->results_schema;
}

void ReOrderFlat::run_internal() {
  assert(dep_results[0]->type == TableType::Flat);
  results = dep_results[0];
  auto ref = results->down_to_flat_table()->get_start_tuple_ref();
  auto &schema = results->schema;

  if (pre_order) {
    pre_orders.resize(results->tuple_count());

#pragma omp parallel for
    for (size_t i = 0; i < results->tuple_count(); i++) {
      pre_orders[i] = this->order_fn(ref, i, this);
    }

    serialsort(
        ref, 0, results->tuple_count(),
        [this]([[maybe_unused]] DiskTupleRef &start, size_t i, size_t j) {
          return pre_orders[i] < pre_orders[j];
        },
        [this, &schema](DiskTupleRef &start, size_t i, size_t j) {
          for (size_t k = 0; k < schema.columns.size(); k++) {
            schema.columns[k].swap(start.get(schema, i, k),
                                   start.get(schema, j, k));
          }
          swap(pre_orders[i], pre_orders[j]);
        });

  } else {
    //   SPDLOG_INFO("{} {}", 0, results->tuple_count());
    serialsort(
        ref, 0, results->tuple_count(),
        [this](DiskTupleRef &start, size_t i, size_t j) {
          // SPDLOG_INFO("compare {}:({}) {}:({}) {}", i,
          //             start.offset(schema, i).to_string(schema), j,
          //             start.offset(schema, j).to_string(schema),
          //             this->order_fn(start, start, i, j));
          return this->cmp_fn(start, start, i, j);
        },
        [&schema](DiskTupleRef &start, size_t i, size_t j) {
          // SPDLOG_INFO("swapping {}:{} {}:{}", i,
          //             start.offset(schema, i).to_string(schema), j,
          //             start.offset(schema, j).to_string(schema));
          for (size_t k = 0; k < schema.columns.size(); k++) {
            schema.columns[k].swap(start.get(schema, i, k),
                                   start.get(schema, j, k));
          }
        });
  }
}

TEST(Operator, ReOrderFlatCmp) {
  // Mat build, Mat probe
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new ReOrderFlat(
      [](DiskTupleRef &start, DiskTupleRef &end, size_t i, size_t j) {
        return *start.get_as<VID>(Type::VID, i, 0) <
               *end.get_as<VID>(Type::VID, j, 0);
      }));
  {
    auto data = unique_ptr<BigOperator>(new InlinedData);

    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::Int64}};

    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));

    std::vector<VID> a = {1, 5, 2, 3, 7, 1, 4, 3};
    std::vector<int64_t> k = {1, 1, 1, 2, 2, 3, 3, 4};
    table->down_to_flat_table()->get_col_diskarray(0)->push_many_nt(a.data(),
                                                                    a.size());
    table->down_to_flat_table()->get_col_diskarray(1)->push_many_nt(k.data(),
                                                                    k.size());

    data->results = std::move(table);
    op->addLastDependency(std::move(data));
  }

  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, ReOrderFlatPreOrder) {
  // Mat build, Mat probe
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new ReOrderFlat(
      [](DiskTupleRef &start, size_t i, [[maybe_unused]] void *p) -> size_t {
        return *start.get_as<VID>(Type::VID, i, 0);
      }));
  {
    auto data = unique_ptr<BigOperator>(new InlinedData);

    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::Int64}};

    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));

    std::vector<VID> a = {1, 5, 2, 3, 7, 1, 4, 3};
    std::vector<int64_t> k = {1, 1, 1, 2, 2, 3, 3, 4};
    table->down_to_flat_table()->get_col_diskarray(0)->push_many_nt(a.data(),
                                                                    a.size());
    table->down_to_flat_table()->get_col_diskarray(1)->push_many_nt(k.data(),
                                                                    k.size());

    data->results = std::move(table);
    op->addLastDependency(std::move(data));
  }

  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}