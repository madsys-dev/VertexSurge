#include "common.hh"
#include "operator/big_operators2.hh"
#include "operator/big_operators3.hh"

using namespace std;

template <size_t N>
SemiJoin<N>::SemiJoin(std::array<VariableName, N> key_variables,
                      bool only_count)
    : only_count(only_count) {
  for (size_t i = 0; i < N; i++) {
    this->key_variables[i] = key_variables[i];
  }
  if (key_variables.size() != N) {
    SPDLOG_ERROR("SemiJoin: on_variables size {} != N {}", key_variables.size(),
                 N);
    assert(0);
  }
}

template <size_t N> void SemiJoin<N>::finishAddingDependency() {
  assert(deps.size() == 2);
  for (size_t i = 0; i < N; i++) {
    auto &key = key_variables[i];
    auto aks = deps[0]->results_schema.get_schema(key);
    auto bks = deps[1]->results_schema.get_schema(key);

    if (aks.has_value() == false) {
      SPDLOG_ERROR("SemiJoin: key {} not found in schema {}", key,
                   deps[0]->results_schema.columns);
      assert(0);
    }
    if (bks.has_value() == false) {
      SPDLOG_ERROR("SemiJoin: key {} not found in schema {}", key,
                   deps[1]->results_schema.columns);
      assert(0);
    }
    assert(aks.value().first == bks.value().first);
    a_idx[i] = aks.value().second;
    b_idx[i] = bks.value().second;
    key_variable_schema.columns.push_back(aks.value().first);
  }

  if (!only_count) {
    // do not new Flat table here because Mat input need Mat output
    results_schema = deps[0]->results_schema;
  } else {
    results_schema = TableSchema{{{"count", Type::Int64}}};
    auto results_flat_table = new FlatTable(results_schema);
    results_flat_table->resize(1);
    results = unique_ptr<Table>(results_flat_table);
  }
}

template <size_t N> void SemiJoin<N>::run_internal() {
  build();
  // for (auto x : b_set) {
  //   SPDLOG_INFO("{}", x.to_string(key_variable_schema));
  // }

  probe();
}

template <size_t N> void SemiJoin<N>::build() {
  auto &b = dep_results[1];
  switch (b->type) {
  case TableType::Flat: {
#pragma omp parallel for
    for (size_t i = 0; i < b->tuple_count(); i++) {
      FixedTuple<N> ft = b->down_to_flat_table()->get_fixed_tuple_at(i, b_idx);
      b_set.insert(ft);
    }
    break;
  }
  case TableType::VarRowMatrix: {
    if (N == 2) {
      b->down_to_var_row_matrix_table()->set_row_hash_map();
      b->down_to_var_row_matrix_table()->set_col_hash_map();
    } else {
      auto its = b->down_to_var_row_matrix_table()->get_iterators(
          omp_get_max_threads());
      // SPDLOG_INFO("{}",b->tuple_count());
#pragma omp parallel for
      for (auto &it : its) {
        // SPDLOG_INFO("{} {}", it.row_offset, it.col_offset);
        VID r, c;
        while (it.next(r, c)) {
          // SPDLOG_INFO("{},{}",r,c);
          FixedTuple<N> ft;
          for (size_t i = 0; i < N; i++) {
            if (b_idx[i] == 0) {
              ft[i].vid = r;
            }
            if (b_idx[i] == 1) {
              ft[i].vid = c;
            }
          }
          b_set.insert(ft);
        }
      }
    }

    break;
  }
  case TableType::ColMainMatrix: {
    assert(0);
    break;
  }

  default: {
    assert(0);
  }
  }
}

template <size_t N> bool SemiJoin<N>::probe_once(T &t) {
  // SPDLOG_INFO("Probe {}", t.to_string(key_variable_schema));
  auto &b = dep_results[1];
  switch (b->type) {
  case TableType::Flat: {
    return b_set.find(t) != b_set.end();
    break;
  }
  case TableType::VarRowMatrix: {
    return b_set.find(t) != b_set.end();
    break;
  }
  case TableType::ColMainMatrix: {
    assert(0);
    break;
  }

  default: {
    assert(0);
  }
  }
}

template <> bool SemiJoin<2>::probe_once(T &t) {
  // SPDLOG_INFO("Probe {} by Mat", t.to_string(key_variable_schema));
  auto &b = dep_results[1];
  switch (b->type) {
  case TableType::Flat: {
    return b_set.find(t) != b_set.end();
    break;
  }
  case TableType::VarRowMatrix: {
    VID r = t[0].vid, c = t[1].vid;
    if (b_idx[0] == 1 && b_idx[1] == 0) {
      std::swap(r, c);
    }
    auto mt = b->down_to_var_row_matrix_table();
    auto ridx = mt->find_row_idx(r), cidx = mt->find_col_idx(c);

    if (ridx.has_value() && cidx.has_value()) {
      return mt->get_matrix()->get(ridx.value(), cidx.value());
    } else {
      return false;
    }
    break;
  }
  case TableType::ColMainMatrix: {
    assert(0);
    break;
  }

  default: {
    assert(0);
  }
  }
}

template <size_t N> void SemiJoin<N>::probe() {
  vector<int64_t> counts(omp_get_max_threads(), 0);

  auto &a = dep_results[0];
  switch (a->type) {
  case TableType::Flat: {
    if (only_count == false) {
      auto results_flat_table = new FlatTable(results_schema, true);
      results = unique_ptr<Table>(results_flat_table);
    }
#pragma omp parallel for
    for (size_t i = 0; i < a->tuple_count(); i++) {
      FixedTuple<N> ft = a->down_to_flat_table()->get_fixed_tuple_at(i, a_idx);
      // SPDLOG_INFO("{}", ft.to_string(key_variable_schema));
      if (probe_once(ft)) {
        if (only_count == false) {
          auto tuple_ref = a->down_to_flat_table()->get_tuple_ref_at(i);
          // SPDLOG_INFO("Plus 1");
          // SPDLOG_INFO("push to results {}",
          // tuple_ref.to_string(results_schema));
          results->down_to_flat_table()->push_back_single_by_tuple_ref(
              tuple_ref);
        } else {
          counts[omp_get_thread_num()] += 1;
        }
      }
    }
    results->down_to_flat_table()->convert_to_sequential();
    break;
  }
  case TableType::VarRowMatrix: {
    if (only_count == false) {
      results = dep_results[0];
    }
    auto its =
        a->down_to_var_row_matrix_table()->get_iterators(omp_get_max_threads());
    auto mt = a->down_to_var_row_matrix_table();
    mt->set_col_hash_map();
    mt->set_row_hash_map();

#pragma omp parallel for
    for (auto &it : its) {
      VID r, c;
      while (it.next(r, c)) {
        FixedTuple<N> ft;
        for (size_t i = 0; i < N; i++) {
          if (a_idx[i] == 0) {
            ft[i].vid = r;
          }
          if (a_idx[i] == 1) {
            ft[i].vid = c;
          }
        }
        if (probe_once(ft)) {
          if (only_count) {
            counts[omp_get_thread_num()] += 1;
          }
        } else {
          if (only_count == false) {
            // SPDLOG_INFO("OK");
            VID ridx = mt->find_row_idx(r).value(),
                cidx = mt->find_col_idx(c).value();
            mt->get_matrix()->set_atomic(ridx, cidx, false);
          }
        }
      }
    }
    break;
  }

  default: {
    assert(0);
  }
  }
  if (only_count) {
    int64_t count = 0;
    for (auto &c : counts) {
      count += c;
    }
    SPDLOG_INFO("SEMI JOIN COUNT: {}", count);
    results->down_to_flat_table()->set(0, 0, &count);
  }
}

template class SemiJoin<1>;
template class SemiJoin<2>;
template class SemiJoin<3>;
template class SemiJoin<4>;

TEST(Operator, SemiJoinFlatFlat) {
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new SemiJoin<1>({"k"}, true));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);

    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::Int64}};

    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<int64_t> k = {1, 6, 7, 8, 2, 3, 3, 4};
    table->down_to_flat_table()->get_col_diskarray(0)->push_many_nt(a.data(),
                                                                    a.size());
    table->down_to_flat_table()->get_col_diskarray(1)->push_many_nt(k.data(),
                                                                    k.size());

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);

    data->results_schema.columns = {{"b", Type::VID}, {"k", Type::Int64}};

    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));

    auto array = table->down_to_flat_table()->get_col_diskarray(0);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<int64_t> k = {1, 1, 1, 2, 2, 7, 8, 4};

    table->down_to_flat_table()->get_col_diskarray(0)->push_many_nt(a.data(),
                                                                    a.size());
    table->down_to_flat_table()->get_col_diskarray(1)->push_many_nt(k.data(),
                                                                    k.size());

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  op->finishAddingDependency();
  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, SemiJoinFlatMat) {
  // Flat build, Mat probe

  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new SemiJoin<1>({"k"}, false));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));
    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<int64_t> k = {1, 6, 7, 8, 2, 3, 3, 4};

    table->down_to_flat_table()->get_col_diskarray(0)->push_many_nt(a.data(),
                                                                    a.size());
    table->down_to_flat_table()->get_col_diskarray(1)->push_many_nt(k.data(),
                                                                    k.size());

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"b", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new VarRowMatrixTable(data->results_schema));

    auto mat = make_shared<DenseVarStackMatrix>(10, 10, 512);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 7, 8, 4};
    std::map<VID, size_t> k_map;
    std::vector<VID> k_mapv;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
        k_mapv.push_back(x);
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));

    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_var_row_matrix_table()->set_matrix(std::move(mat));
    table->down_to_var_row_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }
  op->finishAddingDependency();

  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, SemiJoinMatFlat) {
  // Mat build, Flat probe
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new SemiJoin<1>({"k"}, false));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));
    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 6, 7, 8, 2, 3, 3, 4};

    table->down_to_flat_table()->get_col_diskarray(0)->push_many_nt(a.data(),
                                                                    a.size());
    table->down_to_flat_table()->get_col_diskarray(1)->push_many_nt(k.data(),
                                                                    k.size());

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"b", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new VarRowMatrixTable(data->results_schema));

    auto mat = make_shared<DenseVarStackMatrix>(10, 10, 512);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<int64_t> k = {1, 1, 1, 2, 2, 7, 8, 4};
    std::map<VID, size_t> k_map;
    std::vector<VID> k_mapv;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
        k_mapv.push_back(x);
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));

    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_var_row_matrix_table()->set_matrix(std::move(mat));
    table->down_to_var_row_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  op->finishAddingDependency();
  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, SemiJoinMatMat) {
  // Mat build, Mat probe
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new SemiJoin<1>({"k"}, true));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new VarRowMatrixTable(data->results_schema));

    auto mat = make_shared<DenseVarStackMatrix>(10, 10, 512);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 6, 7, 8, 2, 3, 3, 4};
    std::map<VID, size_t> k_map;
    std::vector<VID> k_mapv;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
        k_mapv.push_back(x);
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));

    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_var_row_matrix_table()->set_matrix(std::move(mat));
    table->down_to_var_row_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"b", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new VarRowMatrixTable(data->results_schema));

    auto mat = make_shared<DenseVarStackMatrix>(10, 10, 512);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 7, 8, 4};
    std::map<VID, size_t> k_map;
    std::vector<VID> k_mapv;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
        k_mapv.push_back(x);
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));
    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_var_row_matrix_table()->set_matrix(std::move(mat));
    table->down_to_var_row_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  op->finishAddingDependency();
  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}