#include <tbb/concurrent_vector.h>

#include "operator/big_operators.hh"
#include "operator/big_operators2.hh"

using namespace std;

HashJoin::HashJoin(string key_variable_name, bool only_count)
    : key_variable_name(key_variable_name), only_count(only_count) {}

void HashJoin::finishAddingDependency() {
  if (!only_count) {
    results_schema = TableSchema::join_schema(
        deps[0]->results_schema, deps[1]->results_schema, key_variable_name);

  } else {
    results_schema = TableSchema{{{"count", Type::Int64}}};
    auto results_flat_table = new FlatTable(results_schema);
    results_flat_table->resize(1);
    results = unique_ptr<Table>(results_flat_table);
  }

  SPDLOG_INFO("Results Schema {}", results_schema.columns);
}

void HashJoin::build() {
  build_table = dep_results[0].get();
  probe_table = dep_results[1].get();

  assert(dep_results.size() == 2);

  auto pks = probe_table->schema.get_schema(key_variable_name);
  auto bks = build_table->schema.get_schema(key_variable_name);
  if (pks->first != bks->first) {
    SPDLOG_ERROR("{}", key_variable_name);
  }
  assert(pks->first == bks->first);

  if (build_table->type == TableType::Flat &&
      probe_table->type == TableType::Flat) {
    if (build_table->tuple_count() < probe_table->tuple_count()) {
      swap(build_table, probe_table);
    }

  } else if (build_table->type == TableType::Flat &&
             probe_table->type == TableType::ColMainMatrix) {
    if (pks.value().second != 1) {
      SPDLOG_ERROR("{}", key_variable_name);
    }
    assert(pks.value().second == 1);
    if (build_table->tuple_count() <
        probe_table->down_to_col_main_matrix_table()->col_count) {
      swap(build_table, probe_table);
    }

  } else if (build_table->type == TableType::ColMainMatrix &&
             probe_table->type == TableType::Flat) {

    assert(bks.value().second == 1);
    if (build_table->down_to_col_main_matrix_table()->col_count <
        probe_table->tuple_count()) {
      swap(build_table, probe_table);
    }
  } else if (build_table->type == TableType::ColMainMatrix &&
             probe_table->type == TableType::ColMainMatrix) {
    assert(bks.value().second == 1);
    assert(pks.value().second == 1);
    if (build_table->down_to_col_main_matrix_table()->col_count <
        probe_table->down_to_col_main_matrix_table()->col_count) {
      swap(build_table, probe_table);
    }
  } else {
    // not supported
    SPDLOG_ERROR("Build table type {} and probe table type {} not supported",
                 build_table->type, probe_table->type);
    assert(0);
  }
  SPDLOG_INFO("Build table {} has {} tuples", build_table->type,
              build_table->tuple_count());

  auto ks = build_table->schema.get_schema(key_variable_name);
  if (ks.has_value() == false) {
    SPDLOG_ERROR("Key variable {} not found in schema", key_variable_name);
    exit(1);
  }
  size_t key_idx = ks->second;

  switch (build_table->type) {
  case TableType::Flat: {
    TupleValueHash::set_type(ks->first.type);
    if (!only_count) {

      decltype(flat_hash_map)::accessor ac;
      auto ref = build_table->down_to_flat_table()->get_start_tuple_ref();
#pragma omp parallel for private(ac)
      for (size_t i = 0; i < build_table->tuple_count(); i++) {
        flat_hash_map.insert(ac,
                             ref.copy_out_at(build_table->schema, i, key_idx));
        ac->second.push_back(i);
      }
    } else {
      decltype(count_only_hash_map)::accessor ac;
      auto ref = build_table->down_to_flat_table()->get_start_tuple_ref();
#pragma omp parallel for private(ac)
      for (size_t i = 0; i < build_table->tuple_count(); i++) {
        count_only_hash_map.insert(
            ac, ref.copy_out_at(build_table->schema, i, key_idx));
        ac->second++;
      }
    }

    TupleValueHash::release_type();
    break;
  }
  case TableType::CSR: {
    // Not supported
    assert(0);
    break;
  }
  case TableType::ColMainMatrix: {
    if (!only_count) {
      auto &col_map =
          build_table->down_to_col_main_matrix_table()->get_col_map();
      if (col_map.has_value()) {
#pragma omp parallel for
        for (size_t i = 0; i < col_map.value()->size(); i++) {
          col_main_hash_map.insert({col_map.value()->as_ptr<VID>()[i], i});
        }
      }
    } else {
      TupleValueHash::set_type(Type::VID);
      auto mat = build_table->down_to_col_main_matrix_table()->get_matrix();
      auto &col_map =
          build_table->down_to_col_main_matrix_table()->get_col_map();
#pragma omp parallel for
      for (size_t i = 0; i < mat->get_col_count(); i++) {
        auto key_value = TupleValue(i);
        if (col_map.has_value()) {
          key_value.vid = col_map.value()->as_ptr<VID>()[i];
        }
        count_only_hash_map.insert({key_value, mat->count_bits_in_col(i)});
      }

      TupleValueHash::release_type();
    }

    break;
  }
  case TableType::Factorized: {
    // Not supported
    assert(0);
    break;
  }
  default: {
    assert(0);
  }
  }
}

void HashJoin::probe() {
  SPDLOG_INFO("Probe table has {} tuples", probe_table->tuple_count());

  auto probe_ks = probe_table->schema.get_schema(key_variable_name);
  if (probe_ks.has_value() == false) {
    SPDLOG_ERROR("Key variable {} not found in schema", key_variable_name);
    exit(1);
  }
  FlatTable *results_flat_table;
  if (!only_count) {
    results_flat_table = new FlatTable(results_schema, true);
  } else {
    results_flat_table = new FlatTable(results_schema);
    results_flat_table->resize(1);
  }
  int64_t total_count = 0;

  switch (probe_table->type) {
  case TableType::Flat: {
    auto probe_ref = probe_table->down_to_flat_table()->get_start_tuple_ref();
    auto probe_to_result_map = probe_table->schema.map_to(results_schema);
    if (!only_count) {
      switch (build_table->type) {
      case TableType::Flat: {

        auto build_ref =
            build_table->down_to_flat_table()->get_start_tuple_ref();
        auto build_to_result_map = build_table->schema.map_to(results_schema);

        decltype(flat_hash_map)::const_accessor ac;
#pragma omp parallel for private(ac)
        for (size_t pi = 0; pi < probe_table->tuple_count(); pi++) {
          auto key =
              probe_ref.copy_out_at(probe_table->schema, pi, probe_ks->second);
          if (flat_hash_map.find(ac, key)) {
            // SPDLOG_INFO("{}, find key
            // {}",probe_table->down_to_flat_table()->tuple_to_string(pi),
            // key.to_string(probe_ks->first.type));
            for (const auto &bi : ac->second) {
              // SPDLOG_INFO(
              //     "find build index:{} {}", bi,
              //     build_table->down_to_flat_table()->tuple_to_string(bi));
              for (size_t pci = 0; pci < probe_to_result_map.size(); pci++) {
                if (probe_to_result_map[pci] != static_cast<size_t>(-1) &&
                    pci != probe_ks->second) {
                  // SPDLOG_INFO("copy pi, ({} : {}) -> ({} :
                  // {})",pci,probe_table->schema.columns[pci],
                  // probe_to_result_map[pci],results_schema.columns[probe_to_result_map[pci]]);

                  results_flat_table->push_back_single(
                      probe_to_result_map[pci],
                      probe_ref.get(probe_table->schema, pi, pci));
                }
              }
              for (size_t bci = 0; bci < build_to_result_map.size(); bci++) {
                if (build_to_result_map[bci] != static_cast<size_t>(-1)) {
                  results_flat_table->push_back_single(
                      build_to_result_map[bci],
                      build_ref.get(build_table->schema, bi, bci));
                }
              }
            }
          }
        }
        break;
      }
      case TableType::ColMainMatrix: {
        bool is_hash = build_table->down_to_col_main_matrix_table()
                           ->get_col_map()
                           .has_value();
        auto col_main_table = build_table->down_to_col_main_matrix_table();

        size_t build_side_payload_idx =
            build_table->schema.map_to(results_schema)[0];

        decltype(col_main_hash_map)::const_accessor ac;
#pragma omp parallel for private(ac)
        for (size_t i = 0; i < probe_table->tuple_count(); i++) {
          VID vid =
              *probe_ref.get_as<VID>(probe_table->schema, i, probe_ks->second);
          size_t col_idx = vid;
          if (is_hash) {
            col_main_hash_map.find(ac, vid);
            col_idx = ac->second;
          }

          ColMainMatrixTable::Iterator it(*col_main_table, col_idx,
                                          col_idx + 1);
          VID r, c;
          while (it.next(r, c)) {
            for (size_t pi = 0; pi < probe_to_result_map.size(); pi++) {
              results_flat_table->push_back_single(
                  probe_to_result_map[pi],
                  probe_ref.get(probe_table->schema, i, pi));
            }
            results_flat_table->push_back_single(build_side_payload_idx, &r);
          }
        }

        break;
      }
      default: {
        assert(0);
      }
      }

    } else {
      // for count only hashmaps, no need to switch on build map type
      decltype(count_only_hash_map)::const_accessor ac;
#pragma omp parallel for private(ac) reduction(+ : total_count)
      for (size_t i = 0; i < probe_table->tuple_count(); i++) {
        auto key =
            probe_ref.copy_out_at(probe_table->schema, i, probe_ks->second);
        if (count_only_hash_map.find(ac, key)) {
          total_count += ac->second;
        }
      }
      // SPDLOG_INFO("total count {}",total_count);
    }
    break;
  }
  case TableType::ColMainMatrix: {
    if (!only_count) {
      switch (build_table->type) {
      case TableType::Flat: {
        bool probe_has_col_map = probe_table->down_to_col_main_matrix_table()
                                     ->get_col_map()
                                     .has_value();
        auto p_col_main_table = probe_table->down_to_col_main_matrix_table();
        auto p_mat = probe_table->down_to_col_main_matrix_table()->get_matrix();

        decltype(flat_hash_map)::const_accessor ac;
#pragma omp parallel for private(ac)
        for (size_t col_idx = 0; col_idx < p_mat->get_col_count(); col_idx++) {
          VID p_vid = col_idx;
          if (probe_has_col_map) {
            p_vid =
                p_col_main_table->get_col_map().value()->as_ptr<VID>()[col_idx];
          }
          TupleValue key;
          key.vid = p_vid;
          auto probe_idx_map = probe_table->schema.map_to(results_schema);
          auto build_idx_map = build_table->schema.map_to(results_schema);
          SPDLOG_DEBUG("probe_idx_map: {}", probe_idx_map);
          SPDLOG_DEBUG("build_idx_map: {}", build_idx_map);
          auto build_ref =
              build_table->down_to_flat_table()->get_start_tuple_ref();

          if (flat_hash_map.find(ac, key)) {
            for (size_t y = 0; y < p_mat->get_row_count(); y++) {
              if (!p_mat->get(y, col_idx)) {
                continue;
              }
              VID p_rvid = y;
              if (p_col_main_table->get_row_map().has_value()) {
                p_rvid =
                    p_col_main_table->get_row_map().value()->as_ptr<VID>()[y];
              }
              for (auto &bri : ac->second) {
                results_flat_table->push_back_single(probe_idx_map[0], &p_rvid);
                for (size_t bci = 0; bci < build_idx_map.size(); bci++) {
                  results_flat_table->push_back_single(
                      build_idx_map[bci],
                      build_ref.get(build_table->schema, bri, bci));
                }
              }
            }
          }
        }

        break;
      }

      case TableType::ColMainMatrix: {

        bool build_has_col_map = build_table->down_to_col_main_matrix_table()
                                     ->get_col_map()
                                     .has_value();
        auto b_col_main_table = build_table->down_to_col_main_matrix_table();

        bool probe_has_col_map = probe_table->down_to_col_main_matrix_table()
                                     ->get_col_map()
                                     .has_value();
        auto p_col_main_table = probe_table->down_to_col_main_matrix_table();
        auto p_mat = probe_table->down_to_col_main_matrix_table()->get_matrix();

        size_t build_payload_idx =
            build_table->schema.map_to(results_schema)[0];
        auto probe_idx_map = probe_table->schema.map_to(results_schema);

        decltype(col_main_hash_map)::const_accessor ac;
#pragma omp parallel for private(ac)
        for (size_t col_idx = 0; col_idx < p_mat->get_col_count(); col_idx++) {
          VID p_vid = col_idx;
          if (probe_has_col_map) {
            p_vid =
                p_col_main_table->get_col_map().value()->as_ptr<VID>()[col_idx];
          }

          size_t b_col_idx = p_vid;
          if (build_has_col_map) {
            if (col_main_hash_map.find(ac, p_vid)) {
              b_col_idx = ac->second;
            } else {
              continue;
            }
          }
          // SPDLOG_INFO("key {} at col {}", p_vid, col_idx);

          VID p_r, p_c, b_r, b_c;
          ColMainMatrixTable::Iterator pit(*p_col_main_table, col_idx,
                                           col_idx + 1),
              bit(*b_col_main_table, b_col_idx, b_col_idx + 1);
          // SPDLOG_INFO("expected count p {}, base {}",
          //             p_col_main_table->down_to_col_main_matrix_table()
          //                 ->get_matrix()
          //                 ->count_bits_in_col(col_idx),
          //             b_col_main_table->down_to_col_main_matrix_table()
          //                 ->get_matrix()
          //                 ->count_bits_in_col(b_col_idx));
          vector<pair<VID, VID>> b_r_c(
              b_col_main_table->down_to_col_main_matrix_table()
                  ->get_matrix()
                  ->count_bits_in_col(b_col_idx));

          size_t b_idx = 0;
          while (bit.next(b_r, b_c)) {
            b_r_c[b_idx] = {b_r, b_c};
            b_idx++;
          }
          while (pit.next(p_r, p_c)) {
            for (auto &b_pair : b_r_c) {
              results_flat_table->push_back_single(probe_idx_map[0], &p_r);
              results_flat_table->push_back_single(probe_idx_map[1], &p_vid);
              results_flat_table->push_back_single(build_payload_idx,
                                                   &b_pair.first);
            }
          }
        }

        break;
      }
      default: {
        assert(0);
      }
      }
    } else {
      // for count only hashmaps, no need to switch on build map type

      bool probe_has_col_map = probe_table->down_to_col_main_matrix_table()
                                   ->get_col_map()
                                   .has_value();
      auto p_col_main_table = probe_table->down_to_col_main_matrix_table();
      auto p_mat = probe_table->down_to_col_main_matrix_table()->get_matrix();

      decltype(count_only_hash_map)::const_accessor ac;
#pragma omp parallel for private(ac) reduction(+ : total_count)
      for (size_t col_idx = 0; col_idx < p_mat->get_col_count(); col_idx++) {
        VID p_vid = col_idx;
        if (probe_has_col_map) {
          p_vid =
              p_col_main_table->get_col_map().value()->as_ptr<VID>()[col_idx];
        }
        TupleValue key;
        key.vid = p_vid;
        if (count_only_hash_map.find(ac, key)) {
          total_count += ac->second * p_mat->count_bits_in_col(col_idx);
        }
      }
      // SPDLOG_INFO("total count {}",total_count);
    }
    break;
  }
  default: {
    assert(0);
  }
  }
  if (!only_count) {
    results_flat_table->convert_to_sequential();
    results = unique_ptr<Table>(results_flat_table);
  } else {
    // SPDLOG_INFO("total count before {}", total_count);
    results->down_to_flat_table()->set(0, 0, &total_count);
  }
}

void HashJoin::run_internal() {
  build();
  probe();
}

TEST(Operator, HashJoinFlatFlat) {
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new HashJoin("k", false));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);

    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::Int64}};

    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<int64_t> k = {1, 1, 1, 2, 2, 3, 3, 4};
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
    std::vector<int64_t> k = {1, 1, 1, 2, 2, 3, 3, 4};

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
  EXPECT_EQ(op->results->tuple_count(), 18);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, HashJoinFlatMat) {
  // Flat build, Mat probe

  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new HashJoin("k", false));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));
    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};

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
    auto table =
        unique_ptr<Table>(new ColMainMatrixTable(data->results_schema));

    auto mat = make_shared<DenseColMainMatrix>(10, 4);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};
    std::map<VID, size_t> k_map;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));
    std::vector<VID> k_mapv = {1, 2, 3, 4};
    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_col_main_matrix_table()->set_matrix(std::move(mat));
    table->down_to_col_main_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }
  op->finishAddingDependency();

  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  EXPECT_EQ(op->results->tuple_count(), 18);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, HashJoinMatFlat) {
  // Mat build, Flat probe
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new HashJoin("k", false));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));
    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};

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
    auto table =
        unique_ptr<Table>(new ColMainMatrixTable(data->results_schema));

    auto mat = make_shared<DenseColMainMatrix>(10, 10);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};
    std::map<VID, size_t> k_map;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));
    std::vector<VID> k_mapv = {1, 2, 3, 4};
    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_col_main_matrix_table()->set_matrix(std::move(mat));
    table->down_to_col_main_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  op->finishAddingDependency();
  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  EXPECT_EQ(op->results->tuple_count(), 18);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, HashJoinMatMat) {
  // Mat build, Mat probe
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new HashJoin("k", false));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::VID}};
    auto table =
        unique_ptr<Table>(new ColMainMatrixTable(data->results_schema));

    auto mat = make_shared<DenseColMainMatrix>(10, 4);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};
    std::map<VID, size_t> k_map;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));
    std::vector<VID> k_mapv = {1, 2, 3, 4};
    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_col_main_matrix_table()->set_matrix(std::move(mat));
    table->down_to_col_main_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"b", Type::VID}, {"k", Type::VID}};
    auto table =
        unique_ptr<Table>(new ColMainMatrixTable(data->results_schema));

    auto mat = make_shared<DenseColMainMatrix>(10, 4);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};
    std::map<VID, size_t> k_map;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));
    std::vector<VID> k_mapv = {1, 2, 3, 4};
    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_col_main_matrix_table()->set_matrix(std::move(mat));
    table->down_to_col_main_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  op->finishAddingDependency();
  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  EXPECT_EQ(op->results->tuple_count(), 18);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, HashJoinFlatFlatCountOnly) {
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new HashJoin("k", true));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);

    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::Int64}};

    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<int64_t> k = {1, 1, 1, 2, 2, 3, 3, 4};
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
    std::vector<int64_t> k = {1, 1, 1, 2, 2, 3, 3, 4};

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
  EXPECT_EQ(op->results->tuple_count(), 1);
  EXPECT_EQ(*reinterpret_cast<int64_t *>(op->results->get_raw_ptr(0, 0)), 18);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, HashJoinFlatMatCountOnly) {
  // Flat build, Mat probe

  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new HashJoin("k", true));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));
    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};

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
    auto table =
        unique_ptr<Table>(new ColMainMatrixTable(data->results_schema));

    auto mat = make_shared<DenseColMainMatrix>(10, 4);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};
    std::map<VID, size_t> k_map;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));
    std::vector<VID> k_mapv = {1, 2, 3, 4};
    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_col_main_matrix_table()->set_matrix(std::move(mat));
    table->down_to_col_main_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }
  op->finishAddingDependency();

  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  EXPECT_EQ(op->results->tuple_count(), 1);
  EXPECT_EQ(*reinterpret_cast<int64_t *>(op->results->get_raw_ptr(0, 0)), 18);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, HashJoinMatFlatCountOnly) {
  // Mat build, Flat probe
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new HashJoin("k", true));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));
    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};

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
    auto table =
        unique_ptr<Table>(new ColMainMatrixTable(data->results_schema));

    auto mat = make_shared<DenseColMainMatrix>(10, 10);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};
    std::map<VID, size_t> k_map;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));
    std::vector<VID> k_mapv = {1, 2, 3, 4};
    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_col_main_matrix_table()->set_matrix(std::move(mat));
    table->down_to_col_main_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  op->finishAddingDependency();
  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  EXPECT_EQ(op->results->tuple_count(), 1);
  EXPECT_EQ(*reinterpret_cast<int64_t *>(op->results->get_raw_ptr(0, 0)), 18);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, HashJoinMatMatCountOnly) {
  // Mat build, Mat probe
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new HashJoin("k", true));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"k", Type::VID}};
    auto table =
        unique_ptr<Table>(new ColMainMatrixTable(data->results_schema));

    auto mat = make_shared<DenseColMainMatrix>(10, 4);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};
    std::map<VID, size_t> k_map;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));
    std::vector<VID> k_mapv = {1, 2, 3, 4};
    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_col_main_matrix_table()->set_matrix(std::move(mat));
    table->down_to_col_main_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"b", Type::VID}, {"k", Type::VID}};
    auto table =
        unique_ptr<Table>(new ColMainMatrixTable(data->results_schema));

    auto mat = make_shared<DenseColMainMatrix>(10, 4);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> k = {1, 1, 1, 2, 2, 3, 3, 4};
    std::map<VID, size_t> k_map;
    for (auto x : k) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(a[i], k_map.at(k[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));
    std::vector<VID> k_mapv = {1, 2, 3, 4};
    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_col_main_matrix_table()->set_matrix(std::move(mat));
    table->down_to_col_main_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  op->finishAddingDependency();
  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  EXPECT_EQ(op->results->tuple_count(), 1);
  EXPECT_EQ(*reinterpret_cast<int64_t *>(op->results->get_raw_ptr(0, 0)), 18);

  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}