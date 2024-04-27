#include "operator/big_operators.hh"
#include "operator/big_operators2.hh"

using namespace std;

Mintersect::Mintersect(string new_vertex_var_name, string new_vertex_type,
                       bool only_count)
    : new_vertex_var_name(new_vertex_var_name),
      new_vertex_type(new_vertex_type), only_count(only_count) {

  vs = graph_store->schema.get_vertex_schema(new_vertex_type);
}

void Mintersect::finishAddingDependency() {
  auto base_schema = deps[0]->results_schema;
  var_base_idx.resize(deps.size());
  edges_u_idx.resize(deps.size());
  edges_nv_idx.resize(deps.size());
  for (size_t i = 1; i < deps.size(); i++) {
    assert(deps[i]->results_schema.columns.size() == 2);
    auto tks = deps[i]->results_schema.get_schema(new_vertex_var_name);
    if (tks->first.type != Type::VID) {
      SPDLOG_ERROR("{}", deps[i]->results_schema.columns);
    }
    assert(tks->first.type == Type::VID);
    if (i == 1) {
      ks = tks->first;
    } else {
      assert(ks == tks->first);
    }
    edges_nv_idx[i] = tks->second;
    size_t u_idx = 1 - tks->second;
    edges_u_idx[i] = u_idx;
    auto u = deps[i]->results_schema.columns[u_idx];
    auto bus = base_schema.get_schema(u.name);
    var_base_idx[i] = bus->second;
    assert(bus->first.type == Type::VID);
  }
  if (only_count) {
    results_schema = TableSchema{{{"count", Type::Int64}}};
  } else {
    results_schema = base_schema;
    results_schema.columns.push_back(ks);
    assert(results_schema.has_duplicate_variable() == false);
  }
}

void Mintersect::run_internal() {
  // Base table, row tables

  // check data
  size_t col_main_table_count = 0;
  optional<size_t> col_main_table_row_count = nullopt;

  for (size_t i = 1; i < dep_results.size(); i++) {
    switch (dep_results[i]->type) {
    case TableType::Flat: {
      break;
    }
    case TableType::ColMainMatrix: {
      SPDLOG_DEBUG("results[{}] schema: {}", i, dep_results[i]->schema.columns);
      assert(dep_results[i]->schema.columns[0] == ks); // Row is the new vertex
      auto table = dep_results[i]->down_to_col_main_matrix_table();

      if (table->col_map.has_value())
        table->set_col_hash_map();
      if (table->row_map.has_value())
        table->set_row_hash_map();
      col_main_table_count++;

      // assert all col_main_table with shape (R,C) has the same R
      if (col_main_table_row_count.has_value()) {
        assert(col_main_table_row_count.value() ==
               table->get_matrix()->get_row_count());
      } else {
        col_main_table_row_count = table->get_matrix()->get_row_count();
      }

      break;
    }
    case TableType::VarRowMatrix: {
      assert(0);
      break;
    }
    default: {
      assert(0);
      break;
    }
    }
  }

  // if all row table is col main table
  size_t cache_row_count = vs.get_count();
  if (col_main_table_count == dep_results.size() - 1) {
    cache_row_count = col_main_table_row_count.value();
  }

  SPDLOG_INFO("{}, base count: {}, intersect deps: {}, target vertex: {}({}), "
              "i-cached row count: {}",
              PhysicalOperator::get_physical_operator_name(this),
              dep_results[0]->tuple_count(), dep_results.size() - 1, vs.type,
              vs.get_count(), cache_row_count);

  size_t max_threads = omp_get_max_threads();

  FlatTable *results_flat_table;
  if (only_count) {
    results_flat_table = new FlatTable(results_schema, false);
  } else {
    results_flat_table = new FlatTable(results_schema, true);
  }

  vector<size_t> base_result_map;
  size_t nv_idx = -1; // new vertex idx
  if (!only_count) {
    base_result_map = dep_results[0]->schema.map_to(results_schema);
    nv_idx = results_schema.get_schema(new_vertex_var_name)->second;
  }

  // intermediate data
  vector<unique_ptr<DenseColMainMatrix>> intersect_results(max_threads);
  for (size_t i = 0; i < max_threads; i++) {
    // 2 col, col-0 is result, col-1 is for flat table
    SPDLOG_INFO("Build Intersect Cache");
    intersect_results[i] = unique_ptr<DenseColMainMatrix>(
        new DenseColMainMatrix(cache_row_count, 2));
  }

  // assume all row map are the same
  std::optional<std::shared_ptr<DiskArray<NoType>>> target_row_map;
  for (size_t i = 1; i < dep_results.size(); i++) {
    if (dep_results[i]->type == TableType::ColMainMatrix) {
      target_row_map =
          dep_results[i]->down_to_col_main_matrix_table()->get_row_map();
      break;
    }
  }

  auto tackle_di_for_base_vid = [this](size_t di, VID bvid,
                                       DenseColMainMatrix *ir) {
    if (dep_results[di]->type == TableType::ColMainMatrix) {
      auto table = dep_results[di]->down_to_col_main_matrix_table();
      auto dcol_idx = table->find_col_idx(bvid);
      if (dcol_idx.has_value()) {
        // SPDLOG_INFO("bvid: {}, dcol_idx:{} count:{}", bvid, dcol_idx.value(),
        // dep_results[di]
        //     ->down_to_col_main_matrix_table()
        //     ->get_matrix()
        //     ->count_bits_in_col(dcol_idx.value()));
        ir->and_col_from(
            0, *dep_results[di]->down_to_col_main_matrix_table()->get_matrix(),
            dcol_idx.value());
      } else {
        ir->set_col_to(0, false);
      }

    } else {
      // TODO: Optimize Scanning Flat Table
      auto flat_table = dep_results[di]->down_to_flat_table();
      ir->set_col_to(1, false);
      VID u, payload;
      for (size_t i = 0; i < flat_table->tuple_count(); i++) {
        flat_table->get_nt(i, edges_u_idx[di], &u);
        if (u != bvid)
          continue;
        flat_table->get_nt(i, edges_nv_idx[di], &payload);
        // SPDLOG_INFO("to be intersect {}, vb:{},
        // nvb:{}",flat_table->tuple_to_string(i),var_base_idx[di],edges_nv_idx[di]);
        ir->set(payload, 1, true);
      }
      ir->and_col_from(0, *ir, 1);
    }
  };

  // auto after_intersect_a_tuple =
  //     [this,results_flat_table,&base_result_map,&target_row_map,nv_idx](DenseColMainMatrix*
  //     ir,size_t bi,void* (*f)(FlatTable*,size_t,size_t) ) {
  //       size_t num = ir->count_bits_in_col(0);
  //       SPDLOG_TRACE("bi: {}, num: {}", bi, num);
  //       total_num.fetch_add(num, std::memory_order_relaxed);

  //       if (num == 0 || only_count) {
  //         return;
  //       }

  //       // push to results
  //       size_t start = results_flat_table->atomic_alloc(num);
  //       BitmapIterator bit(
  //           reinterpret_cast<BitmapIterator::V *>(ir->get_col_start(0)),
  //           ir->get_row_count());
  //       VID x;
  //       for (size_t oi = start; oi < start + num; oi++) {
  //         bit.next(x);
  //         for (size_t i = 0; i < base_result_map.size(); i++) {
  //           results_flat_table->hold_set(oi, base_result_map[i],
  //                                           f(bi,i));
  //         }
  //         if (target_row_map.has_value()) {
  //           x = target_row_map.value()->as_ptr<VID>()[x];
  //         }
  //         results_flat_table->hold_set(oi, nv_idx, &x);
  //       }
  //     };

  if (dep_results[0]->type == TableType::Flat) {
    // decide base type
    auto base_input = dep_results[0]->down_to_flat_table();
// run
#pragma omp parallel for
    for (size_t bi = 0; bi < base_input->tuple_count(); bi++) {
      // tasks are pre-assigned, no need to concurrent control
      auto &ir = intersect_results[omp_get_thread_num()];
      ir->set_col_to(0, true);
      // SPDLOG_INFO("{}", base_input->tuple_to_string(bi));
      for (size_t di = 1; di < dep_results.size(); di++) {
        VID bvid = base_input->get_T<VID>(bi, var_base_idx[di]);
        tackle_di_for_base_vid(di, bvid, ir.get());
      }

      size_t num = ir->count_bits_in_col(0);
      // SPDLOG_INFO("bi: {}, num: {}", bi, num);
      total_num.fetch_add(num, std::memory_order_relaxed);

      if (num == 0 || only_count) {
        continue;
      }

      BitmapIterator bit(
          reinterpret_cast<BitmapIterator::V *>(ir->get_col_start(0)),
          ir->get_row_count());
      VID x;
      while (bit.next(x)) {

        for (size_t i = 0; i < base_result_map.size(); i++) {
          results_flat_table->push_back_single(base_result_map[i],
                                               base_input->get_raw_ptr(bi, i));
        }
        if (target_row_map.has_value()) {
          x = target_row_map.value()->as_ptr<VID>()[x];
        }
        results_flat_table->push_back_single(nv_idx, &x);
      }
    }
  } else if (dep_results[0]->type == TableType::VarRowMatrix) {
    auto base_input = dep_results[0]->down_to_var_row_matrix_table();

    auto iters = base_input->get_iterators(max_threads);

    // atomic_size_t finished = 0;
#pragma omp parallel for
    for (auto &iter : iters) {
      VID base_vid[2];
      while (iter.next(base_vid[0], base_vid[1])) {
        auto &ir = intersect_results[omp_get_thread_num()];
        ir->set_col_to(0, true);
        for (size_t di = 1; di < dep_results.size(); di++) {
          VID bvid = base_vid[var_base_idx[di]];
          if (dep_results[di]->type == TableType::ColMainMatrix) {
            auto table = dep_results[di]->down_to_col_main_matrix_table();
            auto dcol_idx = table->find_col_idx(bvid);
            if (dcol_idx.has_value()) {
              ir->and_col_from(0,
                               *dep_results[di]
                                    ->down_to_col_main_matrix_table()
                                    ->get_matrix(),
                               dcol_idx.value());
            } else {
              ir->set_col_to(0, false);
            }

          } else {
            auto flat_table = dep_results[di]->down_to_flat_table();
            ir->set_col_to(1, false);
            VID u, payload;
            for (size_t i = 0; i < flat_table->tuple_count(); i++) {

              flat_table->get_nt(i, var_base_idx[di], &u);
              if (u != bvid)
                continue;

              flat_table->get_nt(i, edges_nv_idx[di], &payload);
              ir->set(payload, 1, true);
            }
            ir->and_col_from(0, *ir, 1);
          }
        }

        size_t num = ir->count_bits_in_col(0);
        // SPDLOG_INFO("({},{}): {}", base_vid[0], base_vid[1],num);
        total_num.fetch_add(num, std::memory_order_relaxed);
        if (num == 0 || only_count) {
          continue;
        }

        // size_t start = results_flat_table->atomic_alloc(num);
        // BitmapIterator bit(
        //     reinterpret_cast<BitmapIterator::V *>(ir->get_col_start(0)),
        //     ir->get_row_count());
        // VID x;
        // for (size_t oi = start; oi < start + num; oi++) {
        //   bit.next(x);
        //   for (size_t i = 0; i < base_result_map.size(); i++) {
        //     results_flat_table->hold_set(oi, base_result_map[i],
        //     &base_vid[i]);
        //   }
        //   if (target_row_map.has_value()) {
        //     x = target_row_map.value()->as_ptr<VID>()[x];
        //   }
        //   results_flat_table->hold_set(oi, nv_idx, &x);
        // }

        // size_t me_idx = finished.fetch_add(1);
        // if (finished.load() % 10000 == 0) {
        //   SPDLOG_INFO("{} finished, total {}", me_idx,
        //               results_flat_table->tuple_count());
        // }

        BitmapIterator bit(
            reinterpret_cast<BitmapIterator::V *>(ir->get_col_start(0)),
            ir->get_row_count());
        VID x;
        while (bit.next(x)) {
          for (size_t i = 0; i < base_result_map.size(); i++) {
            results_flat_table->push_back_single(base_result_map[i],
                                                 &base_vid[i]);
          }
          if (target_row_map.has_value()) {
            x = target_row_map.value()->as_ptr<VID>()[x];
          }
          results_flat_table->push_back_single(nv_idx, &x);
        }
      }
    }
  } else {
    assert(0);
  }
  SPDLOG_INFO("{} results {} tuples",
              PhysicalOperator::get_physical_operator_name(this), total_num);
  if (only_count) {
    results_flat_table->atomic_alloc(1);
    results_flat_table->set(0, 0, &total_num);
  }
  results_flat_table->convert_to_sequential();
  results = unique_ptr<Table>(results_flat_table);
}

TEST(Operator, MintersectFlatFlatFlat) {
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new Mintersect("c", "Person", false));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"b", Type::VID}};
    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));
    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> b = {1, 1, 1, 2, 2, 3, 3, 4};

    table->down_to_flat_table()->get_col_diskarray(0)->push_many_nt(a.data(),
                                                                    a.size());
    table->down_to_flat_table()->get_col_diskarray(1)->push_many_nt(b.data(),
                                                                    b.size());

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"c", Type::VID}};
    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));
    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> c = {1, 1, 1, 2, 2, 3, 3, 4};

    table->down_to_flat_table()->get_col_diskarray(0)->push_many_nt(a.data(),
                                                                    a.size());
    table->down_to_flat_table()->get_col_diskarray(1)->push_many_nt(c.data(),
                                                                    c.size());

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }
  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"b", Type::VID}, {"c", Type::VID}};
    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));
    std::vector<VID> b = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> c = {1, 1, 1, 2, 2, 3, 3, 4};

    table->down_to_flat_table()->get_col_diskarray(0)->push_many_nt(b.data(),
                                                                    b.size());
    table->down_to_flat_table()->get_col_diskarray(1)->push_many_nt(c.data(),
                                                                    c.size());

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  op->finishAddingDependency();
  op->run();

  op->get_merged_prof().report();

  SPDLOG_INFO("{}", op->results_schema.columns);
  op->results->print(nullopt);
  EXPECT_EQ(op->results->tuple_count(), 3);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}

TEST(Operator, MintersectFlatMatMat) {
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(new Mintersect("c", "Person", false));

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"a", Type::VID}, {"b", Type::VID}};
    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));
    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> b = {1, 1, 1, 2, 2, 3, 3, 4};

    table->down_to_flat_table()->get_col_diskarray(0)->push_many_nt(a.data(),
                                                                    a.size());
    table->down_to_flat_table()->get_col_diskarray(1)->push_many_nt(b.data(),
                                                                    b.size());

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"c", Type::VID}, {"a", Type::VID}};
    auto table =
        unique_ptr<Table>(new ColMainMatrixTable(data->results_schema));
    auto mat = make_shared<DenseColMainMatrix>(10, 10);

    std::vector<VID> a = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> c = {1, 1, 1, 2, 2, 3, 3, 4};

    std::map<VID, size_t> k_map;
    std::vector<VID> k_mapv;
    for (auto x : a) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
        k_mapv.push_back(x);
      }
    }

    for (size_t i = 0; i < a.size(); i++) {
      mat->set(c[i], k_map.at(a[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));

    col_map->push_many_nt(k_mapv.data(), k_mapv.size());

    table->down_to_col_main_matrix_table()->set_matrix(std::move(mat));
    table->down_to_col_main_matrix_table()->get_col_map() = std::move(col_map);

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }
  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"c", Type::VID}, {"b", Type::VID}};
    auto table =
        unique_ptr<Table>(new ColMainMatrixTable(data->results_schema));

    auto mat = make_shared<DenseColMainMatrix>(10, 10);

    std::vector<VID> b = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<VID> c = {1, 1, 1, 2, 2, 3, 3, 4};

    std::map<VID, size_t> k_map;
    std::vector<VID> k_mapv;
    for (auto x : b) {
      if (k_map.contains(x) == false) {
        k_map[x] = k_map.size();
        k_mapv.push_back(x);
      }
    }

    for (size_t i = 0; i < b.size(); i++) {
      mat->set(c[i], k_map.at(b[i]), true);
    }

    shared_ptr<DiskArray<NoType>> col_map = make_shared<DiskArray<NoType>>();
    col_map->set_sizeof_data(sizeof(VID));

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
  EXPECT_EQ(op->results->tuple_count(), 3);
  SPDLOG_INFO("{} Tuples", op->results->tuple_count());
}