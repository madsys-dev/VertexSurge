#include "operator/big_operators.hh"
#include "operator/big_operators2.hh"
#include <algorithm>

using namespace std;

VExpand::VExpand(TypeName start_type, TypeName target_type, TypeName edge_type,
                 Direction dir, size_t k_min, size_t k_max,
                 ExpandPathType path_type, ExpandStrategy strategy,
                 VariableName start_variable_name,
                 optional<VariableName> target_variable_name)
    : VExpand(start_type, target_type, edge_type, dir, k_min, k_max,
              VExpandPathParameters{path_type}, strategy, start_variable_name,
              target_variable_name) {}

VExpand::VExpand(TypeName src_type, TypeName dst_type, TypeName edge_type,
                 Direction dir, VExpandPathParameters path_params,
                 ExpandStrategy strategy, VariableName variable_name,
                 VariableName target_variable_name)
    : VExpand(src_type, dst_type, edge_type, dir, 1,
              VExpandPathParameters::K_MAX_FIND_SHORTEST, path_params, strategy,
              variable_name, target_variable_name) {
  assert(path_params.path_type == ExpandPathType::FindShortest);
  assert(path_params.with_distance == true);
}

VExpand::VExpand(TypeName start_type, TypeName target_type, TypeName edge_type,
                 Direction dir, size_t k_min, size_t k_max,
                 VExpandPathParameters path_params, ExpandStrategy strategy,
                 VariableName start_variable_name,
                 optional<VariableName> target_variable_name)
    : VExpand({ExpandEdgeParam(start_type, target_type, edge_type, dir)}, k_min,
              k_max, path_params, strategy, start_variable_name,
              target_variable_name) {}

VExpand::VExpand(vector<ExpandEdgeParam> _edge_params, size_t k_min,
                 size_t k_max, VExpandPathParameters path_params,
                 ExpandStrategy strategy, VariableName start_variable_name,
                 optional<VariableName> target_variable_name)
    : VExpand(MultipleStepExpandParams(k_min, k_max)
                  .set_expand_edge_params_all(_edge_params),
              path_params, strategy, start_variable_name,
              target_variable_name) {}

VExpand::VExpand(MultipleStepExpandParams multi_step_edge_params,
                 VExpandPathParameters path_params, ExpandStrategy strategy,
                 VariableName start_variable_name,
                 std::optional<VariableName> target_variable_name)
    : k_min(multi_step_edge_params.k_min), k_max(multi_step_edge_params.k_max),
      multi_step_edge_params(multi_step_edge_params),
      start_variable_name(start_variable_name),
      target_variable_name(target_variable_name), path_params(path_params),
      strategy(strategy), status(path_params.path_type, strategy) {

  set_total_time_on();

  for (auto &ep : this->multi_step_edge_params.multi_params) {
    for (auto &p : ep) {
      string edge_name =
          fmt::format("{}_{}_{}", p.start_type, p.edge_type, p.target_type);
      p.es = graph_store->schema.get_edge_schema(edge_name);
      // SPDLOG_INFO("edge params: {}", p.edge_type);
      if (start_type.has_value()) {
        assert(start_type.value() == p.start_type);
      } else {
        start_type = p.start_type;
      }

      if (target_type.has_value()) {
        assert(target_type.value() == p.target_type);
      } else {
        target_type = p.target_type;
      }

      auto new_ss = graph_store->schema.get_vertex_schema(p.start_type);
      if (ss.has_value()) {
        assert(ss.value() == new_ss);
      } else {
        ss = new_ss;
      }

      auto new_ts = graph_store->schema.get_vertex_schema(p.target_type);
      if (ts.has_value()) {
        assert(ts.value() == new_ts);
      } else {
        ts = new_ts;
      }
      if (k_max > 1 && p.es.is_self_edge() == false) {
        SPDLOG_ERROR("k max should be 1 when edge is not self edge");
        exit(1);
      }
    }
  }

  for (size_t i = 1; i <= k_max; i++) {
    addChild(unique_ptr<PhysicalOperator>(
        new Expand<MT>(fmt::format("Expand {}", i), status,
                       this->multi_step_edge_params.multi_params[i - 1],
                       path_params.path_type, strategy, dense_strategy)));
  }
}

void VExpand::finishAddingDependency() {
  assert(deps.size() == 1);
  auto start_vs =
      deps[0]->results_schema.get_schema(start_variable_name).value().first;
  ValueSchema target_vs = {target_variable_name.has_value()
                               ? target_variable_name.value()
                               : fmt::format("{}.id", target_type.value()),
                           Type::VID};
  if (path_params.path_type == ExpandPathType::FindShortest) {
    target_vs = deps[0]
                    ->results_schema.get_schema(target_variable_name.value())
                    .value()
                    .first;
  }

  results_schema = TableSchema{{start_vs, target_vs}};

  if (path_params.with_distance) {
    results_schema.add_non_duplicated_variable(
        ValueSchema{"distance", Type::Int32});
  }

  switch (strategy) {
  case ExpandStrategy::CSR: {
    results = shared_ptr<Table>(new FlatTable(results_schema, true));
    break;
  }
  case ExpandStrategy::Dense: {
    switch (path_params.path_type) {
    case ExpandPathType::Any:
      [[fallthrough]];
    case ExpandPathType::Shortest: {
      if (path_params.with_distance) {
        if (std::is_same<MTT, ColMainMatrixTable>::value) {
          results = shared_ptr<Table>(new MultipleMatrixTable(
              results_schema, TableType::MultipleColMainMatrix));
        } else if (std::is_same<MTT, VarRowMatrixTable>::value) {
          results = shared_ptr<Table>(new MultipleMatrixTable(
              results_schema, TableType::MultipleVarRowMatrix));
        } else {
          assert(false);
        }
      } else {
        results = shared_ptr<Table>(new MTT(results_schema));
      }
      break;
    }
    case ExpandPathType::FindShortest: {
      // will use dep_results[0]
      break;
    }
    default: {
      assert(0);
    }
    }
    break;
  }
  default: {
    assert(0);
  }
  }
}

bool VExpand::before_execution() {
  SPDLOG_DEBUG("{} Run {}", get_physical_operator_name(this), __FUNCTION__);
  start_variable =
      get_flat_variable_from_dep_results(start_variable_name).value();
  if (start_variable.data->size() == 0) {
    SPDLOG_WARN("No Start Vertex for VExpand!");
    return false;
  }

  if (path_params.path_type == ExpandPathType::FindShortest) {
    target_variable =
        get_flat_variable_from_dep_results(target_variable_name.value())
            .value();
    results = dep_results[0];
    Int32 init_dis = -1;
    results->down_to_flat_table()->extend_horizontal(
        ValueSchema{"distance", Type::Int32}, &init_dis);
    assert(results->schema ==
           results_schema); // check this is ok since results schema is set on
                            // adding dependencies

    distance_variable = get_flat_variable_from_my_result("distance").value();
    target_found_count = 0;
    for (size_t i = 0; i < start_variable.size(); i++) {
      if (start_variable.data_as<VID>()[i] ==
          target_variable.data_as<VID>()[i]) {
        distance_variable.data_as<Int32>()[i] = 0;
        target_found_count += 1;
      }
    }
  }

  SPDLOG_DEBUG("Expand Start from {}, count {}", start_variable_name,
               start_variable.size());
  // drop v is safe, cause flat variable is in flat table.

  size_t total_edge_count = 0; // get total edge count to decide stack size
  for (const auto &p : multi_step_edge_params.multi_params[0]) {
    // only consider first step, in most cases this is ok
    total_edge_count += p.es.get_count();
  }
  status.set_start_vertex_and_possible_matrix(
      {start_variable.data_as<VID>(), start_variable.size()},
      ts.value().get_count(), total_edge_count);
  if (strategy == ExpandStrategy::Dense) {
    // Timer t("init and pre load all multi m layer");
    status.out_layer_idx = 0;
    size_t init_layer_count = k_max < 3 ? 3 : k_max;
    status.init_multiple_m_layer(init_layer_count);
    // status.load_all_multi_m_layer_and_visit();
  }

  last_visit_count = status.visit_count;
  return true;
}

bool VExpand::after_child(size_t i) {
  k_actual_max = i + 1;
  switch (path_params.path_type) {
  case ExpandPathType::Any: {
    break;
  }
  case ExpandPathType::Shortest: {
    if (last_visit_count == status.visit_count) {
      SPDLOG_INFO("{} expand {} visits, no more visit, return status visit", i,
                  last_visit_count);
      return false;
    }
    last_visit_count = status.visit_count;
    break;
  }
  case ExpandPathType::FindShortest: {
    switch (strategy) {
    case ExpandStrategy::CSR: {
      assert(0);
    }
    case ExpandStrategy::Dense: {
      size_t d = i + 1;
      assert(status.out_layer_idx == i + 1);

      auto &m = status.get_layer_m(d);
      VID *target_v = target_variable.data_as<VID>();
      // VID *start_v= start_variable.data_as<VID>();
      Int32 *dis = distance_variable.data_as<Int32>();
      for (VID r = 0; r < start_variable.size(); r++) {
        if (dis[r] == -1 && m.get(r, target_v[r]) == true) {
          // SPDLOG_INFO("Found {} to {} length {}",start_v[r],target_v[r],d);
          dis[r] = d;
          target_found_count += 1;
        }
      }

      if (target_found_count == distance_variable.size()) {
        // SPDLOG_INFO("found {} targets",target_found_count);
        SPDLOG_INFO("all targets found, return ");
        return false;
      }

      if (last_visit_count == status.visit_count) {
        SPDLOG_INFO("{} expand, {} visits, no more visit, return status visit",
                    i + 1, last_visit_count);
        return false;
      }
      last_visit_count = status.visit_count;
      break;
    }
    default: {
      assert(0);
    }
    }

    break;
  }
  default: {
    assert(0);
  }
  }
  return true;
}

template <typename MT>
shared_ptr<void> make_merged(size_t row_count, size_t column_count,
                             size_t stack_size) {
  if constexpr (std::is_same<MT, DenseColMainMatrix>::value) {
    return make_shared<MT>(row_count, column_count);
  } else if (std::is_same<MT, DenseVarStackMatrix>::value) {
    return make_shared<MT>(row_count, column_count, stack_size);
  } else {
    assert(0);
  }
}

void VExpand::merge_to_MT() {
  std::shared_ptr<MT> merged;
  vector<MT *> fast_union;
  switch (path_params.path_type) {
  case ExpandPathType::Any: {
    break;
  }
  case ExpandPathType::Shortest: {
    if (k_min == 0) {
      SPDLOG_INFO("k_min == 0, return status visit");
      merged = std::move(status.visit);
      goto after_merge;
    }
    break;
  }
  default: {
    assert(0);
  }
  }

  merged = static_pointer_cast<MT>(
      make_merged<MT>(status.v.second, status.target_count, status.stack_size));

  fast_union.push_back(merged.get());

  for (size_t i = k_min; i <= k_actual_max; i++) {
    auto layer_type = status.get_type_of_layer(i);
    switch (layer_type) {
    case 0: {
      auto &csr = status.get_layer_csr(i);
#pragma omp parallel for
      for (size_t r_idx = 0; r_idx < status.v.second; r_idx++) {
        auto [c, cl] = csr.get_data(r_idx);
        for (size_t c_idx = 0; c_idx < cl; c_idx++) {
          merged->set_atomic(r_idx, c[c_idx], true);
        }
      }
      break;
    }
    case 1: {
      // using raw ptr is Ok here
      auto &m = status.get_layer_m(i);
      fast_union.push_back(&m);
      break;
    }
    default: {
      assert(0);
    }
    }
  }
  destructive_fast_union(fast_union.data(), fast_union.size());

after_merge:
  results->down_to_table<MTT>()->get_row_map() = start_variable.data;
  reinterpret_cast<MTT *>(results->down_to_table<MTT>())
      ->set_matrix(std::move(merged));
}

void VExpand::copy_to_multi_MT() {
  SPDLOG_INFO("Export Multiple Matrix");
  auto multi_table = results->down_to_table<MultipleMatrixTable>();
  TableSchema schema_without_distance = results_schema;
  schema_without_distance.columns.pop_back();
  for (size_t i = k_min; i <= k_actual_max; i++) {
    auto layer_type = status.get_type_of_layer(i);
    switch (layer_type) {
    case 1: {

      auto new_mtt = std::shared_ptr<Table>(new MTT(schema_without_distance));
      auto new_mt = new_mtt->down_to_table<MTT>();
      new_mt->get_row_map() = start_variable.data;
      new_mt->set_matrix(status.get_layer_m_ptr(i));
      multi_table->add_mat(new_mtt, i);
      break;
    }

    default:
      assert(0);
    }
  }
}

void VExpand::merge_to_Flat() {
  auto flat = results->down_to_flat_table();
  switch (path_params.path_type) {
  case ExpandPathType::Any: {
    vector<unordered_set<VID>> dedup(status.get_vertex_count());

    for (size_t i = k_min; i <= k_actual_max; i++) {
      auto &csr = status.get_layer_csr(i);
      Int32 dis = i;
#pragma omp parallel for
      for (size_t r_idx = 0; r_idx < status.v.second; r_idx++) {
        auto [c, cl] = csr.get_data(r_idx);
        for (size_t c_idx = 0; c_idx < cl; c_idx++) {
          if (dedup[r_idx].contains(c[c_idx]) == false) {
            flat->push_back_single(0, &status.v.first[r_idx]);
            flat->push_back_single(1, &c[c_idx]);
            if (path_params.with_distance) {
              flat->push_back_single(2, &dis);
            }
            dedup[r_idx].insert(c[c_idx]);
          }
        }
      }
    }
    break;
  }
  case ExpandPathType::Shortest: {
    if (k_min == 0 && path_params.with_distance == false) {
      SPDLOG_INFO("k_min == 0 and do not output distance, use sparse visit");
      flat->resize(status.visit_count);
      atomic_size_t cnt = 0;
#pragma omp parallel for
      for (size_t r = 0; r < status.get_vertex_count(); r++) {
        VID start_v = status.v.first[r];
        for (const auto &c : status.get_sparse_visit()[r]) {
          VID target_v = c;

          flat->push_back_single(0, &start_v);
          flat->push_back_single(1, &target_v);
        }
      }
      assert(cnt == flat->tuple_count());
    } else {
      for (size_t i = k_min; i <= k_actual_max; i++) {
        auto &csr = status.get_layer_csr(i);
        Int32 dis = i;
#pragma omp parallel for
        for (size_t r_idx = 0; r_idx < status.v.second; r_idx++) {
          auto [c, cl] = csr.get_data(r_idx);
          for (size_t c_idx = 0; c_idx < cl; c_idx++) {
            flat->push_back_single(0, &status.v.first[r_idx]);
            flat->push_back_single(1, &c[c_idx]);
            if (path_params.with_distance) {
              flat->push_back_single(2, &dis);
            }
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
  flat->convert_to_sequential();
}

void VExpand::run_internal() {

  if (early_end) {
    SPDLOG_INFO("VExpand early end");
    return;
  }

  SPDLOG_INFO("{}: Actual Expand Steps: {}, Strategy: {}, DenseStrategy: {}, "
              "PathParameters {}",
              get_physical_operator_name(this), k_actual_max, strategy,
              strategy == ExpandStrategy::CSR ? "None"
                                              : fmt::to_string(dense_strategy),
              path_params);
  if (path_params.path_type == ExpandPathType::FindShortest) {
    SPDLOG_INFO("Find Shortest Path, {} out of {} found", target_found_count,
                distance_variable.size());
    return;
  }
  switch (strategy) {
  case ExpandStrategy::CSR: {
    merge_to_Flat(); // with distance is considered in merge_to_Flat();
    break;
  }
  case ExpandStrategy::Dense: {
    if (path_params.with_distance) {
      copy_to_multi_MT();
    } else {
      merge_to_MT();
    }
    break;
  }
  default: {
    assert(0);
  }
  }
}

TEST(Operator, VExpandMatrix) {
  // Test AnyShortest and AnyPath respectively
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();
  {
    auto start_type = "Person";
    auto target_type = "Person";
    auto edge_type = "knows";
    auto dir = Direction::Forward;
    auto k_min = 1;
    auto k_max = 2;
    auto path_type = ExpandPathType::Shortest;
    auto variable_name = "v";
    auto op = unique_ptr<BigOperator>(new VExpand(
        start_type, target_type, edge_type, dir, k_min, k_max, path_type,
        ExpandStrategy::Dense, variable_name, std::nullopt));

    auto data = unique_ptr<BigOperator>(new InlinedData);

    data->results_schema.columns.push_back(ValueSchema{"v", Type::VID});

    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));

    auto &array = table->down_to_flat_table()->get_col_diskarray(0);
    array->resize(2000);
    uniform_random_numbers(op->graph_store->gen,
                           dynamic_cast<VExpand *>(op.get())->ss->get_count(),
                           array->as_ptr<VID>(), array->size());

    data->results = std::move(table);

    op->addLastDependency(std::move(data));

    op->run();

    SPDLOG_INFO("VEpand Test Ok, {}", op->results->tuple_count());

    op->get_merged_prof().report();
  }

  {
    auto start_type = "Person";
    auto target_type = "Person";
    auto edge_type = "knows";
    auto dir = Direction::Forward;
    auto k_min = 1;
    auto k_max = 2;
    auto path_type = ExpandPathType::Any;
    auto variable_name = "v";
    auto op = unique_ptr<BigOperator>(new VExpand(
        start_type, target_type, edge_type, dir, k_min, k_max, path_type,
        ExpandStrategy::Dense, variable_name, std::nullopt));

    auto data = unique_ptr<BigOperator>(new InlinedData);

    data->results_schema.columns.push_back(ValueSchema{"v", Type::VID});

    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));

    auto &array = table->down_to_flat_table()->get_col_diskarray(0);
    array->resize(2000);
    uniform_random_numbers(op->graph_store->gen,
                           dynamic_cast<VExpand *>(op.get())->ss->get_count(),
                           array->as_ptr<VID>(), array->size());

    data->results = std::move(table);

    op->addLastDependency(std::move(data));

    op->run();

    SPDLOG_INFO("VEpand Test Ok, {}", op->results->tuple_count());

    op->get_merged_prof().report();
  }
}

TEST(Operator, VExpandFlat) {
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();
  {
    mt19937 gen(1234);
    auto start_type = "Person";
    auto target_type = "Person";
    auto edge_type = "knows";
    auto dir = Direction::Forward;
    auto k_min = 1;
    auto k_max = 2;
    auto path_type = ExpandPathType::Shortest;
    auto variable_name = "v";
    auto op = unique_ptr<BigOperator>(new VExpand(
        start_type, target_type, edge_type, dir, k_min, k_max, path_type,
        ExpandStrategy::CSR, variable_name, std::nullopt));

    auto data = unique_ptr<BigOperator>(new InlinedData);

    data->results_schema.columns.push_back(ValueSchema{"v", Type::VID});

    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));

    auto &array = table->down_to_flat_table()->get_col_diskarray(0);
    array->resize(2000);
    uniform_random_numbers(gen,
                           dynamic_cast<VExpand *>(op.get())->ss->get_count(),
                           array->as_ptr<VID>(), array->size());

    data->results = std::move(table);

    op->addLastDependency(std::move(data));

    op->run();

    SPDLOG_INFO("VEpand Test Ok, {}", op->results->tuple_count());

    // for (size_t i = 0; i < op->results->down_to_flat_table()->tuple_count();
    //      i++) {
    //   SPDLOG_INFO("{}",
    //   op->results->down_to_flat_table()->tuple_to_string(i));
    // }
  }

  {
    mt19937 gen(1234);
    auto start_type = "Person";
    auto target_type = "Person";
    auto edge_type = "knows";
    auto dir = Direction::Forward;
    auto k_min = 1;
    auto k_max = 2;
    auto path_type = ExpandPathType::Any;
    auto variable_name = "v";
    auto op = unique_ptr<BigOperator>(new VExpand(
        start_type, target_type, edge_type, dir, k_min, k_max, path_type,
        ExpandStrategy::CSR, variable_name, std::nullopt));

    auto data = unique_ptr<BigOperator>(new InlinedData);

    data->results_schema.columns.push_back(ValueSchema{"v", Type::VID});

    auto table = unique_ptr<Table>(new FlatTable(data->results_schema));

    auto &array = table->down_to_flat_table()->get_col_diskarray(0);
    array->resize(2000);
    uniform_random_numbers(gen,
                           dynamic_cast<VExpand *>(op.get())->ss->get_count(),
                           array->as_ptr<VID>(), array->size());

    data->results = std::move(table);

    op->addLastDependency(std::move(data));

    op->run();

    SPDLOG_INFO("VEpand Test Ok, {}", op->results->tuple_count());

    // for (size_t i = 0; i < op->results->down_to_flat_table()->tuple_count();
    //      i++) {
    //   SPDLOG_INFO("{}",
    //   op->results->down_to_flat_table()->tuple_to_string(i));
    // }
  }
}