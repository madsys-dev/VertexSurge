#include "operator/operators_impls2.hh"

using namespace std;

Count::Count() {}
void Count::run_internal() {
  assert(dep_results.size() == 1);
  results_schema.columns.push_back({"count", Type::Int64});
  auto flat_table = new FlatTable(results_schema);
  Int64 count = dep_results[0]->tuple_count();
  flat_table->resize(1);
  flat_table->set(0, 0, &count);
  results = unique_ptr<Table>(flat_table);
}

TableConvert::TableConvert(TableType type, bool transpose)
    : convert_to(type), transpose(transpose) {}

void TableConvert::finishAddingDependency() {
  assert(deps.size() == 1);
  results_schema = deps[0]->results_schema;
  if (transpose) {
    assert(results_schema.columns.size() == 2);
    swap(results_schema.columns[0], results_schema.columns[1]);
  }
}

void TableConvert::run_internal() {
  if (convert_to == TableType::ColMainMatrix &&
      dep_results[0]->type == TableType::VarRowMatrix) {

    auto src_mat = dep_results[0]->down_to_var_row_matrix_table()->get_matrix();
    shared_ptr<DenseColMainMatrix> dst_mat;
    if (transpose) {
      dst_mat = make_shared<DenseColMainMatrix>(src_mat->get_col_count(),
                                                src_mat->get_row_count());
    } else {

      dst_mat = make_shared<DenseColMainMatrix>(src_mat->get_row_count(),
                                                src_mat->get_col_count());
    }
    auto dst_table = new ColMainMatrixTable(results_schema);
    dst_table->take_maps_from(dep_results[0]->down_to_var_row_matrix_table());

    if (transpose) {
      // TODO: more efficient way to do this
      swap(dst_table->get_row_map(), dst_table->get_col_map());
      swap(dst_table->row_hash_map, dst_table->col_hash_map);
#pragma omp parallel for
      for (size_t r = 0; r < src_mat->get_row_count(); r++) {
        for (size_t c = 0; c < src_mat->get_col_count(); c++) {
          dst_mat->set(c, r, src_mat->get(r, c));
        }
      }
    } else {
      dst_mat->copy_from_variable_row_mat(src_mat);
    }

    dst_table->set_matrix(std::move(dst_mat));
    results = unique_ptr<Table>(dst_table);

  } else if (convert_to == TableType::Flat) {

    auto flat_table = new FlatTable(results_schema, true);
    if (dep_results[0]->type == TableType::ColMainMatrix) {
      convert_BMT_to_Flat<ColMainMatrixTable>(
          flat_table, dep_results[0]->down_to_col_main_matrix_table());
    } else if (dep_results[0]->type == TableType::VarRowMatrix) {
      convert_BMT_to_Flat<VarRowMatrixTable>(
          flat_table, dep_results[0]->down_to_var_row_matrix_table());
    } else if (dep_results[0]->type == TableType::MultipleColMainMatrix ||
               dep_results[0]->type == TableType::MultipleVarRowMatrix) {
      dep_results[0]->down_to_table<MultipleMatrixTable>()->output_to_flat(
          flat_table);
    } else {
      assert(0);
    }
    flat_table->convert_to_sequential();

    results = unique_ptr<Table>(flat_table);
  } else {
    SPDLOG_ERROR("Convert from {} to {} not supported", dep_results[0]->type,
                 convert_to);
    exit(1);
  }
}

Projection::Projection(
    std::vector<std::pair<VariableName, VariableName>> projection_map)
    : projection_map(projection_map) {}

void Projection::finishAddingDependency() {
  assert(deps.size() == 1);
  results_schema = TableSchema{};
  for (auto &[a, b] : projection_map) {
    auto old_vs = deps[0]->results_schema.get_schema(a);
    if (old_vs.has_value() == false) {
      SPDLOG_ERROR("Variable {} not found in schema {}", a,
                   deps[0]->results_schema);
      assert(old_vs.has_value());
    }
    auto vs = old_vs.value().first;
    vs.name = b;
    results_schema.columns.push_back(vs);
    old_variable_idx.push_back(old_vs.value().second);
  }
}

void Projection::run_internal() {
  switch (dep_results[0]->type) {
  case TableType::Flat: {
    auto flat_table = new FlatTable(results_schema);
    for (size_t i = 0; i < old_variable_idx.size(); i++) {
      flat_table->get_col_diskarray(i) =
          dep_results[0]->down_to_flat_table()->get_col_diskarray(
              old_variable_idx[i]);
    }
    results = unique_ptr<Table>(flat_table);
    break;
  }
  case TableType::ColMainMatrix: {
    assert(results_schema.columns.size() == 2);
    dep_results[0]->schema = results_schema;
    break;
  }
  case TableType::VarRowMatrix: {
    assert(results_schema.columns.size() == 2);
    dep_results[0]->schema = results_schema;
    break;
  }

  default: {
    assert(0);
  }
  }
}

MatrixTableSelect::MatrixTableSelect(VariableName vname) : key_name(vname) {}

void MatrixTableSelect::finishAddingDependency() {
  assert(deps.size() == 2);
  results_schema = deps[0]->results_schema;
  assert(results_schema.columns.size() == 2);

  ks = deps[0]->results_schema.get_schema(key_name)->first;
  for (size_t i = 0; i < deps.size(); i++) {
    assert(ks == deps[i]->results_schema.get_schema(key_name)->first);
    key_idx.push_back(deps[i]->results_schema.get_schema(key_name)->second);
  }
}

void MatrixTableSelect::run_internal() {
  switch (dep_results[0]->type) {
  case TableType::VarRowMatrix: {
    assert(key_idx[0] == 1);
    assert(dep_results[1]->type == TableType::Flat);

    results = shared_ptr<Table>(new VarRowMatrixTable(results_schema));
    auto new_table = results->down_to_var_row_matrix_table();
    auto old_table = dep_results[0]->down_to_var_row_matrix_table();
    auto old_mat = old_table->get_matrix();

    if (old_table->col_map.has_value()) {
      // not implemented
      assert(0);
    } else {
      new_table->col_map =
          dep_results[1]->down_to_flat_table()->get_col_diskarray(key_idx[1]);
    }

    if (old_table->col_map.has_value() &&
        old_table->col_hash_map.has_value() == false) {
      old_table->set_col_hash_map();
    }

    auto new_mat = make_shared<DenseVarStackMatrix>(
        old_mat->get_row_count(), new_table->col_map.value()->size(),
        old_mat->get_stack_row_count());

#pragma omp parallel for
    for (size_t ci = 0; ci < new_table->col_map.value()->size(); ci++) {
      for (size_t si = 0; si < new_mat->get_stack_count(); si++) {
        size_t old_ci = new_table->col_map.value()->as_ptr<VID>()[ci];
        if (old_table->col_hash_map.has_value()) {
          old_ci = old_table->col_hash_map.value().at(old_ci);
        }
        new_mat->or_stack_col_from(si, ci, *old_mat, old_ci);
      }
    }

    new_table->set_matrix(std::move(new_mat));

    new_table->row_map = old_table->row_map;
    new_table->row_hash_map = old_table->row_hash_map;

    // SPDLOG_WARN("new table col map {}", new_table->col_map.value()->size());
    // for (size_t i = 0; i < new_table->col_map.value()->size(); i++) {
    //   SPDLOG_WARN("new table col map {} {}", i,
    //               new_table->col_map.value()->as_ptr<VID>()[i]);
    // }

    break;
  }
  case TableType::ColMainMatrix: {
    assert(key_idx[0] == 1);
    assert(0);
    break;
  }
  default: {
    assert(0);
  }
  }
}

TEST(Operator, TableConvertVRtoCM) {
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op = unique_ptr<BigOperator>(
      new TableConvert(TableType::ColMainMatrix, false));

  shared_ptr<DenseVarStackMatrix> matc;
  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"b", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new VarRowMatrixTable(data->results_schema));

    auto mat = make_shared<DenseVarStackMatrix>(1000, 10000, 512);

    for (size_t i = 0; i < mat->get_row_count(); i++) {
      for (size_t j = 0; j < mat->get_col_count(); j++) {
        mat->set(i, j, random_bool(g.gen));
      }
    }

    matc = mat;
    table->down_to_var_row_matrix_table()->set_matrix(std::move(mat));

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  op->finishAddingDependency();
  op->run();

  op->get_merged_prof().report();

  auto matr = op->results->down_to_col_main_matrix_table()->get_matrix();

  for (size_t i = 0; i < matc->get_row_count(); i++) {
    for (size_t j = 0; j < matc->get_col_count(); j++) {
      assert(matc->get(i, j) == matr->get(i, j));
    }
  }
}

TEST(Operator, TableConvertVRtoCM_T) {
  Graph g("/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100");
  g.load();

  auto op =
      unique_ptr<BigOperator>(new TableConvert(TableType::ColMainMatrix, true));

  shared_ptr<DenseVarStackMatrix> matc;
  {
    auto data = unique_ptr<BigOperator>(new InlinedData);
    data->results_schema.columns = {{"b", Type::VID}, {"k", Type::VID}};
    auto table = unique_ptr<Table>(new VarRowMatrixTable(data->results_schema));

    auto mat = make_shared<DenseVarStackMatrix>(1000, 10000, 512);

    for (size_t i = 0; i < mat->get_row_count(); i++) {
      for (size_t j = 0; j < mat->get_col_count(); j++) {
        mat->set(i, j, random_bool(g.gen));
      }
    }

    matc = mat;
    table->down_to_var_row_matrix_table()->set_matrix(std::move(mat));

    data->results = std::move(table);
    op->addDependency(std::move(data));
  }

  op->finishAddingDependency();
  op->run();

  op->get_merged_prof().report();

  auto matr = op->results->down_to_col_main_matrix_table()->get_matrix();

  for (size_t i = 0; i < matc->get_row_count(); i++) {
    for (size_t j = 0; j < matc->get_col_count(); j++) {
      assert(matc->get(i, j) == matr->get(j, i));
    }
  }
}