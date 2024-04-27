#pragma once
#include "common.hh"

#include "expand.hh"

////////////////////////////Template
/// Definitions////////////////////////////////////////////

template <typename M>
requires BitMatrix<M>
void ExpandStatus<M>::new_m_layer() {
  SPDLOG_DEBUG("New Matrix layer, from {}", dir);
  size_t layer_count = layers.size();
  get_layer_m(layer_count);
}

template <typename M>
requires BitMatrix<M>
void ExpandStatus<M>::resize_layer(size_t to_at_least) {
  if (layers.size() < to_at_least) {
    // size_t formmer = layers.size();
    layers.resize(to_at_least, LayerData<M>(std::in_place_type<bool>, false));
    // for (size_t i = formmer; i < layers.size(); i++) {
    // layers[i] =
    // LayerData<M>(std::in_place_type<bool>, false);
    // }
  }
}

template <typename M>
requires BitMatrix<M>
void ExpandStatus<M>::new_m_layer_at(size_t idx) {
  resize_layer(idx + 1);
  layers[idx] = std::move(LayerData<M>{
      std::shared_ptr<M>(new M(get_vertex_count(), target_count))});
}

template <> inline void ExpandStatus<DenseVarStackMatrix>::new_m_layer() {
  // Timer t("new_m_layer");
  SPDLOG_INFO("New Matrix layer, from {}", dir);
  size_t layer_count = layers.size();
  get_layer_m(layer_count);
}

template <>
inline void ExpandStatus<DenseVarStackMatrix>::new_m_layer_at(size_t idx) {
  resize_layer(idx + 1);
  layers[idx] = LayerData<DenseVarStackMatrix>{
      std::shared_ptr<DenseVarStackMatrix>(new DenseVarStackMatrix(
          get_vertex_count(), target_count, stack_size))};
}

template <typename M>
requires BitMatrix<M>
void ExpandStatus<M>::new_csr_layer() {
  SPDLOG_DEBUG("New CSR layer, from {}", dir);
  [[maybe_unused]] size_t layer_count = layers.size();

  get_layer_csr(layer_count);
}

template <typename M>
requires BitMatrix<M>
void ExpandStatus<M>::new_csr_layer_at(size_t idx) {
  resize_layer(idx + 1);
  layers[idx] = std::move(
      LayerData<M>{std::shared_ptr<MTCSR>(new MTCSR(v.second, thread_count))});
}

template <typename M>
requires BitMatrix<M> size_t ExpandStatus<M>::get_count_of(size_t layer_idx) {
  auto &v = layers[layer_idx];
  switch (v.index()) {
  case 0: {
    return std::get<0>(v)->count();
  }
  case 1: {
    return std::get<1>(v)->count();
  }
  default: {
    assert(false);
  }
  }
}

template <typename M>
requires BitMatrix<M> ExpandStatus<M>::ExpandStatus(ExpandPathType path_type,
                                                    ExpandStrategy strategy)
    : thread_count(omp_get_max_threads()), path_type(path_type),
      strategy(strategy) {
  use_sparse_visit = false;
  if (strategy == ExpandStrategy::CSR) {
    use_sparse_visit = true;
  }
}

// template <typename M>
// requires BitMatrix<M> ExpandStatus<M>::ExpandStatus(std::filesystem::path
// dir)
//     : dir(dir), thread_count(omp_get_max_threads()) {
//   if (std::filesystem::exists(dir) == false) {
//     SPDLOG_TRACE("{} not exists, creating", dir);
//     std::filesystem::create_directory(dir);
//   }
// }

// template <typename M>
// requires BitMatrix<M> ExpandStatus<M>::ExpandStatus(std::filesystem::path
// dir,
//                                                     VIDArrayRef v,
//                                                     AnalyticGraph *graph)
//     : dir(dir), thread_count(omp_get_max_threads()) {
//   if (std::filesystem::exists(dir) == false) {
//     SPDLOG_TRACE("{} not exists, creating", dir);
//     std::filesystem::create_directory(dir);
//   }
//   set_v_graph(v, graph);
// }

template <typename M>
requires BitMatrix<M>
void ExpandStatus<M>::set_start_vertex_and_possible_matrix(VIDArrayRef, size_t,
                                                           size_t) {
  // This function assert false for default implementation
  // Please implement specification for BitMatrix
  assert(false);
}


size_t decide_k0(size_t start_count, size_t target_count, size_t edge_count);


template <>
inline void
ExpandStatus<DenseVarStackMatrix>::set_start_vertex_and_possible_matrix(
    VIDArrayRef v, size_t target_count, size_t edge_count) {
  this->target_count = target_count;
  this->edge_count = edge_count;

  auto [vd, vlen] = v;
  this->v = v;
  stack_size = decide_k0(this->v.second, target_count, edge_count);
  SPDLOG_INFO("VarStackMatrix StackRowCount k0 = {}, V0 = {}", stack_size,
              CACHE_ON_USAGE / stack_size * 4);

  switch (path_type) {
  case ExpandPathType::Any: {
    break;
  }

  case ExpandPathType::FindShortest:
    [[fallthrough]];
  case ExpandPathType::Shortest: {
    if (use_sparse_visit) {
      sparse_visit.resize(vlen);
    } else {
      SPDLOG_INFO("allocate visit");
      visit =
          std::make_shared<DenseVarStackMatrix>(vlen, target_count, stack_size);
    }
    break;
  }
  default: {
    assert(0);
  }
  }

  init_first_layer();
}

template <>
inline void
ExpandStatus<DenseColMainMatrix>::set_start_vertex_and_possible_matrix(
    VIDArrayRef v, size_t target_count, size_t edge_count) {
  this->target_count = target_count;
  this->edge_count = edge_count;

  auto [vd, vlen] = v;
  this->v = v;

  switch (path_type) {
  case ExpandPathType::Any: {
    break;
  }
  case ExpandPathType::Shortest: {
    if (use_sparse_visit) {
      sparse_visit.resize(vlen);
    } else {
      visit = std::make_shared<DenseColMainMatrix>(vlen, target_count);
    }
    break;
  }
  default: {
    assert(0);
  }
  }

  init_first_layer();
}

template <typename M>
requires BitMatrix<M>
void ExpandStatus<M>::init_first_layer() {
  switch (strategy) {
  case ExpandStrategy::Adaptive:
    [[fallthrough]];
  case ExpandStrategy::CSR: {
    init_first_csr_layer();
    break;
  }
  case ExpandStrategy::Dense: {
    init_first_m_layer();
    break;
  }
  default: {
    assert(0);
  }
  };
}

template <typename M>
requires BitMatrix<M>
void ExpandStatus<M>::init_first_csr_layer() {
  get_layer_csr(0);
  // new_csr_layer();
  std::shared_ptr<MTCSR> first_csr =
      std::get<std::shared_ptr<MTCSR>>(layers[0]);
  for (size_t i = 0; i < v.second; i++) {
    first_csr->push_whole_list(i, {&v.first[i], 1});
    switch (path_type) {
    case ExpandPathType::Any: {
      break;
    }
    case ExpandPathType::Shortest: {
      if (use_sparse_visit)
        sparse_visit[i].insert(v.first[i]);
      else
        visit->set(i, v.first[i], true);

      break;
    }
    default: {
      assert(0);
    }
    }
  }
  visit_count = v.second;
}

template <typename M>
requires BitMatrix<M>
void ExpandStatus<M>::init_first_m_layer() {
  get_layer_m(0);
  // new_m_layer();
  auto first_m = std::get<std::shared_ptr<M>>(layers[0]);
  for (size_t i = 0; i < v.second; i++) {
    first_m->set(i, v.first[i], true);
    switch (path_type) {
    case ExpandPathType::Any: {
      break;
    }
    case ExpandPathType::FindShortest:
      [[fallthrough]];
    case ExpandPathType::Shortest: {
      if (use_sparse_visit)
        sparse_visit[i].insert(v.first[i]);
      else
        visit->set(i, v.first[i], true);
      break;
    }
    default: {
      assert(0);
    }
    }
  }
  visit_count = v.second;
}

template <typename M>
requires BitMatrix<M>
Expand<M>::Expand(std::string desc, ExpandStatus<M> &es,
                  std::string from_vertex, std::string to_vertex,
                  std::string edge_type, Direction dir, ExpandPathType type,
                  ExpandStrategy strategy, ExpandDenseStrategy dense_strategy)
    : Expand<M>(desc, es,
                {ExpandEdgeParam(from_vertex, to_vertex, edge_type, dir)}, type,
                strategy, dense_strategy) {}

template <typename M>
requires BitMatrix<M>
Expand<M>::Expand(std::string desc, ExpandStatus<M> &es,
                  std::vector<ExpandEdgeParam> _edge_params,
                  ExpandPathType type, ExpandStrategy strategy,
                  ExpandDenseStrategy dense_strategy)
    : status(es), desc(desc), edge_params(_edge_params), type(type),
      is_from_src(true), strategy(strategy), dense_strategy(dense_strategy) {

  for (auto &p : edge_params) {
    p.es = graph_store->schema.get_edge_schema(
        fmt::format("{}_{}_{}", p.start_type, p.edge_type, p.target_type));
    if (p.dir == Direction::Undecide)
      p.dir = p.es.give_direction(p.start_type);
    p.es.check_direction(p.dir, p.start_type);
  }
}
