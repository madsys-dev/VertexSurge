
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <omp.h>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unordered_set>
#include <variant>

#include "common.hh"
#include "operator/expand_impl.hh"
#include "peano.hh"
#include "storage/CSR.hh"
#include "storage/bitmap.hh"
#include "utils.hh"

using namespace std;

VExpandPathParameters VExpandPathParameters::Any() {
  VExpandPathParameters p;
  p.path_type = ExpandPathType::Any;
  return p;
}

VExpandPathParameters VExpandPathParameters::Shortest() {
  VExpandPathParameters p;
  p.path_type = ExpandPathType::Shortest;
  return p;
}

VExpandPathParameters VExpandPathParameters::FindShortest() {
  VExpandPathParameters p;
  p.path_type = ExpandPathType::FindShortest;
  p.with_distance = true;
  return p;
}

template <typename M>
requires BitMatrix<M>
void Expand<M>::expand_csr() {
  status.new_csr_layer();
  MTCSR &base = status.get_layer_csr(status.get_layer_count() - 2);
  MTCSR &out = status.get_layer_csr(status.get_layer_count() - 1);

  Timer t("OnlyExpand");
  auto edge_refs =
      graph_store->get_edges_refs(edge_params, EdgesRefRequest::CSRRequest());
  if (type == ExpandPathType::Shortest) {

#pragma omp parallel for
    for (size_t r = 0; r < status.get_vertex_count(); r++) {
      auto pusher = out.get_data_pusher(r);
      auto [data, data_len] = base.get_data(r);
      for (size_t j = 0; j < data_len; j++) {
        VID s = data[j];

        // SPDLOG_INFO("Edge Count for {} is
        // {}",s,edge_ref.index()[s+1]-edge_ref.index()[s]); size_t count = 0;
        for (const auto &edge_ref : edge_refs) {
          for (Offset ki = edge_ref.index()[s]; ki < edge_ref.index()[s + 1];
               ki++) {
            VID x = edge_ref.csr_data()[ki];

            // Check visit
            if (status.use_sparse_visit) {
              if (status.get_sparse_visit()[r].contains(x) == false) {
                pusher.push(x);
                // SPDLOG_INFO("destination of {} push {}", s, x);
                // count ++;
                status.get_sparse_visit()[r].insert(x);
              } else {
                // SPDLOG_INFO("duplicated {} of {} at {}",x,s,ki);
              }
            } else {
              if (status.get_visit().get_atomic(r, x) ==
                  false) { // do not need atomic here, since r can only be
                           // executed in one thread
                pusher.push(x);
                // SPDLOG_INFO("destination of {} push {}", s, x);
                // count ++;
                status.get_visit().set_atomic(r, x, true);
              }
            }
            x++;
          }

          // SPDLOG_INFO("Push {} for {}",count,s);
        }
      }
    }
    status.visit_count = 0;
    if (status.use_sparse_visit) {
      for (const auto &s : status.get_sparse_visit()) {
        status.visit_count += s.size();
      }
    } else {
      status.visit_count = status.get_visit().count();
    }

  } else if (type == ExpandPathType::Any) {

#pragma omp parallel for
    for (size_t r = 0; r < status.get_vertex_count(); r++) {
      auto pusher = out.get_data_pusher(r);
      auto [data, data_len] = base.get_data(r);
      for (size_t j = 0; j < data_len; j++) {
        VID s = data[j];
        for (const auto &edge_ref :
             edge_refs) { // Edge Ref Here, since one start vertex can only be
                          // extended once in on MTCSR
          for (Offset ki = edge_ref.index()[s]; ki < edge_ref.index()[s + 1];
               ki++) {
            VID x = edge_ref.csr_data()[ki];
            pusher.push(x);
            // SPDLOG_INFO("{} To {}", s, x);
          }
        }
      }
    }
  } else {
    assert(0);
  }
#ifdef PRINT_EXPAND_COUNT
  SPDLOG_INFO("Pair Count {}", out.count());
#endif
}

template <typename M>
requires BitMatrix<M>
void Expand<M>::csr_to_dense() {
  SPDLOG_INFO("Convert CSR to DenseMatrix({})", typeid(M).name());

  MTCSR csr = std::move(status.get_layer_csr(status.get_layer_count() - 1));

  status.layers.pop_back();
  status.new_m_layer();
  from_csr_to_M2(csr, *std::get<std::shared_ptr<M>>(status.layers.back()));
}

template <typename M>
requires BitMatrix<M>
void Expand<M>::dense_to_csr() {
  SPDLOG_INFO("Convert DenseMatrix({}) to CSR", typeid(M).name());
  M m = std::move(status.get_layer_m(status.get_layer_count() - 1));

  status.layers.pop_back();
  status.new_csr_layer();
  from_M_to_csr2(m, *std::get<std::shared_ptr<MTCSR>>(status.layers.back()));
}

template <BitMatrix T, typename F>
void expand_dense_wrapper(Expand<T> &e, F fn) {
  // e.status.new_m_layer();
  e.status.out_layer_idx += 1;
  T &base = e.status.get_layer_m(e.status.out_layer_idx - 1);
  T &out = e.status.get_layer_m(e.status.out_layer_idx);

  switch (e.dense_strategy) {
  case ExpandDenseStrategy::Naive: {
    fn(base, out);
    break;
  }
  default: {
    assert(0);
  }
  }

  switch (e.type) {
  case ExpandPathType::Any: {
    break;
  }
  case ExpandPathType::Shortest: {
    // Dense must use visit matrix
    Timer t;
    t.start();
    e.status.visit_count =
        T::dedup_and_update_visit_and_count(e.status.get_visit(), out);
    t.printElapsedMilliseconds();
    break;
  }
  default: {
    assert(0);
  }
  }
}

template <> void Expand<DenseColMainMatrix>::expand_dense() {
  expand_dense_wrapper(
      *this, [this](DenseColMainMatrix &base, DenseColMainMatrix &out) {
        Timer t("OnlyExpand");
        auto edges_refs = graph_store->get_edges_refs(
            edge_params, EdgesRefRequest::FlatRequest(FlatOrder::SrcDst, true));
#pragma omp parallel for
        for (const auto &edges_ref : edges_refs) {
          VIDPair *data = edges_ref.flat.start;
          size_t datalen = edges_ref.flat.len;
          for (size_t i = 0; i < datalen; i++) {
            auto [u, v] = data[i];
            out.or_col_from(v, base, u);
          }
        }
      });
}

template <> void Expand<DenseVarStackMatrix>::expand_dense() {
  // status.new_m_layer();

  status.out_layer_idx += 1;
  DenseVarStackMatrix &base = status.get_layer_m(status.out_layer_idx - 1);
  DenseVarStackMatrix &out = status.get_layer_m(status.out_layer_idx);

  switch (dense_strategy) {
  case ExpandDenseStrategy::Hilbert: {
    Timer t("OnlyExpand");
    auto edges_refs = graph_store->get_edges_refs(
        edge_params, EdgesRefRequest::FlatRequest(FlatOrder::Hilbert, false));
    SPDLOG_INFO("EdgeRefs Count {}", edges_refs.size());

#ifdef OCC_ARRAY
    status.cc_array = unique_ptr<DiskArray<std::atomic_uint8_t>>(
        new DiskArray<std::atomic_uint8_t>(base.get_v512_count()));
#endif

#ifdef LOCK_BASED
    status.lock_array = unique_ptr<mutex[]>(new mutex[base.get_v512_count()]);
#endif

    for (const auto &edges_ref : edges_refs) {
      VIDPair *data = edges_ref.flat.start;
      size_t datalen = edges_ref.flat.len;

      for (size_t si = 0; si < base.get_stack_count(); si++) {
#pragma omp parallel for
        for (size_t i = 0; i < datalen; i++) {
          auto [u, v] = data[i];
#ifdef NO_CC
          out.or_stack_col_from(si, v, base, u);
#endif
#ifdef OCC_ARRAY

          out.atomic_or_stack_col_from(si, v, base, u,
                                       status.cc_array.get()->start());
#endif

#ifdef ATOMIC_CC
          out.atomic_v_or_stack_col_from(si, v, base, u);
#endif
#ifdef LOCK_BASED

          out.atomic_or_lock_stack_col_from(si, v, base, u,
                                            status.lock_array.get());
#endif
        }
      }
    }
    break;
  }
  case ExpandDenseStrategy::BlockHilbert: {
    Timer t("OnlyExpand");
    auto edges_refs = graph_store->get_edges_refs(
        edge_params, EdgesRefRequest::FlatRequest(FlatOrder::Hilbert, true));
    // SPDLOG_WARN("{}",dir);

    // SPDLOG_INFO("EdgeRefs Count {}", edges_refs.size());
#pragma omp parallel for schedule(guided)
    for (const auto &edges_ref : edges_refs) {
      // SPDLOG_INFO("edge count {}", edges_ref.flat.len);
      VIDPair *data = edges_ref.flat.start;
      size_t datalen = edges_ref.flat.len;

      // for(size_t i=0;i<datalen;i++){
      //   if(data[i].first==38||data[i].second==38)
      //     SPDLOG_WARN("{} {}",data[i].first,data[i].second);
      // }

      for (size_t si = 0; si < base.get_stack_count(); si++) {

#ifdef WITH_PREFETCH

#ifdef BLOCK_PREFETCH
        for (size_t i = 0; i < datalen; i += BLOCK_PREFETCH_COUNT) {
          size_t end = min(datalen, i + BLOCK_PREFETCH_COUNT);
          out.prefetch_or_stack_cols_from(si, base, data + i, end - i);
        }
#endif

#ifdef PRE_PREFETCH
        out.pre_prefetch(si, base, data);
        for (size_t i = 0; i < datalen; i++) {
          out.or_stack_col_from_with_prefetch(si, base, data, i);
        }

#endif

#else

        for (size_t i = 0; i < datalen; i++) {
          auto [u, v] = data[i];
          out.or_stack_col_from(si, v, base, u);
        }
#endif
      }
    }

    break;
  }
  default: {
    assert(0);
  }
  };

  switch (type) {
  case ExpandPathType::Any: {
#ifdef PRINT_EXPAND_COUNT
    SPDLOG_INFO("Pair Count {}", out.count());
#endif

    break;
  }
  case ExpandPathType::FindShortest:
    [[fallthrough]];
  case ExpandPathType::Shortest: {
    // Dense must use visit matrix
#ifdef PRINT_EXPAND_COUNT
    SPDLOG_INFO("Pair Count {}", out.count());
#endif
    Timer t("dedup_and_update_visit_and_count");
    status.visit_count = DenseVarStackMatrix::dedup_and_update_visit_and_count(
        status.get_visit(), out);
    break;
  }
  default: {
    assert(0);
  }
  }

  // t.stop();
  // SPDLOG_DEBUG("Dedup and Update Visit Time {}", t.elapsedTime());
}

template <typename M>
requires BitMatrix<M>
void Expand<M>::run_internal() {
  switch (strategy) {
  case ExpandStrategy::Adaptive: {
    SPDLOG_DEBUG("Adaptive Expand from {}", status.get_layer_count() - 1);
    auto &last_layer = status.layers.back();
    auto layer_type = last_layer.index();
    switch (layer_type) {
    case 0: {
      // CSR
      auto csr = std::get<std::shared_ptr<MTCSR>>(last_layer);
      size_t count = status.visit_count;
      double p =
          1.0 * count / (status.target_count * status.get_vertex_count());
      SPDLOG_DEBUG("Visit={},Coverage={}", count, p);
      if (csr->count() >= CONVERT_THRESHOLD && p < 0.4) {
        csr_to_dense();
        expand_dense();
      } else {
        expand_csr();
      }
      break;
    }
    case 1: {
      // Matrix
      size_t count;
      auto m = std::get<std::shared_ptr<M>>(last_layer);
      if (type == ExpandPathType::Shortest) {
        count = status.visit_count;
      } else {
        count = m->count();
      }
      double p =
          1.0 * count / (status.target_count * status.get_vertex_count());
      SPDLOG_DEBUG("Visit={},Coverage={}", count, p);
      if (m->count() < CONVERT_THRESHOLD && p > 0.6) {
        dense_to_csr();
        expand_csr();
      } else {
        expand_dense();
      }

      break;
    }
    default: {
      assert(false);
    }
    }
    break;
  }
  case ExpandStrategy::CSR: {
    expand_csr();
    break;
  }
  case ExpandStrategy::Dense: {
    expand_dense();
    break;
  }

  default: {
    assert(false);
  }
  }
  SPDLOG_TRACE("Expand Finish");
  // auto &store_cost = get_prof().get_global_counter(
  //     PhysicalOperator::get_physical_operator_name(this) + ":StoreCost");
  // store_cost += status.get_count_of(status.get_layer_count() - 1);
}

size_t decide_k0(size_t start_count, size_t target_count, size_t edge_count) {
  return 1024;

  // return 2048;
  double k = start_count;
  double V = target_count, E = edge_count, L = CACHE_ON_USAGE;

  double k0;
  k0 = 4 * sqrt(2.0 / 3.0) * sqrt(E * L) / V;

  double k0_1, V0_1, T_1, k0_2, V0_2, T_2;
  double alignment = 512.0;

  k0_1 = floor(k0 / alignment) * alignment;
  V0_1 = L / (k0_1 / 8.0);
  T_1 = (ceil(V / V0_1) + 2) * align_to(k, k0_1) * V / 8.0 +
        ceil(k / k0_1) * 8 * E;

  k0_2 = ceil(k0 / alignment) * alignment;
  V0_2 = L / (k0_1 / 8.0);
  T_2 = (ceil(V / V0_2) + 2) * align_to(k, k0_2) * V / 8.0 +
        ceil(k / k0_2) * 8 * E;

  if (k0_1 == 0) {
    return round(k0_2);
  } else {

    if (T_1 < T_2) {
      return round(k0_1);
    } else {
      return round(k0_2);
    }
  }
}

// template class Expand<DenseStackMatrix>;
// template class ExpandStatus<DenseStackMatrix>;

template class Expand<DenseColMainMatrix>;
template class ExpandStatus<DenseColMainMatrix>;

template class Expand<DenseVarStackMatrix>;
template class ExpandStatus<DenseVarStackMatrix>;

TEST(Expand, VisitSetPerformance) {
  size_t v = 1e7;
  size_t vc = v / 1000;
  {
    mt19937 gen(1234);
    set<VID> s;
    uniform_int_distribution<size_t> dis(0, v - 1);
    Timer t;
    t.start();
    for (size_t i = 0; i < vc; i++) {
      VID x = dis(gen);
      s.insert(x);
    }
    SPDLOG_INFO("Set Insert Time {}, Throughput {} (op/ms), Memory {}B",
                t.elapsedTime(), vc / t.elapsedMs(),
                readable_number(s.size() * sizeof(VID)));
  }

  {
    mt19937 gen(1234);
    unordered_set<VID> s;
    uniform_int_distribution<size_t> dis(0, v - 1);
    Timer t;
    t.start();
    for (size_t i = 0; i < vc; i++) {
      VID x = dis(gen);
      s.insert(x);
    }
    SPDLOG_INFO(
        "Unorderd Set Insert Time {}, Throughput {} (op/ms), Memory {}B",
        t.elapsedTime(), vc / t.elapsedMs(),
        readable_number(double(s.size() * sizeof(VID)) / s.load_factor()));
  }

}
