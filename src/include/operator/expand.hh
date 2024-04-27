#pragma once

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <omp.h>
#include <string>
#include <sys/stat.h>
#include <unordered_set>
#include <variant>
#include <vector>

#include "common.hh"
#include "peano.hh"
#include "physical_operator.hh"
#include "storage/CSR.hh"
#include "storage/bitmap.hh"
#include "storage/disk_array.hh"

enum class ExpandPathType {
  Any,          // AnyPath with length in [k_min(optional),k_max(must)].
  Shortest,     // AnyShortest with length in [k_min(optional),k_max(optional)],
  FindShortest, // AnyShortest with length in [k_min(optional),k_max(optional)],
};

struct VExpandPathParameters {
  // TODO: At merge of VExpand, store distance infomation
  // Maybe for bit matrix, we should store multiple bit matrix at different
  // distance Or export bit matrix to flat table before expanding each step
  ExpandPathType path_type;

  static const size_t K_MAX_FIND_SHORTEST = 50;

  bool with_distance = false;
  bool distinct_target = false;

  static VExpandPathParameters Any();
  static VExpandPathParameters Shortest();
  static VExpandPathParameters FindShortest();

  inline VExpandPathParameters set_with_distance() {
    with_distance = true;
    return *this;
  }
};

enum class ExpandStrategy {
  Adaptive,
  CSR,
  Dense,
};

enum class ExpandDenseStrategy {

  Hilbert,      // Edge list is sorted by peano order
  BlockHilbert, // Edges are partitioned by target *** The final part ***

  // 1D partition on base & out matrix, D=col
  Block1D,    // Parallel in block,
  Implicit1D, // Parallel on edge list,

  // 2D partition on base & out matrix, 2D=(row,col)
  // So the edgelist will be read multiple times
  Block2D,    // Parallel in block,
  Implicit2D, // Parallel on edge list

  // for evaluation
  Naive, // No partition, No SIMD
  SIMD,  // No partition

};

// impl lib fmt for ExpandDenseStrategy
template <> struct fmt::formatter<ExpandDenseStrategy> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(ExpandDenseStrategy strategy, FormatContext &ctx) {
    switch (strategy) {
    case ExpandDenseStrategy::Hilbert:
      return format_to(ctx.out(), "Hilbert");
    case ExpandDenseStrategy::BlockHilbert:
      return format_to(ctx.out(), "BlockHilbert");
    case ExpandDenseStrategy::Block1D:
      return format_to(ctx.out(), "Block1D");
    case ExpandDenseStrategy::Implicit1D:
      return format_to(ctx.out(), "Implicit1D");
    case ExpandDenseStrategy::Block2D:
      return format_to(ctx.out(), "Block2D");
    case ExpandDenseStrategy::Implicit2D:
      return format_to(ctx.out(), "Implicit2D");
    case ExpandDenseStrategy::Naive:
      return format_to(ctx.out(), "Naive");
    case ExpandDenseStrategy::SIMD:
      return format_to(ctx.out(), "SIMD");
    default:
      return format_to(ctx.out(), "Unknown");
    }
  }
};

class VExpand;
// Expand Once
template <typename M>
requires BitMatrix<M>
class Expand;

template <typename M>
requires BitMatrix<M>
using LayerData =
    std::variant<std::shared_ptr<MTCSR>, std::shared_ptr<M>, bool>;

template <typename M>
requires BitMatrix<M>
using LayerDataPtr = std::variant<MTCSR *, M *>;

template <typename M>
requires BitMatrix<M>
class ExpandStatus {

  std::filesystem::path dir;
  size_t thread_count;

  VIDArrayRef v;

  // DiskArray<VID> v;
  std::vector<LayerData<M>> layers;

  bool use_sparse_visit;

  std::shared_ptr<M> visit;

#ifdef OCC_ARRAY
  std::unique_ptr<DiskArray<std::atomic_uint8_t>> cc_array;
#endif
#ifdef LOCK_BASED
  std::unique_ptr<std::mutex[]> lock_array;
#endif

  std::vector<std::unordered_set<VID>> sparse_visit;

  size_t target_count, edge_count;

  ExpandPathType path_type;
  ExpandStrategy strategy;

public:
  size_t out_layer_idx;
  // for DenseVarStackMatrix
  size_t stack_size;

  size_t visit_count;

  ExpandStatus(ExpandPathType path_type, ExpandStrategy strategy);

  // ExpandStatus(std::filesystem::path dir);
  // ExpandStatus(std::filesystem::path dir, VIDArrayRef v, AnalyticGraph
  // *graph);

  void set_start_vertex_and_possible_matrix(VIDArrayRef v, size_t target_count,
                                            size_t edge_count);

  inline size_t get_layer_count() { return layers.size(); }
  inline size_t get_vertex_count() { return v.second; }

  // 0: MTCSR, 1: Matrix
  inline size_t get_type_of_layer(size_t layer_idx) {
    return layers[layer_idx].index();
  }

  void init_first_layer();
  void init_first_csr_layer();
  void init_first_m_layer();
  void new_csr_layer_at(size_t index);
  void new_csr_layer();
  void new_m_layer();
  void resize_layer(size_t to_at_least);
  void new_m_layer_at(size_t index);

  void init_multiple_m_layer(size_t n) {
    for (size_t i = 0; i < n; i++) {
      get_layer_m(i);
    }
  }

  void load_all_multi_m_layer_and_visit() {
    for (size_t i = 0; i < layers.size(); i++) {
      auto &m = get_layer_m(i);
      m.load_to_memory();
    }
    visit->load_to_memory();
  }

  inline std::shared_ptr<M> get_layer_m_ptr(size_t layer_idx) {
    resize_layer(layer_idx + 1);
    if (layers[layer_idx].index() == 2) {
      new_m_layer_at(layer_idx);
    }
    return std::get<std::shared_ptr<M>>(layers[layer_idx]);
  }

  inline std::shared_ptr<MTCSR> get_layer_csr_ptr(size_t layer_idx) {
    resize_layer(layer_idx + 1);
    if (layers[layer_idx].index() == 2) {
      new_csr_layer_at(layer_idx);
    }
    return std::get<std::shared_ptr<MTCSR>>(layers[layer_idx]);
  }

  inline MTCSR &get_layer_csr(size_t layer_idx) {
    return *get_layer_csr_ptr(layer_idx);
  }

  inline M &get_layer_m(size_t layer_idx) {
    return *get_layer_m_ptr(layer_idx);
  }

  inline M &get_visit() { return *visit.get(); }
  inline std::vector<std::unordered_set<VID>> &get_sparse_visit() {
    return sparse_visit;
  }

  size_t get_count_of(size_t layer_idx);

  friend Expand<M>;
  friend VExpand;
};

template <typename M>
requires BitMatrix<M>
class Expand : public PhysicalOperator {
public:
  const static size_t CONVERT_THRESHOLD = 0;
  ExpandStatus<M> &status;

  std::string desc;

  ExpandEdgeParams edge_params;

  // TypeName from_vertex, to_vertex, edge_type;
  // Direction dir;

  ExpandPathType type;
  // Derived
  bool is_from_src; // is from src in edge schema?

  virtual void run_internal() override;
  inline virtual std::string short_description() override { return desc; }

  void expand_csr();
  void expand_dense();

  void csr_to_dense();
  void dense_to_csr();

  // public:
  ExpandStrategy strategy;
  ExpandDenseStrategy dense_strategy;

  Expand(std::string desc, ExpandStatus<M> &es,
         std::vector<ExpandEdgeParam> edge_params, ExpandPathType type,
         ExpandStrategy strategy, ExpandDenseStrategy dense_strategy);

  Expand(std::string desc, ExpandStatus<M> &es, TypeName from_vertex,
         TypeName to_vertex, TypeName edge_type, Direction dir,
         ExpandPathType type, ExpandStrategy strategy,
         ExpandDenseStrategy dense_strategy);

  void set_edges(std::string edge_name);

  friend ExpandStatus<M>;
};

// impl libfmt for ExpandPathType
template <> struct fmt::formatter<ExpandPathType> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(ExpandPathType path_type, FormatContext &ctx) {
    switch (path_type) {
    case ExpandPathType::Any:
      return format_to(ctx.out(), "Any");
    case ExpandPathType::Shortest:
      return format_to(ctx.out(), "Shortest");
    case ExpandPathType::FindShortest:
      return format_to(ctx.out(), "FindShortest");
    default:
      return format_to(ctx.out(), "Unknown");
    }
  }
};

template <> struct fmt::formatter<VExpandPathParameters> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(VExpandPathParameters path_params, FormatContext &ctx) {
    return format_to(ctx.out(),
                     "VExpandPathParameters{{path_type: {}, with_distance: {}, "
                     "distinct_target: {}}}",
                     path_params.path_type, path_params.with_distance,
                     path_params.distinct_target);
  }
};

// impl libfmt for ExpandStrategy
template <> struct fmt::formatter<ExpandStrategy> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(ExpandStrategy strategy, FormatContext &ctx) {
    switch (strategy) {
    case ExpandStrategy::Adaptive:
      return format_to(ctx.out(), "Adaptive");
    case ExpandStrategy::CSR:
      return format_to(ctx.out(), "CSR");
    case ExpandStrategy::Dense:
      return format_to(ctx.out(), "Dense");
    default:
      return format_to(ctx.out(), "Unknown");
    }
  }
};

// impl libfmt for EdgeParams
