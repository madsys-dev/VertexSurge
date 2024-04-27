#pragma once

#include "common.hh"
#include "peano.hh"
#include "tbb/parallel_sort.h"
#include <random>
#include <type_traits>

template <class T> std::decay_t<T> decay_copy(T &&);

template <typename E>
concept Entity = requires(E e) {
  { decay_copy(e.TYPE_NAME) } -> std::same_as<const char *>;
};

template <typename V>
concept Vertex = requires(V v) {
  requires Entity<V>;
  { decay_copy(v.id) } -> std::same_as<VID>;
};

template <typename V>
concept VertexWithRawID = Vertex<V> && requires(V v) {
  typename V::RawID;
  { v.raw_id } -> std::convertible_to<decltype(v.raw_id)>;
};

template <typename E>
concept Edge = requires(E e) {
  requires Entity<E>;

  typename E::SRC;
  requires Vertex<typename E::SRC>;

  typename E::DST;
  requires Vertex<typename E::DST>;

  { decay_copy(e.src_id) } -> std::same_as<VID>;
  { decay_copy(e.dst_id) } -> std::same_as<VID>;
};

template <typename E>
concept EdgeWithRawID = Edge<E> && requires(E e) {
  typename E::SRC;
  requires VertexWithRawID<typename E::SRC>;

  typename E::DST;
  requires VertexWithRawID<typename E::DST>;

  { e.raw_src_id } -> std::convertible_to<decltype(e.raw_src_id)>;
  { e.raw_dst_id } -> std::convertible_to<decltype(e.raw_dst_id)>;
};

template <Edge E> std::string edge_forward_simple_name() {
  return fmt::format("{}_{}_{}", E::SRC::TYPE_NAME, E::TYPE_NAME,
                     E::DST::TYPE_NAME);
}

template <Edge E> std::string edge_backwark_simple_name() {
  return fmt::format("{}_{}_{}", E::DST::TYPE_NAME, E::TYPE_NAME,
                     E::SRC::TYPE_NAME);
}

template <Vertex V> bool vertex_ord(const V &a, const V &b) {
  return a.id < b.id;
}

template <Edge E> bool edge_ord(const E &a, const E &b) {
  if (a.src_id == b.src_id)
    return a.dst_id < b.dst_id;
  else
    return a.src_id < b.src_id;
}

template <Edge E> bool edge_dst_ord(const E &a, const E &b) {
  if (a.dst_id == b.dst_id)
    return a.src_id < b.src_id;
  else
    return a.dst_id < b.dst_id;
}

template <Edge E> bool edge_eq(const E &a, const E &b) {
  return a.src_id == b.src_id && a.dst_id == b.dst_id;
}

template <Edge E> void sort_edges_by_src_dst(E *edge, size_t edge_count) {
  tbb::parallel_sort(edge, edge + edge_count, edge_ord<E>);
}

template <Edge E> void sort_edges_by_dst_src(E *edge, size_t edge_count) {
  tbb::parallel_sort(edge, edge + edge_count, edge_dst_ord<E>);
}

template <Edge E> void random_shuffle_edges(E *edge, size_t edge_count) {
  // std::random_device rd;
  std::mt19937 g(1234);
  std::shuffle(edge, edge + edge_count, g);
}

template <Edge E> void add_reverse_edges(std::vector<E> &edges) {
  size_t len = edges.size();
  for (size_t i = 0; i < len; i++) {
    E new_edge = edges[i];
    std::swap(new_edge.src_id, new_edge.dst_id);
    edges.push_back(new_edge);
  }
}

template <Edge E> void deduplicate_edges(std::vector<E> &edges) {
  auto last = unique(edges.begin(), edges.end(), edge_eq<E>);
  edges.erase(last, edges.end());
}

template <Edge E>
std::vector<Offset> sort_and_generate_index_from_src(size_t source_vertex_count,
                                                     E *edges,
                                                     size_t edge_count) {
  sort_edges_by_src_dst(edges, edge_count);

  VID expected = 0;
  std::vector<Offset> index;
  for (size_t i = 0; i < edge_count; i++) {
    const auto &edge = edges[i];
    if (edge.src_id == expected) {
      index.push_back(static_cast<int64_t>(i));
    } else if (edge.src_id > expected) {
      for (VID j = expected; j <= edge.src_id; j++) {
        index.push_back(static_cast<int64_t>(i));
      }
    }
    expected = edge.src_id + 1;
  }
  for (auto j = expected; j <= source_vertex_count; j++) {
    index.push_back(static_cast<int64_t>(edge_count));
  }
  return index;
}

template <Edge E>
std::vector<Offset> sort_and_generate_index_from_dst(size_t dst_vertex_count,
                                                     E *edges,
                                                     size_t edge_count) {
  sort_edges_by_dst_src(edges, edge_count);

  VID expected = 0;
  std::vector<Offset> index;
  for (size_t i = 0; i < edge_count; i++) {
    const auto &edge = edges[i];
    if (edge.dst_id == expected) {
      index.push_back(static_cast<int64_t>(i));
    } else if (edge.dst_id > expected) {
      for (VID j = expected; j <= edge.dst_id; j++) {
        index.push_back(static_cast<int64_t>(i));
      }
    }
    expected = edge.dst_id + 1;
  }
  for (auto j = expected; j <= dst_vertex_count; j++) {
    index.push_back(static_cast<int64_t>(edge_count));
  }
  return index;
}

template <Edge E>
void sort_edge_in_peano2(size_t src_vertex_count, size_t dst_vertex_count,
                         E *edges, size_t edge_count) {
  size_t n = std::max(src_vertex_count, dst_vertex_count);
  n = round_to_2_power(n);

  tbb::parallel_sort(edges, edges + edge_count, [n](const E &a, const E &b) {
    return XYToIndex(n, a.src_id, a.dst_id, PeanoDirection::RA) <
           XYToIndex(n, b.src_id, b.dst_id, PeanoDirection::RA);
  });
}
