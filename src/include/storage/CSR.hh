#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <omp.h>
#include <sys/types.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common.hh"
#include "storage/bitmap.hh"
#include "storage/disk_array.hh"
#include "tbb/concurrent_hash_map.h"

struct PosLen {
  size_t pos;
  size_t len;
};

class BufferWriter {
  const static size_t DefaultBufferSize = 8192;

  std::filesystem::path path;
  std::ofstream file;

  std::unique_ptr<char[]> buf;
  size_t buf_size;
  size_t buf_idx;

public:
  BufferWriter(const BufferWriter &) = delete;
  BufferWriter(BufferWriter &&) = default;

  BufferWriter(size_t buffer_size = DefaultBufferSize);
  BufferWriter(std::filesystem::path path,
               size_t buffer_size = DefaultBufferSize);
  ~BufferWriter();

  void open(std::filesystem::path path, bool is_append = false);
  void close();

  void write(char *data, size_t len);
  void flush();
};

class LayerCSR {
  std::filesystem::path dir;
  size_t vertex_count, layer_count;
  DiskArray<VID> v;
  std::vector<DiskArray<PosLen>> idxs;
  std::vector<DiskArray<char>> layers;

public:
  LayerCSR(std::filesystem::path);

  inline size_t get_vertex_count() { return vertex_count; }
  inline size_t get_layer_count() { return layer_count; }
  inline VID *get_v() { return &v[0]; }
  inline VID get_v_at(VID v_idx) { return v[v_idx]; }

  template <typename T> void push_data(VID v_idx, size_t at_layer, T &data);
  template <typename T> T get_data(VID v_idx, size_t at_layer);
  template <typename T> void init_first_layer();

  void new_layer();
  void append_raw_data(VID v_idx, size_t layer_idx,
                       std::pair<char *, size_t> data);
  void append_raw_data_atomic(VID v_idx, size_t layer_idx,
                              std::pair<char *, size_t> data);

  void set_v(VID *v, size_t vertex_count);
  std::pair<char *, size_t> get_raw_data(VID v_idx, size_t at_layer);

  PosLen get_poslen(VID vidx, size_t at_layer);

  // The caller must ensure VID are contained
  tbb::concurrent_hash_map<VID, size_t> get_frequency();
  std::vector<std::vector<std::vector<VID>>>
  get_layer_frequency_detailed(VID *lm, size_t lmc); // layer,landmark,data
};

struct TidLenPos {
  uint32_t tid;
  uint32_t len;
  uint64_t pos;
};

static_assert(sizeof(TidLenPos) == 16);

// Multi-Thread CSR, single layer
struct MTCSR {
  size_t thread_count;
  size_t vertex_count;
  DiskArray<TidLenPos> idx;

  // Support Concurrent Write and Read
  std::vector<DiskArray<VID>> data;

  MTCSR(size_t vertex_count, size_t thread_count);

  // MTCSR create files directly under dir distincted by layer_idx,
  // "data-{layer_idx}.bin" and "idx-{layer_idx}.bin".;
  MTCSR(std::filesystem::path dir, size_t layer_idx, size_t vertex_count,
        size_t thread_count, bool overwrite);

  inline size_t get_vertex_count() { return vertex_count; }
  inline TidLenPos get_idx(VID v_idx) { return idx[v_idx]; }
  VIDArrayRef get_data(VID v_idx);
  size_t count();

  void push_whole_list(VID v_idx, VIDArrayRef data);
  struct DataPusher {
    TidLenPos &idx;
    MTCSR &csr;
    DataPusher(VID v_idx, MTCSR &csr, size_t tid = -1);
    void push(VID data);
  };
  DataPusher get_data_pusher(VID v_idx, size_t tid = -1);
};

class LayerMultiThreadCSR {
  std::filesystem::path dir;
  size_t vertex_count, layer_count, thread_count;
  DiskArray<VID> v;
  std::vector<DiskArray<TidLenPos>> idxs;
  std::vector<std::vector<DiskArray<VID>>> layers;

public:
  struct DataPusher {
    VID v_idx;
    size_t at_layer;
    LayerMultiThreadCSR &csr;
    TidLenPos &idx;

    DataPusher(size_t tid, VID v_idx, size_t at_layer,
               LayerMultiThreadCSR &csr);

    void push(VID data);
  };

  LayerMultiThreadCSR(std::filesystem::path);

  inline size_t get_vertex_count() { return vertex_count; }
  inline size_t get_layer_count() { return layer_count; }
  inline VID *get_v() { return &v[0]; }
  inline TidLenPos get_idx(VID vidx, size_t at_layer) {
    return idxs[at_layer][vidx];
  }

  void set_v(VID *v, size_t vertex_count);
  void init_first_layer();
  void new_layer();

  void push_data(size_t tid, VID v_idx, size_t at_layer, VIDArrayRef data);
  DataPusher get_data_pusher(size_t tid, VID v_idx, size_t at_layer);

  VIDArrayRef get_data(VID v_idx, size_t at_layer);
};

class MultiVisit {
  std::filesystem::path dir;

  size_t vertex_count, visit_bit_length;
  VID *v;
  std::vector<MmapBitmap> visit_bitmap;

public:
  MultiVisit(std::filesystem::path dir);

  inline size_t get_vertex_count() { return vertex_count; }
  inline size_t get_visit_bit_length() { return visit_bit_length; }

  void set_v(VID *v, size_t vertex_count, size_t visit_bit_length);
  VID get_v_at(size_t at);
  inline VID *get_v() { return v; }

  void clone_from(MultiVisit &other);

  MmapBitmap &get_bitmap_visit(VID v_idx);

  std::vector<size_t> source_visit_count();

  std::vector<size_t> target_visit_count();
};

class AppendOnlyMultiVertexWriter {
  std::filesystem::path dir;
  std::vector<VID> v;
  std::vector<BufferWriter> buffer;

public:
  AppendOnlyMultiVertexWriter(const AppendOnlyMultiVertexWriter &) = delete;
  AppendOnlyMultiVertexWriter(AppendOnlyMultiVertexWriter &&) = default;

  AppendOnlyMultiVertexWriter(std::filesystem::path);
  ~AppendOnlyMultiVertexWriter();

  void set_v(VID *v, size_t vertex_count);
  void append_data(VID v, std::pair<char *, size_t> data);
};

