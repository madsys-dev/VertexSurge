#include "storage/CSR.hh"

#include "common.hh"
#include "storage/bitmap.hh"
#include "utils.hh"
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <omp.h>
#include <string>
#include <sys/types.h>
#include <tbb/concurrent_hash_map.h>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace std;

BufferWriter::BufferWriter(size_t buffer_size)
    : buf(make_unique<char[]>(buffer_size)), buf_size(buffer_size), buf_idx(0) {

}

BufferWriter::BufferWriter(std::filesystem::path path, size_t buffer_size)
    : path(path), file(path), buf(make_unique<char[]>(buffer_size)),
      buf_size(buffer_size), buf_idx(0) {}

void BufferWriter::open(std::filesystem::path path, bool is_append) {
  this->path = path;
  if (file.is_open()) {
    flush();
    file.close();
  }
  if (is_append) {
    file.open(path, ios::app);
  } else {
    file.open(path);
  }
}

void BufferWriter::close() {
  flush();
  file.close();
}

void BufferWriter::flush() {
  file.write(buf.get(), buf_idx);
  buf_idx = 0;
}

void BufferWriter::write(char *data, size_t len) {
  if (buf_idx + len > buf_size) {
    flush();
  }
  if (len > buf_size) {
    file.write(data, len);
    return;
  }
  memcpy(buf.get() + buf_idx, data, len);
  buf_idx += len;
}

BufferWriter::~BufferWriter() {
  flush();
  file.close();
}

LayerCSR::LayerCSR(std::filesystem::path dir) : dir(dir) {
  if (filesystem::exists(dir) == false) {
    assert(filesystem::create_directory(dir));
  }

  Config conf(dir / "config.txt");
  vertex_count = conf.get_or_init<size_t>("VertexCount", 0);
  layer_count = conf.get_or_init<size_t>("LayerCount", 0);
  if (vertex_count > 0) {
    v = DiskArray<VID>(dir / "vertex.bin", false);
    for (size_t i = 0; i < layer_count; i++) {
      idxs.push_back(
          DiskArray<PosLen>(dir / ("idxs" + to_string(i) + ".bin"), false));
      layers.push_back(
          DiskArray<char>(dir / ("data" + to_string(i) + ".bin"), false));
    }
  }
}

void LayerCSR::new_layer() {
  size_t now_layer_idx = get_layer_count();
  layer_count += 1;
  auto new_idx = DiskArray<PosLen>(
      dir / ("idxs" + to_string(now_layer_idx) + ".bin"), true);
  new_idx.resize(vertex_count, PosLen{static_cast<size_t>(-1), 0});
  idxs.push_back(std::move(new_idx));

  layers.push_back(DiskArray<char>(
      dir / ("data" + to_string(now_layer_idx) + ".bin"), true));
}



template <>
void LayerCSR::push_data(VID v_idx, size_t at_layer,
                         pair<VID *, size_t> &data) {
  // cout<<"append_data, len "<<data.second<<endl;
  append_raw_data(v_idx, at_layer,
                  {(char *)data.first, data.second * sizeof(VID)});
}

void LayerCSR::append_raw_data(VID v_idx, size_t layer_idx,
                               std::pair<char *, size_t> data) {
  auto [data_p, data_size] = data;
  // cout<<__FUNCTION__<<" layers[layer_idx].size():
  // "<<layers[layer_idx].size()<<endl;
  PosLen pl = {layers[layer_idx].size(), data_size};
  idxs[layer_idx][v_idx] = pl;
  layers[layer_idx].push_many(data_p, data_size);
}

void LayerCSR::append_raw_data_atomic(VID v_idx, size_t layer_idx,
                                      std::pair<char *, size_t> data) {
  auto [data_p, data_size] = data;
  size_t start = layers[layer_idx].push_many_atomic(data_p, data_size);
  PosLen pl = {start, data_size};
  idxs[layer_idx][v_idx] = pl;
}

void LayerCSR::set_v(VID *v, size_t vertex_count) {
  assert(get_vertex_count() == 0);
  assert(this->v.size() == 0);
  this->vertex_count = vertex_count;
  this->v = DiskArray<VID>(dir / "vertex.bin", true);
  this->v.push_many(v, vertex_count);
}

pair<char *, size_t> LayerCSR::get_raw_data(VID v_idx, size_t at_layer) {
  return {&layers[at_layer][0] + idxs[at_layer][v_idx].pos,
          idxs[at_layer][v_idx].len};
}

PosLen LayerCSR::get_poslen(VID vidx, size_t at_layer) {
  return idxs[at_layer][vidx];
}

MTCSR::MTCSR(size_t vertex_count, size_t thread_count)
    : thread_count(thread_count), vertex_count(vertex_count),
      idx(vertex_count) {
  for (size_t i = 0; i < thread_count; i++) {
    data.emplace_back();
  }
}

MTCSR::MTCSR(std::filesystem::path dir, size_t layer_idx, size_t vertex_count,
             size_t thread_count, bool overwrite)
    : thread_count(thread_count), vertex_count(vertex_count),
      idx(dir / ("idxs" + to_string(layer_idx) + ".bin"), vertex_count, true) {
  for (size_t i = 0; i < thread_count; i++) {
    data.emplace_back(dir / ("data" + to_string(layer_idx) + ".bin"),
                      overwrite);
  }
}

VIDArrayRef MTCSR::get_data(VID v_idx) {
  assert(v_idx < vertex_count);
  auto tlp = idx[v_idx];
  return {&data[tlp.tid][tlp.pos], tlp.len};
}

size_t MTCSR::count() {
  size_t re = 0;
  for (size_t i = 0; i < thread_count; i++) {
    re += data[i].size();
  }
  return re;
}

void MTCSR::push_whole_list(VID v_idx, VIDArrayRef v_data) {
  auto [data_p, data_size] = v_data;
  size_t end = data[omp_get_thread_num()].push_many(data_p, data_size);
  size_t start = end - data_size;
  TidLenPos tlp = {static_cast<uint32_t>(omp_get_thread_num()),
                   static_cast<uint32_t>(data_size), start};
  idx[v_idx] = tlp;
}

MTCSR::DataPusher::DataPusher(VID v_idx, MTCSR &csr, size_t tid)
    : idx(csr.idx[v_idx]), csr(csr) {
  if (tid == static_cast<size_t>(-1)) {
    idx.tid = omp_get_thread_num();
  } else {
    idx.tid = tid;
  }
  idx.pos = csr.data[idx.tid].size();
  idx.len = 0;
}

void MTCSR::DataPusher::push(VID data) {
  csr.data[idx.tid].push_back(std::move(data));
  idx.len++;
}

MTCSR::DataPusher MTCSR::get_data_pusher(VID v_idx, size_t tid) {
  return DataPusher(v_idx, *this, tid);
}

LayerMultiThreadCSR::LayerMultiThreadCSR(std::filesystem::path dir) : dir(dir) {
  if (filesystem::exists(dir) == false) {
    assert(filesystem::create_directory(dir));
  }

  Config conf(dir / "config.txt");
  vertex_count = conf.get_or_init<size_t>("VertexCount", 0);
  layer_count = conf.get_or_init<size_t>("LayerCount", 0);
  thread_count = conf.get_or_init<size_t>("ThreadCount", omp_get_max_threads());

  if (vertex_count > 0) {
    v = DiskArray<VID>(dir / "vertex.bin", false);
    for (size_t i = 0; i < layer_count; i++) {
      idxs.push_back(
          DiskArray<TidLenPos>(dir / ("idxs" + to_string(i) + ".bin"), false));
      layers.push_back({});
      for (size_t tid = 0; tid < thread_count; tid++) {
        layers[tid].push_back(DiskArray<VID>(
            dir / ("data" + to_string(i) + "-" + to_string(tid) + ".bin"),
            false));
      }
    }
  }
}

void LayerMultiThreadCSR::set_v(VID *v, size_t vertex_count) {
  assert(get_vertex_count() == 0);
  assert(this->v.size() == 0);
  this->vertex_count = vertex_count;
  this->v = DiskArray<VID>(dir / "vertex.bin", true);
  this->v.push_many(v, vertex_count);
}

void LayerMultiThreadCSR::init_first_layer() {
  assert(get_layer_count() == 0);
  new_layer();
#pragma omp parallel for
  for (size_t i = 0; i < get_vertex_count(); i++) {
    size_t tid = omp_get_thread_num();
    VID x = get_v()[i];
    VIDArrayRef p = {&x, 1};
    push_data(tid, i, 0, p);
  }
}

void LayerMultiThreadCSR::new_layer() {
  size_t now_layer_idx = get_layer_count();
  layer_count += 1;
  auto new_idx = DiskArray<TidLenPos>(
      dir / ("idxs" + to_string(now_layer_idx) + ".bin"), true);
  new_idx.resize(vertex_count, TidLenPos{0, 0, static_cast<uint64_t>(-1)});
  idxs.push_back(std::move(new_idx));

  layers.push_back({});
  for (size_t tid = 0; tid < thread_count; tid++) {
    layers.back().push_back(
        DiskArray<VID>(dir / ("data" + to_string(now_layer_idx) + "-" +
                              to_string(tid) + ".bin"),
                       true));
  }
}

void LayerMultiThreadCSR::push_data(size_t tid, VID v_idx, size_t at_layer,
                                    std::pair<VID *, size_t> data) {
  auto [data_p, data_size] = data;
  size_t end = layers[at_layer][tid].push_many(data_p, data_size);
  size_t start = end - data_size;
  TidLenPos tlp = {static_cast<uint32_t>(tid), static_cast<uint32_t>(data_size),
                   start};
  idxs[at_layer][v_idx] = tlp;
}

LayerMultiThreadCSR::DataPusher::DataPusher(size_t tid, VID v_idx,
                                            size_t at_layer,
                                            LayerMultiThreadCSR &csr)
    : v_idx(v_idx), at_layer(at_layer), csr(csr),
      idx(csr.idxs[at_layer][v_idx]) {
  idx.tid = tid;
  idx.pos = csr.layers[at_layer][tid].size();
  idx.len = 0;
}

void LayerMultiThreadCSR::DataPusher::push(VID data) {
  csr.layers[at_layer][idx.tid].push_back(std::move(data));
  idx.len++;
}

LayerMultiThreadCSR::DataPusher
LayerMultiThreadCSR::get_data_pusher(size_t tid, VID v_idx, size_t at_layer) {
  return DataPusher(tid, v_idx, at_layer, *this);
}

VIDArrayRef LayerMultiThreadCSR::get_data(VID v_idx, size_t at_layer) {
  auto tlp = idxs[at_layer][v_idx];
  return {&layers[at_layer][tlp.tid][tlp.pos], tlp.len};
}

MultiVisit::MultiVisit(filesystem::path dir) : dir(dir) {
  if (filesystem::exists(dir) == false) {
    filesystem::create_directory(dir);
  }
  Config conf(dir / "config.txt");
  vertex_count = conf.get_or_init("VertexCount", 0);
  visit_bit_length = conf.get_or_init("VisitBitLength", 0);
  if (vertex_count > 0) {
    loadArrayWithMmap(dir / "vertex.bin", v, vertex_count);
  }
}

vector<size_t> MultiVisit::source_visit_count() {
  vector<size_t> res;
  for (size_t i = 0; i < vertex_count; i++) {
    res.push_back(visit_bitmap[i].count());
  }
  return res;
}

vector<size_t> MultiVisit::target_visit_count() {
  vector<size_t> res(visit_bit_length, 0);
  for (size_t i = 0; i < vertex_count; i++) {
    get_bitmap_visit(i).for_each([&res](size_t x) { res[x]++; });
  }
  return res;
}

void MultiVisit::set_v(VID *v, size_t vertex_count, size_t visit_bit_length) {

  assert(this->vertex_count == 0);
  this->visit_bit_length = visit_bit_length;
  storeArrayWithMmap(dir / "vertex.bin", v, vertex_count);
  this->vertex_count = vertex_count;
  loadArrayWithMmap(dir / "vertex.bin", this->v, this->vertex_count);

  visit_bitmap.reserve(vertex_count);
  for (size_t i = 0; i < vertex_count; i++) {
    visit_bitmap.emplace_back(dir / ("visit" + to_string(i) + ".bin"),
                              visit_bit_length, false);
  }
}

VID MultiVisit::get_v_at(size_t at) { return v[at]; }

MmapBitmap &MultiVisit::get_bitmap_visit(VID v_idx) {
  return visit_bitmap[v_idx];
}




void MultiVisit::clone_from(MultiVisit &other) {
  set_v(other.v, other.vertex_count, other.visit_bit_length);
  for (size_t i = 0; i < vertex_count; i++) {
    visit_bitmap[i].clone_from(other.visit_bitmap[i]);
  }
}



template <>
pair<VID *, size_t> LayerCSR::get_data<pair<VID *, size_t>>(VID v_idx,
                                                            size_t at_layer) {
  auto [data, len] = get_raw_data(v_idx, at_layer);
  return {reinterpret_cast<VID *>(data), len / sizeof(VID)};
}

tbb::concurrent_hash_map<VID, size_t> LayerCSR::get_frequency() {
  tbb::concurrent_hash_map<VID, size_t> freq;
#pragma omp parallel for
  for (size_t i = 0; i < vertex_count; i++) {
    for (size_t j = 0; j < get_layer_count(); j++) {
      auto [data, len] = get_data<VIDArrayRef>(i, j);
      for (size_t k = 0; k < len; k++) {
        modify_or_insert<VID, size_t>(freq, data[k], 1,
                                      [](size_t &v) { v += 1; });
      }
    }
  }
  return freq;
}

vector<vector<vector<VID>>> LayerCSR::get_layer_frequency_detailed(VID *lm,
                                                                   size_t lmc) {
  set<VID> lms(lm, lm + lmc);

  vector<tbb::concurrent_hash_map<VID, std::vector<VID>>> freq(
      get_layer_count());
  vector<vector<vector<VID>>> re(get_layer_count(), vector<vector<VID>>(lmc));

#pragma omp parallel for
  for (size_t i = 0; i < vertex_count; i++) {
    for (size_t j = 0; j < get_layer_count(); j++) {
      auto [data, len] = get_data<VIDArrayRef>(i, j);
      for (size_t k = 0; k < len; k++) {
        if (lms.count(data[k])) {
          modify_or_insert<VID, vector<VID>>(
              freq[j], data[k], {static_cast<VID>(i)},
              [i](vector<VID> &v) { v.push_back(i); });
        }
      }
    }
  }

  for (size_t i = 0; i < freq.size(); i++) {
    for (size_t j = 0; j < lmc; j++) {
      tbb::concurrent_hash_map<VID, std::vector<VID>>::accessor x;
      if (freq[i].find(x, lm[j]))
        re[i][j] = x->second;
    }
  }

  return re;
}

template <> void LayerCSR::init_first_layer<pair<VID *, size_t>>() {
  assert(get_layer_count() == 0);
  new_layer();
  for (size_t i = 0; i < get_vertex_count(); i++) {
    VID x = get_v_at(i);
    pair<VID *, size_t> p = {&x, 1};
    // cout<<__FUNCTION__<<": "<<x<<" "<<endl;
    push_data(i, 0, p);
  }
}

AppendOnlyMultiVertexWriter::AppendOnlyMultiVertexWriter(filesystem::path path)
    : dir(path) {
  if (filesystem::exists(dir)) {
    cerr << "Removing old data at " << dir << endl;
    filesystem::remove(dir);
  }
  filesystem::create_directory(dir);
}

AppendOnlyMultiVertexWriter::~AppendOnlyMultiVertexWriter() {
  Config conf(dir / "config.txt");
  conf.set("VertexCount", v.size());
  conf.store();
  storeArrayWithMmap(dir / "vertex.bin", v.data(), v.size());
}

void AppendOnlyMultiVertexWriter::set_v(VID *v, size_t vertex_count) {
  this->v = vector<VID>(v, v + vertex_count);
  for (size_t i = 0; i < vertex_count; i++) {
    buffer.emplace_back(dir / ("output-" + to_string(v[i]) + ".bin"));
  }
}

void AppendOnlyMultiVertexWriter::append_data(VID v_idx,
                                              pair<char *, size_t> data) {
  buffer[v_idx].write(data.first, data.second);
}

