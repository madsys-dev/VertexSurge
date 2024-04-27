#pragma once

#include "common.hh"
#include "csv.hh"
#include "schema.hh"
#include "storage/disk_array.hh"
#include "storage/graph_concepts.hh"
#include "type.hh"
#include <nlohmann/json.hpp>

template <VertexWithRawID V>
std::unordered_map<typename V::RawID, VID>
unify_vertex_and_collect_statistics(std::vector<V> &vertices,
                                    VertexSchema &schema) {
  std::unordered_map<typename V::RawID, VID> re;
  for (VID i = 0; i < vertices.size(); i++) {
    assert(re.contains(vertices[i].raw_id) == false);
    re[vertices[i].raw_id] = i;
    vertices[i].id = i;
  }
  schema.statistics.count = vertices.size();
  return re;
}

template <EdgeWithRawID E>
void map_edge_id(
    std::vector<E> &edges,
    const std::unordered_map<typename E::SRC::RawID, VID> &src_map,
    const std::unordered_map<typename E::DST::RawID, VID> &dst_map) {
#pragma omp parallel for
  for (auto &e : edges) {
    e.src_id = src_map.at(e.raw_src_id);
    e.dst_id = dst_map.at(e.raw_dst_id);
  }
}

template <typename P>
void map_params_id(
    std::vector<P> &params,
    const std::unordered_map<typename P::VID_MAP_TYPE::RawID, VID> &map) {
#pragma omp parallel for
  for (auto &p : params) {
    p.id = map.at(p.raw_id);
  }
}

template <typename P>
void map_params_2_id(
    std::vector<P> &params,
    const std::unordered_map<typename P::VID_MAP_TYPE::RawID, VID> &map) {
#pragma omp parallel for
  for (auto &p : params) {
    p.id1 = map.at(p.raw_id1);
    p.id2 = map.at(p.raw_id2);
  }
}

template <Edge E>
void unify_edge_and_collect_statistics(std::vector<E> &edges,
                                       GraphSchema &graph_schema) {
  EdgeSchema &schema =
      graph_schema.get_edge_schema(edge_forward_simple_name<E>());

  if (schema.directed == false) {
    SPDLOG_INFO("Adding reverse edge for {}", typeid(E).name());
    add_reverse_edges(edges);
  } else {
    SPDLOG_INFO("{} is directed edge", typeid(E).name());
  }

  if (schema.allow_multiple_edge == false) {
    deduplicate_edges(edges);
  } else {
    SPDLOG_INFO("{} allows multiple edge", typeid(E).name());
  }

  sort_edges_by_src_dst(edges.data(), edges.size());
  schema.statistics.count = edges.size();
}

template <class... Ts>
void unify_all_edges(GraphSchema &graph_schema, Ts &...e) {
  ([&] { unify_edge_and_collect_statistics(e, graph_schema); }(), ...);
}

template <Edge E>
void write_vid_pair_to_disk(std::filesystem::path to,
                            const std::vector<E> &edges, Direction dir) {
  DiskArray<VIDPair> edge_on_disk(to, edges.size(), true);
#pragma omp parallel for
  for (size_t i = 0; i < edges.size(); i++) {
    switch (dir) {
    case Direction::Forward:
      [[fallthrough]];
    case Direction::Both: {
      edge_on_disk[i] = {edges[i].src_id, edges[i].dst_id};
      break;
    }
    case Direction::Backward: {
      edge_on_disk[i] = {edges[i].dst_id, edges[i].src_id};
      break;
    }
    default: {
      assert(0);
    }
    }
  }
}

// !! edges must be sorted by dst src !!
template <Edge E>
std::vector<std::vector<E>> partition_by_dst(const std::vector<E> &edges,
                                             size_t count) {
  std::vector<std::vector<E>> re;

  re.resize(count);
  size_t pos = 0;
  size_t expected = 0;
  size_t max_block_size = 0;
  size_t min_block_size = std::numeric_limits<size_t>::max();
  for (size_t i = 0; i < count; i++) {
    expected = (edges.size() - pos) / (count - i);
    expected += pos;
    while (true) {
      re[i].push_back(edges[pos]);
      pos++;
      if (pos == edges.size() ||
          (pos >= expected && edges[pos].dst_id != edges[pos - 1].dst_id)) {
        break;
      }
    }
    min_block_size = std::min(min_block_size, re[i].size());
    max_block_size = std::max(max_block_size, re[i].size());
  }
  SPDLOG_INFO("{} Partition, Size: {}-{}({},{})", count, min_block_size,
              max_block_size, max_block_size - min_block_size,
              1.0 * (max_block_size - min_block_size) / max_block_size);
  return re;
}

#define VERTEX_DISK_ARRAY_INIT(ENTITY, PROPERTY_NAME, TYPE, VAR)               \
  do {                                                                         \
    DiskArray<TYPE> on_disk(to / #ENTITY "." #PROPERTY_NAME, VAR.size(),       \
                            true);                                             \
    _Pragma("omp parallel for") for (size_t i = 0; i < VAR.size(); i++) {      \
      on_disk[i] = VAR[i].PROPERTY_NAME;                                       \
    }                                                                          \
    SPDLOG_INFO("Wrote {} {} to {}", VAR.size(), #PROPERTY_NAME,               \
                to / #ENTITY "." #PROPERTY_NAME);                              \
  } while (0)

template <Edge E>
void gen_and_write_one_edge(std::filesystem::path to, std::vector<E> &edges,
                            GraphSchema &graph_schema) {
  const EdgeSchema &schema =
      graph_schema.get_edge_schema(edge_forward_simple_name<E>());
  size_t src_vertex_count =
      graph_schema.get_vertex_schema(E::SRC::TYPE_NAME).get_count();
  size_t dst_vertex_count =
      graph_schema.get_vertex_schema(E::DST::TYPE_NAME).get_count();
  SPDLOG_INFO(" - Generating Storage for {}", schema.type);

  auto [csr_src_dst, csr_dst_src] = schema.src_or_dst_for_gen();

  if (csr_src_dst) {
    SPDLOG_INFO("Generating CSR from SRC");
    auto csr_index = sort_and_generate_index_from_src(
        src_vertex_count, edges.data(), edges.size());
    DiskArray<Offset> index_on_disk(to / schema.get_src_dst_csr_index_name(),
                                    csr_index.size(), true);
#pragma omp parallel for
    for (size_t i = 0; i < csr_index.size(); i++) {
      index_on_disk[i] = csr_index[i];
    }

    DiskArray<VID> dst_on_disk(to / schema.get_src_dst_csr_data_name(),
                               edges.size(), true);
#pragma omp parallel for
    for (size_t i = 0; i < edges.size(); i++) {
      dst_on_disk[i] = edges[i].dst_id;
    }
  }
  if (csr_dst_src) {
    SPDLOG_INFO("Generating CSR from DST");
    auto csr_index = sort_and_generate_index_from_dst(
        dst_vertex_count, edges.data(), edges.size());
    DiskArray<Offset> index_on_disk(to / schema.get_dst_src_csr_index_name(),
                                    csr_index.size(), true);
#pragma omp parallel for
    for (size_t i = 0; i < csr_index.size(); i++) {
      index_on_disk[i] = csr_index[i];
    }

    DiskArray<VID> src_on_disk(to / schema.get_dst_src_csr_data_name(),
                               edges.size(), true);
#pragma omp parallel for
    for (size_t i = 0; i < edges.size(); i++) {
      src_on_disk[i] = edges[i].src_id;
    }
  }

  std::vector<Direction> dirs_for_flat_edges;
  auto [src_dst, dst_src] = schema.src_or_dst_for_gen();
  if (src_dst)
    dirs_for_flat_edges.push_back(Direction::Forward);
  if (dst_src)
    dirs_for_flat_edges.push_back(Direction::Backward);

  for (const auto &dir : dirs_for_flat_edges) {
    for (auto &f : schema.store_info.flats) {
      SPDLOG_INFO("Generating {} {} Edges in One File", dir,
                  EdgeSchema::StorageInfo::to_string(f));

      switch (f) {
      case EdgeSchema::StorageInfo::FlatEdgeOrder::Hilbert: {
        sort_edge_in_peano2(src_vertex_count, dst_vertex_count, edges.data(),
                            edges.size());
        break;
      }
      case EdgeSchema::StorageInfo::FlatEdgeOrder::SrcDst: {
        sort_edges_by_src_dst(edges.data(), edges.size());
        break;
      }
      case EdgeSchema::StorageInfo::FlatEdgeOrder::DstSrc: {
        sort_edges_by_dst_src(edges.data(), edges.size());
        break;
      }
      case EdgeSchema::StorageInfo::FlatEdgeOrder::Random: {
        random_shuffle_edges(edges.data(), edges.size());
        break;
      }
      default:
        assert(0);
      }
      write_vid_pair_to_disk(
          to / schema.get_flat_edges_name(dir, f, std::nullopt), edges, dir);
    }
  }
  for (const auto &dir : dirs_for_flat_edges) {
    for (size_t order_id = 0;
         order_id < schema.store_info.partitioned_flats.size(); order_id++) {
      auto order = schema.store_info.partitioned_flats[order_id];
      auto count = schema.store_info.partition_counts[order_id];
      SPDLOG_INFO("Generating {} Edges of {} in {} Partitions",
                  EdgeSchema::StorageInfo::to_string(order), dir, count);
      sort_edges_by_dst_src(edges.data(), edges.size());
      auto partitions = partition_by_dst(edges, count);
      for (size_t i = 0; i < partitions.size(); i++) {
        auto &partition = partitions[i];
        switch (order) {
        case EdgeSchema::StorageInfo::FlatEdgeOrder::Hilbert: {
          sort_edge_in_peano2(src_vertex_count, dst_vertex_count,
                              partition.data(), partition.size());
          break;
        }
        case EdgeSchema::StorageInfo::FlatEdgeOrder::SrcDst: {
          sort_edges_by_src_dst(partition.data(), partition.size());
          break;
        }
        case EdgeSchema::StorageInfo::FlatEdgeOrder::DstSrc: {
          sort_edges_by_dst_src(partition.data(), partition.size());
          break;
        }
        case EdgeSchema::StorageInfo::FlatEdgeOrder::Random: {
          random_shuffle_edges(partition.data(), partition.size());
          break;
        }
        default:
          assert(0);
        }

        write_vid_pair_to_disk(to / schema.get_flat_edges_name(dir, order, i),
                               partition, dir);
      }
    }
  }
}

template <class... Ts>
void gen_and_write_all_edges(std::filesystem::path to,
                             GraphSchema &graph_schema, Ts &...edges) {
  ([&]() { gen_and_write_one_edge(to, edges, graph_schema); }(), ...);
}

template <ExportableToCSV T>
void write_param_to_csv(std::filesystem::path to, std::vector<T> &params) {
  SPDLOG_INFO("Writing {} Params {} to {}", params.size(), T::NAME,
              to / fmt::format("{}.csv", T::NAME));
  save_to_csv(to / fmt::format("{}.csv", T::NAME), params);
}

template <class... Ts>
void write_params_to_csv(std::filesystem::path to, Ts &...params) {
  ([&]() { write_param_to_csv(to, params); }(), ...);
}

template <Edge E>
void write_edge_to_txt(std::ofstream &out, std::vector<E> &edge) {
  for (auto &e : edge) {
    out << e.src_id << " " << e.dst_id << std::endl;
  }
}

template <class... Ts>
void write_edges_to_txt(std::filesystem::path to, Ts &...params) {
  std::ofstream out(to / "edges.txt");
  SPDLOG_INFO("Write to {}", to / "edges.txt");
  ([&]() { write_edge_to_txt(out, params); }(), ...);
}

template <ExportableToCSV T>
requires Vertex<T>
void write_vertex_to_csv(std::filesystem::path to, std::vector<T> &vertex) {
  auto dir = to / fmt::format("{}.csv", T::TYPE_NAME);
  SPDLOG_INFO("Writing {} Vertices to {}", vertex.size(), dir);
  save_to_csv(dir, vertex, false);
}

template <class... Ts>
void write_vertices_to_csv(std::filesystem::path to, Ts &...vertices) {
  ([&]() { write_vertex_to_csv(to, vertices); }(), ...);
}

template <ExportableToCSV E>
requires Edge<E>
void write_edge_to_csv(std::filesystem::path to, std::vector<E> &edge) {
  auto edge_name = E::TYPE_NAME;
  auto dir = to / fmt::format("{}.csv", edge_name);
  SPDLOG_INFO("Writing {} Edges to {}", edge.size(), dir);
  save_to_csv(dir, edge, false);
}

template <class... Ts>
void write_edges_to_csv(std::filesystem::path to, Ts &...edges) {
  ([&]() { write_edge_to_csv(to, edges); }(), ...);
}

class SocialNetworkImporter {

  struct Person {
    VID id;
    inline static const char *TYPE_NAME = "Person";

    using RawID = uint64_t;
    RawID raw_id;

    ShortString name;
    TagValue8 community;

    Person() = default;
    Person(VID id, ShortString name, TagValue8 community)
        : id(id), name(name), community(community){};

    static Person load_from_ldbc_bi(const std::vector<std::string> &titles,
                                    std::vector<std::string> &&values);

    static Person load_from_ldbc_ic(const std::vector<std::string> &titles,
                                    std::vector<std::string> &&values);

    inline static std::string csv_header() { return "id,name,community"; }
    inline std::string to_csv_line() const {
      return fmt::format("{},{},{}", id, name, community);
    }

    inline static VertexSchema gen_schema() {
      auto re = VertexSchema("Person");
      re.add_property("id", Type::VID);
      re.add_property("name", Type::ShortString);
      re.add_property("community", Type::TagValue8);
      return re;
    }
  };

  struct knows {
    using SRC = SocialNetworkImporter::Person;
    using DST = SocialNetworkImporter::Person;
    inline static const char *TYPE_NAME = "knows";
    VID src_id;
    VID dst_id;

    SRC::RawID raw_src_id;
    DST::RawID raw_dst_id;

    knows() = default;

    static knows load_from_ldbc_bi(const std::vector<std::string> &titles,
                                   std::vector<std::string> &&values);

    static knows load_from_ldbc_ic(const std::vector<std::string> &titles,
                                   std::vector<std::string> &&values);
    static knows load_from_ldbc_fin(const std::vector<std::string> &titles,
                                    std::vector<std::string> &&values);

    static knows load_from_snap_csv(const std::vector<std::string> &titles,
                                    std::vector<std::string> &&values);

    inline static std::string csv_header() { return "SRC_ID,DST_ID"; }
    inline std::string to_csv_line() {
      return fmt::format("{},{}", src_id, dst_id);
    }

    inline static EdgeSchema gen_schema() {
      auto re = EdgeSchema("knows");
      re.directed = false;
      re.allow_multiple_edge = false;
      re.src = "Person";
      re.dst = "Person";
      return re;
    }
  };

  std::vector<Person> persons;
  std::vector<knows> knowses;

  void init_graph_schema();
  void set_random_community();

  bool rearranged = false;
  void rearrange();

public:
  SocialNetworkImporter() = default;

  void load_form_ldbc_bi(std::filesystem::path dir);
  void load_form_ldbc_ic(std::filesystem::path dir);
  void load_from_snap_txt(std::filesystem::path dir);
  void load_from_snap_csv(std::filesystem::path dir);

  void output_vlgpm(std::filesystem::path to);
  void output_kuzu_csv(std::filesystem::path to);
  void output_tugraph(std::filesystem::path to);
  void output_label_graph(std::filesystem::path to);

  GraphSchema graph_schema;
  ImportConfig import_config;
  std::optional<size_t> pcnt;
};

class TransferImporter {

  struct Account {
    VID id;
    inline static const char *TYPE_NAME = "Account";

    using RawID = uint64_t;
    RawID raw_id;

    Account() = default;
    Account(VID id) : id(id){};

    inline static std::string csv_header() { return "id"; }
    inline std::string to_csv_line() const { return fmt::format("{}", id); }

    inline static VertexSchema gen_schema() {
      auto re = VertexSchema("Account");
      re.add_property("id", Type::VID);
      return re;
    }
  };

  struct transfer {
    using SRC = TransferImporter::Account;
    using DST = TransferImporter::Account;
    inline static const char *TYPE_NAME = "transfer";
    VID src_id;
    VID dst_id;
    SRC::RawID raw_src_id;
    DST::RawID raw_dst_id;

    transfer() = default;

    static transfer load_from_rabo(const std::vector<std::string> &titles,
                                   std::vector<std::string> &&values);
    static transfer load_from_ldbc_fin(const std::vector<std::string> &titles,
                                       std::vector<std::string> &&values);

    inline static std::string csv_header() { return "SRC_ID,DST_ID"; }
    inline std::string to_csv_line() {
      return fmt::format("{},{}", src_id, dst_id);
    }

    inline static EdgeSchema gen_schema() {
      auto re = EdgeSchema("transfer");
      re.directed = true;
      re.allow_multiple_edge = true;
      re.src = "Account";
      re.dst = "Account";
      return re;
    }
  };

  std::vector<Account> accounts;
  std::vector<transfer> transfers;

  void init_graph_schema();

  bool rearranged = false;
  void rearrange();

public:
  TransferImporter() = default;

  // Only Edges, no Vertex, Vertex ids are assigned when rearrange() function is
  // called.
  void load_from_rabo(std::filesystem::path csv_path);
  void load_from_ldbc_fin(std::filesystem::path dir);

  void output_vlgpm(std::filesystem::path to);
  void output_kuzu_csv(std::filesystem::path to);
  void output_label_graph(std::filesystem::path to);

  GraphSchema graph_schema;
  ImportConfig import_config;
  std::optional<size_t> pcnt;
};

/*
This imports

Person [id:ID]
Account [id:ID]
Medium [id:ID,isBlocked:TagValue8]
Loadn [id:ID,balance:INT64]

own Person -> Account
transfer Account -> Account
withdraw Account -> Account
signIn Medium -> Account
deposit Loan -> Account
*/
template <class T> bool compare_by_raw_id(const T &a, const T &b) {
  return a.raw_id < b.raw_id;
}

class LDBCFinBenchImporter {
  struct Person {
    VID id;
    inline static const char *TYPE_NAME = "Person";

    using RawID = uint64_t;
    RawID raw_id;

    Person() = default;
    Person(VID id) : id(id){};

    static Person load_from_ldbc_fin(const std::vector<std::string> &titles,
                                     std::vector<std::string> &&values);

    inline static std::string csv_header() { return "id"; }
    inline std::string to_csv_line() const { return fmt::format("{}", id); }

    inline static VertexSchema gen_schema() {
      auto re = VertexSchema("Person");
      re.add_property("id", Type::VID);
      return re;
    }
  };

  struct Account {
    VID id;
    inline static const char *TYPE_NAME = "Account";

    using RawID = uint64_t;
    RawID raw_id;
    Account() = default;
    Account(VID id) : id(id){};
    static Account load_from_ldbc_fin(const std::vector<std::string> &titles,
                                      std::vector<std::string> &&values);
    inline static std::string csv_header() { return "id"; }
    inline std::string to_csv_line() const { return fmt::format("{}", id); }
    inline static VertexSchema gen_schema() {
      auto re = VertexSchema("Account");
      re.add_property("id", Type::VID);
      return re;
    }
  };

  struct Medium {
    VID id;
    TagValue8 isBlocked;
    inline static const char *TYPE_NAME = "Medium";

    using RawID = uint64_t;
    RawID raw_id;
    Medium() = default;
    Medium(VID id, TagValue8 isBlocked) : id(id), isBlocked(isBlocked){};
    static Medium load_from_ldbc_fin(const std::vector<std::string> &titles,
                                     std::vector<std::string> &&values);
    inline static std::string csv_header() { return "id,isBlocked"; }
    inline std::string to_csv_line() const {
      return fmt::format("{},{}", id, isBlocked);
    }
    inline static VertexSchema gen_schema() {
      auto re = VertexSchema("Medium");
      re.add_property("id", Type::VID);
      re.add_property("isBlocked", Type::TagValue8);
      return re;
    }
  };

  struct Loan {
    VID id;
    int64_t balance;
    inline static const char *TYPE_NAME = "Loan";

    using RawID = uint64_t;
    RawID raw_id;
    Loan() = default;
    Loan(VID id, int64_t balance) : id(id), balance(balance){};
    static Loan load_from_ldbc_fin(const std::vector<std::string> &titles,
                                   std::vector<std::string> &&values);
    inline static std::string csv_header() { return "id,balance"; }
    inline std::string to_csv_line() const {
      return fmt::format("{},{}", id, balance);
    }
    inline static VertexSchema gen_schema() {
      auto re = VertexSchema("Loan");
      re.add_property("id", Type::VID);
      re.add_property("balance", Type::Int64);
      return re;
    }
  };

  struct own {
    using SRC = LDBCFinBenchImporter::Person;
    using DST = LDBCFinBenchImporter::Account;
    inline static const char *TYPE_NAME = "own";
    VID src_id;
    VID dst_id;
    SRC::RawID raw_src_id;
    DST::RawID raw_dst_id;

    own() = default;

    static own load_from_ldbc_fin(const std::vector<std::string> &titles,
                                  std::vector<std::string> &&values);

    inline static std::string csv_header() { return "SRC_ID,DST_ID"; }
    inline std::string to_csv_line() {
      return fmt::format("{},{}", src_id, dst_id);
    }

    inline static EdgeSchema gen_schema() {
      auto re = EdgeSchema("own");
      re.directed = true;
      re.allow_multiple_edge = false;
      re.src = "Person";
      re.dst = "Account";
      return re;
    }
  };

  struct transfer {
    using SRC = LDBCFinBenchImporter::Account;
    using DST = LDBCFinBenchImporter::Account;
    inline static const char *TYPE_NAME = "transfer";
    VID src_id;
    VID dst_id;
    SRC::RawID raw_src_id;
    DST::RawID raw_dst_id;

    transfer() = default;

    static transfer load_from_ldbc_fin(const std::vector<std::string> &titles,
                                       std::vector<std::string> &&values);
    inline static std::string csv_header() { return "SRC_ID,DST_ID"; }
    inline std::string to_csv_line() {
      return fmt::format("{},{}", src_id, dst_id);
    }

    inline static EdgeSchema gen_schema() {
      auto re = EdgeSchema("transfer");
      re.directed = true;
      re.allow_multiple_edge = true;
      re.src = "Account";
      re.dst = "Account";
      return re;
    }
  };

  struct withdraw {
    using SRC = LDBCFinBenchImporter::Account;
    using DST = LDBCFinBenchImporter::Account;
    inline static const char *TYPE_NAME = "withdraw";
    VID src_id;
    VID dst_id;
    SRC::RawID raw_src_id;
    DST::RawID raw_dst_id;

    withdraw() = default;

    static withdraw load_from_ldbc_fin(const std::vector<std::string> &titles,
                                       std::vector<std::string> &&values);
    inline static std::string csv_header() { return "SRC_ID,DST_ID"; }
    inline std::string to_csv_line() {
      return fmt::format("{},{}", src_id, dst_id);
    }

    inline static EdgeSchema gen_schema() {
      auto re = EdgeSchema("withdraw");
      re.directed = true;
      re.allow_multiple_edge = true;
      re.src = "Account";
      re.dst = "Account";
      return re;
    }
  };

  struct signIn {
    using SRC = LDBCFinBenchImporter::Medium;
    using DST = LDBCFinBenchImporter::Account;
    inline static const char *TYPE_NAME = "signIn";
    VID src_id;
    VID dst_id;
    SRC::RawID raw_src_id;
    DST::RawID raw_dst_id;

    signIn() = default;

    static signIn load_from_ldbc_fin(const std::vector<std::string> &titles,
                                     std::vector<std::string> &&values);
    inline static std::string csv_header() { return "SRC_ID,DST_ID"; }
    inline std::string to_csv_line() {
      return fmt::format("{},{}", src_id, dst_id);
    }

    inline static EdgeSchema gen_schema() {
      auto re = EdgeSchema("signIn");
      re.directed = true;
      re.allow_multiple_edge = true;
      re.src = "Medium";
      re.dst = "Account";
      return re;
    }
  };

  struct deposit {
    using SRC = LDBCFinBenchImporter::Loan;
    using DST = LDBCFinBenchImporter::Account;
    inline static const char *TYPE_NAME = "deposit";
    VID src_id;
    VID dst_id;
    SRC::RawID raw_src_id;
    DST::RawID raw_dst_id;

    deposit() = default;

    static deposit load_from_ldbc_fin(const std::vector<std::string> &titles,
                                      std::vector<std::string> &&values);
    inline static std::string csv_header() { return "SRC_ID,DST_ID"; }
    inline std::string to_csv_line() {
      return fmt::format("{},{}", src_id, dst_id);
    }

    inline static EdgeSchema gen_schema() {
      auto re = EdgeSchema("deposit");
      re.directed = true;
      re.allow_multiple_edge = true;
      re.src = "Loan";
      re.dst = "Account";
      return re;
    }
  };

  struct tcr1_param {
    using VID_MAP_TYPE = LDBCFinBenchImporter::Account;
    inline static const char *NAME = "tcr1";
    VID id;
    VID_MAP_TYPE::RawID raw_id;
    tcr1_param() = default;

    static tcr1_param load_from_ldbc_fin(const std::vector<std::string> &titles,
                                         std::vector<std::string> &&values);
    inline static std::string csv_header() { return "id"; }
    inline std::string to_csv_line() { return fmt::format("{}", id); }
  };

  struct tcr2_param {
    using VID_MAP_TYPE = LDBCFinBenchImporter::Person;
    inline static const char *NAME = "tcr2";
    VID id;
    VID_MAP_TYPE::RawID raw_id;
    tcr2_param() = default;

    static tcr2_param load_from_ldbc_fin(const std::vector<std::string> &titles,
                                         std::vector<std::string> &&values);
    inline static std::string csv_header() { return "id"; }
    inline std::string to_csv_line() { return fmt::format("{}", id); }
  };

  struct tcr3_param {
    using VID_MAP_TYPE = LDBCFinBenchImporter::Account;
    inline static const char *NAME = "tcr3";
    VID id1, id2;
    VID_MAP_TYPE::RawID raw_id1, raw_id2;
    tcr3_param() = default;

    static tcr3_param load_from_ldbc_fin(const std::vector<std::string> &titles,
                                         std::vector<std::string> &&values);
    inline static std::string csv_header() { return "id1,id2"; }
    inline std::string to_csv_line() { return fmt::format("{},{}", id1, id2); }
  };

  struct tcr6_param {
    using VID_MAP_TYPE = LDBCFinBenchImporter::Account;
    inline static const char *NAME = "tcr6";
    VID id;
    VID_MAP_TYPE::RawID raw_id;
    tcr6_param() = default;

    static tcr6_param load_from_ldbc_fin(const std::vector<std::string> &titles,
                                         std::vector<std::string> &&values);
    inline static std::string csv_header() { return "id"; }
    inline std::string to_csv_line() { return fmt::format("{}", id); }
  };

  struct tcr8_param {
    using VID_MAP_TYPE = LDBCFinBenchImporter::Loan;
    inline static const char *NAME = "tcr8";
    VID id;
    VID_MAP_TYPE::RawID raw_id;
    tcr8_param() = default;

    static tcr8_param load_from_ldbc_fin(const std::vector<std::string> &titles,
                                         std::vector<std::string> &&values);
    inline static std::string csv_header() { return "id"; }
    inline std::string to_csv_line() { return fmt::format("{}", id); }
  };

  std::vector<Account> accounts;
  std::vector<Person> persons;
  std::vector<Medium> mediums;
  std::vector<Loan> loans;

  std::vector<own> owns;
  std::vector<transfer> transfers;
  std::vector<withdraw> withdraws;
  std::vector<signIn> signIns;
  std::vector<deposit> deposits;

  std::vector<tcr1_param> tcr1_params;
  std::vector<tcr2_param> tcr2_params;
  std::vector<tcr3_param> tcr3_params;
  std::vector<tcr6_param> tcr6_params;
  std::vector<tcr8_param> tcr8_params;

  void init_graph_schema();

  bool rearranged = false;
  void rearrange();

public:
  LDBCFinBenchImporter() = default;

  void load_from_ldbc_fin(std::filesystem::path dir);
  void load_params_from_ldbc_fin(std::filesystem::path dir);

  void output_vlgpm(std::filesystem::path to);
  void output_kuzu_csv(std::filesystem::path to);
  void output_label_graph(std::filesystem::path to);

  GraphSchema graph_schema;
  ImportConfig import_config;
  std::optional<size_t> pcnt;
};
