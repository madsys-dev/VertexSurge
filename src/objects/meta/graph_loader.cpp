#include <algorithm>
#include <map>
#include <mutex>

#include <unordered_set>

#include "csv.hh"
#include "meta/graph_loader.hh"
#include "storage/disk_array.hh"
#include "storage/graph_concepts.hh"
#include "tbb/parallel_sort.h"
#include "utils.hh"

using json = nlohmann::json;
using namespace std;

SocialNetworkImporter::Person SocialNetworkImporter::Person::load_from_ldbc_bi(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  Person p;
  bool check[3] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "id") {
      p.raw_id = stoull(values[i]);
      check[0] = true;
    }
    if (titles[i] == "firstName") {
      removeNonDisplayableCharacters(values[i]);
      p.name = short_string_from_string(values[i]);
      check[1] = true;
    }
    if (titles[i] == "LocationCityId") {
      p.community = stoull(values[i]);
      check[2] = true;
    }
  }
  if (!check[0] || !check[1] || !check[2]) {
    SPDLOG_ERROR("CSV parse failure, \ntitles: {}\n values: {}", titles,
                 values);
    exit(1);
  }
  return p;
}

SocialNetworkImporter::Person SocialNetworkImporter::Person::load_from_ldbc_ic(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  Person p;
  bool check[3] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "id") {
      p.raw_id = stoull(values[i]);
      check[0] = true;
    }
    if (titles[i] == "firstName") {
      removeNonDisplayableCharacters(values[i]);
      p.name = short_string_from_string(values[i]);
      check[1] = true;
    }
    if (titles[i] == "place") {
      p.community = stoull(values[i]);
      check[2] = true;
    }
  }
  if (!check[0] || !check[1] || !check[2]) {
    SPDLOG_ERROR("CSV parse failure, \ntitles: {}\n values: {}", titles,
                 values);
    exit(1);
  }
  return p;
}

SocialNetworkImporter::knows SocialNetworkImporter::knows::load_from_ldbc_bi(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  knows k;
  bool check[2] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "Person1Id") {
      k.raw_src_id = stoull(values[i]);
      check[0] = true;
    }
    if (titles[i] == "Person2Id") {
      k.raw_dst_id = stoull(values[i]);
      check[1] = true;
    }
  }
  if (!check[0] || !check[1]) {
    SPDLOG_ERROR("CSV parse failure, \ntitles: {}\n values: {}", titles,
                 values);
    exit(1);
  }
  return k;
}

SocialNetworkImporter::knows SocialNetworkImporter::knows::load_from_ldbc_ic(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  knows k;
  assert(titles.size() == values.size());
  assert(titles.size() == 3);
  assert(titles[0] == "Person.id");
  assert(titles[1] == "Person.id");

  k.raw_src_id = stoull(values[0]);
  k.raw_dst_id = stoull(values[1]);

  return k;
}

SocialNetworkImporter::knows SocialNetworkImporter::knows::load_from_snap_csv(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  knows k;
  assert(titles.size() == values.size());
  assert(titles.size() == 2);
  assert(titles[0] == "node_1");
  assert(titles[1] == "node_2");

  k.raw_src_id = stoull(values[0]);
  k.raw_dst_id = stoull(values[1]);

  return k;
}

void SocialNetworkImporter::load_form_ldbc_bi(filesystem::path dir) {
  auto snapshot_dir = dir / "graphs" / "csv" / "bi" / "composite-merged-fk" /
                      "initial_snapshot";
  SPDLOG_INFO("Loading Social Network from LDBC at {}", snapshot_dir);

  persons = load_from_multiple_csv_with_f<SocialNetworkImporter::Person>(
      get_csr_filepath_in(snapshot_dir / "dynamic" / "Person"), '|',
      Person::load_from_ldbc_bi);

  knowses = load_from_multiple_csv_with_f<SocialNetworkImporter::knows>(
      get_csr_filepath_in(snapshot_dir / "dynamic" / "Person_knows_Person"),
      '|', knows::load_from_ldbc_bi);

  SPDLOG_INFO("Load {} Vertex from LDBC Person", persons.size());
  SPDLOG_INFO("Load {} Knows from LDBC knows", knowses.size());
}

void SocialNetworkImporter::load_form_ldbc_ic(std::filesystem::path dir) {
  auto dynamic = dir / "social_network" / "dynamic";

  persons = load_from_csv_with_f<SocialNetworkImporter::Person>(
      dynamic / "person_0_0.csv", '|', Person::load_from_ldbc_ic);

  knowses = load_from_csv_with_f<SocialNetworkImporter::knows>(
      dynamic / "person_knows_person_0_0.csv", '|', knows::load_from_ldbc_ic);

  SPDLOG_INFO("Load {} Vertex from LDBC Person", persons.size());
  SPDLOG_INFO("Load {} Knows from LDBC knows", knowses.size());
}

void SocialNetworkImporter::load_from_snap_txt(std::filesystem::path dir) {
  std::ifstream file(dir);
  std::string line;
  std::unordered_set<VID> vertex_set;
  std::mt19937 gen(1234);
  // size_t expected_community_size = 2000;

  while (std::getline(file, line)) {
    // 忽略以#开头的行
    if (line[0] == '#') {
      continue;
    }
    // 将行字符串转换为整数对
    uint64_t x, y;
    std::sscanf(line.c_str(), "%lu %lu", &x, &y);
    knows k;
    k.raw_src_id = x;
    k.raw_dst_id = y;
    knowses.push_back(k);
    vertex_set.insert(x);
    vertex_set.insert(y);
  }
  SPDLOG_INFO("Load {} Knows from SNAP txt", knowses.size());
  file.close();

  // std::uniform_int_distribution<TagValue8> random_community(
  //     0, std::min(static_cast<TagValue8>(vertex_set.size() /
  //                                        expected_community_size),
  //                 numeric_limits<TagValue8>::max()));
  for (auto &v : vertex_set) {
    Person p;
    p.raw_id = v;
    p.name = random_letter_short_string(gen);
    persons.push_back(p);
  }
}

void SocialNetworkImporter::load_from_snap_csv(std::filesystem::path dir) {

  std::unordered_set<VID> vertex_set;
  std::mt19937 gen(1234);

  knowses = load_from_csv_with_f<SocialNetworkImporter::knows>(
      dir, ',', knows::load_from_snap_csv);

  for (auto &k : knowses) {
    vertex_set.insert(k.src_id);
    vertex_set.insert(k.dst_id);
  }

  for (auto &v : vertex_set) {
    Person p;
    p.raw_id = v;
    p.name = random_letter_short_string(gen);
    persons.push_back(p);
  }

  SPDLOG_INFO("Load {} Knows from LDBC knows", knowses.size());
}

void SocialNetworkImporter::init_graph_schema() {
  graph_schema.vertex.push_back(Person::gen_schema());
  graph_schema.edge.push_back(knows::gen_schema());

  graph_schema.graph_type = "Social Network";

  for (auto &es : graph_schema.edge) {
    if (es.type == "knows") {
      es.store_info.add_src_dst(std::nullopt);
      es.store_info.add_src_dst(pcnt);
      es.store_info.add_hilbert(std::nullopt);
      es.store_info.add_hilbert(pcnt);
    } else {
      throw std::runtime_error("Unknown edge type");
    }
  }
}

void SocialNetworkImporter::set_random_community() {
  size_t expected_community_size = 2048;
  vector<TagValue8> community(persons.size());
  vector<size_t> community_sel = {1,   1,   1,   10,   10,   10,
                                  100, 100, 100, 1000, 1000, 1000};

  size_t idx = 0;
  for (size_t c = 0; c < community_sel.size(); c++) {
    for (size_t x = 0; x < expected_community_size * community_sel[c]; x++) {
      if (idx < persons.size()) {
        community[idx] = c;
        idx++;
      }
    }
  }
  if (idx < persons.size()) {
    SPDLOG_WARN("Person after {} are not assigned to community", idx);
  } else {
    SPDLOG_INFO("Person are all assigned to community");
  }
  while (idx < persons.size()) {
    community[idx] = community_sel.size();
    idx++;
  }

  std::mt19937 gen(1234);
  std::shuffle(community.begin(), community.end(), gen);

  for (size_t i = 0; i < persons.size(); i++) {
    persons[i].community = community[i];
  }
}

void SocialNetworkImporter::rearrange() {
  init_graph_schema();

  std::sort(persons.begin(), persons.end(), compare_by_raw_id<Person>);
  set_random_community();

  SPDLOG_INFO("Rearranging new id index");
  std::unordered_map<Person::RawID, VID> person_raw_to_new =
      unify_vertex_and_collect_statistics(
          persons, graph_schema.get_vertex_schema("Person"));

  map_edge_id(knowses, person_raw_to_new, person_raw_to_new);
  unify_all_edges(graph_schema, knowses);

  SPDLOG_INFO("Rearranging complete");
  rearranged = true;
}

void SocialNetworkImporter::output_vlgpm(std::filesystem::path to) {
  if (rearranged == false) {
    rearrange();
  }
  if (filesystem::exists(to) == false) {
    filesystem::create_directories(to);
  } else {
    SPDLOG_WARN("Overwriting...");
  }

  SPDLOG_INFO("Writing Social Networks to {}", to);

  SPDLOG_INFO("Writing Vertex");

  VERTEX_DISK_ARRAY_INIT(Person, id, VID, persons);
  VERTEX_DISK_ARRAY_INIT(Person, name, ShortString, persons);
  VERTEX_DISK_ARRAY_INIT(Person, community, TagValue8, persons);

  SPDLOG_INFO("Writing Edges");
  gen_and_write_all_edges(to, graph_schema, knowses);

  json schema = graph_schema;
  std::ofstream schema_file(to / "schema.json");
  schema_file << setw(4) << schema << std::endl;
  SPDLOG_INFO("Wrote graph schema to {}", to / "schema.json");
}

void SocialNetworkImporter::output_kuzu_csv(std::filesystem::path to) {
  if (rearranged == false) {
    rearrange();
  }

  if (filesystem::exists(to) == false) {
    filesystem::create_directories(to);
  } else {
    SPDLOG_WARN("Overwriting...");
  }

  SPDLOG_INFO("Writing Social Network in CSV to {}", to);
  write_vertices_to_csv(to, persons);
  write_edges_to_csv(to, knowses);

  json schema = graph_schema;
  std::ofstream schema_file(to / "schema.json");
  schema_file << setw(4) << schema << std::endl;
  SPDLOG_INFO("Wrote graph schema to {}", to / "schema.json");
}

void SocialNetworkImporter::output_label_graph(std::filesystem::path to) {
  if (rearranged == false)
    rearrange();

  if (filesystem::exists(to) == false)
    filesystem::create_directories(to);
  else
    SPDLOG_WARN("Overwriting...");

  write_edges_to_txt(to, knowses);

  ofstream out(to / fmt::format("vertices.txt"));
  for (size_t i = 0; i < persons.size(); i++) {
    out << fmt::format("{} {}\n", persons[i].id,
                       static_cast<int>(persons[i].community));
  }
}

TransferImporter::transfer TransferImporter::transfer::load_from_rabo(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {

  std::unordered_map<std::string, Account::RawID> raw_id_map;
  auto get_id = [&raw_id_map](std::string &v) {
    if (raw_id_map.contains(v) == false)
      raw_id_map[v] = raw_id_map.size();
    return raw_id_map.at(v);
  };

  transfer t;
  bool check[2] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "start_id") {
      t.raw_src_id = get_id(values[i]);
      check[0] = true;
    }
    if (titles[i] == "end_id") {
      t.raw_dst_id = get_id(values[i]);
      check[1] = true;
    }
  }
  if (!check[0] || !check[1]) {
    SPDLOG_ERROR("CSV parse failure, \ntitles: {}\n values: {}", titles,
                 values);
    exit(1);
  }
  return t;
}

TransferImporter::transfer TransferImporter::transfer::load_from_ldbc_fin(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  transfer t;
  bool check[2] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "fromId") {
      t.raw_src_id = std::stoull(values[i]);
      check[0] = true;
    }
    if (titles[i] == "toId") {
      t.raw_dst_id = std::stoull(values[i]);
      check[1] = true;
    }
  }
  if (!check[0] || !check[1]) {
    SPDLOG_ERROR("CSV parse failure, \ntitles: {}\n values: {}", titles,
                 values);
    exit(1);
  }
  return t;
}

void TransferImporter::load_from_rabo(std::filesystem::path csv_path) {
  transfers = load_from_csv_with_f<TransferImporter::transfer>(
      csv_path, ';', transfer::load_from_rabo);
  SPDLOG_INFO("Load {} Transfer from Rabo", transfers.size());
}

void TransferImporter::load_from_ldbc_fin(std::filesystem::path dir) {
  std::filesystem::path csv_path =
      dir / "snapshot" / "AccountTransferAccount.csv";
  transfers = load_from_csv_with_f<TransferImporter::transfer>(
      csv_path, '|', transfer::load_from_ldbc_fin);
  SPDLOG_INFO("Load {} Transfer from LDBC fin", transfers.size());
}

void TransferImporter::init_graph_schema() {
  graph_schema.vertex.push_back(Account::gen_schema());
  graph_schema.edge.push_back(transfer::gen_schema());
  graph_schema.graph_type = "Bank Transfer";

  for (auto &es : graph_schema.edge) {
    if (es.type == "transfer") {
      es.store_info.add_src_dst(std::nullopt);
      es.store_info.add_src_dst(pcnt);
      es.store_info.add_hilbert(std::nullopt);
      es.store_info.add_hilbert(pcnt);
    } else {
      throw std::runtime_error("Unkown edge type");
    }
  }
}

void TransferImporter::rearrange() {
  init_graph_schema();
  std::sort(accounts.begin(), accounts.end(), compare_by_raw_id<Account>);

  SPDLOG_INFO("Rearranging new id index");

  std::unordered_map<uint64_t, VID> account_raw_to_new =
      unify_vertex_and_collect_statistics(
          accounts, graph_schema.get_vertex_schema("Account"));

  map_edge_id(transfers, account_raw_to_new, account_raw_to_new);

  unify_all_edges(graph_schema, transfers);

  SPDLOG_INFO("Rearranging complete");
  rearranged = true;
}

void TransferImporter::output_vlgpm(filesystem::path to) {
  if (rearranged == false)
    rearrange();

  if (filesystem::exists(to) == false)
    filesystem::create_directories(to);
  else
    SPDLOG_WARN("Overwriting...");

  SPDLOG_INFO("Writing Bank Transfer to {}", to);

  SPDLOG_INFO("Writing Vertex");

  VERTEX_DISK_ARRAY_INIT(Account, id, VID, accounts);

  SPDLOG_INFO("Writing Edges");
  gen_and_write_all_edges(to, graph_schema, transfers);

  json schema = graph_schema;
  std::ofstream schema_file(to / "schema.json");
  schema_file << setw(4) << schema << std::endl;
  SPDLOG_INFO("Wrote graph schema to {}", to / "schema.json");
}

void TransferImporter::output_kuzu_csv(std::filesystem::path to) {
  if (rearranged == false)
    rearrange();

  if (filesystem::exists(to) == false)
    filesystem::create_directories(to);
  else
    SPDLOG_WARN("Overwriting...");

  SPDLOG_INFO("Writing Bank Transfer to {}", to);

  SPDLOG_INFO("Writing Vertex");

  write_vertices_to_csv(to, accounts);
  write_edges_to_csv(to, transfers);

  json schema = graph_schema;
  std::ofstream schema_file(to / "schema.json");
  schema_file << setw(4) << schema << std::endl;
  SPDLOG_INFO("Wrote graph schema to {}", to / "schema.json");
}

void TransferImporter::output_label_graph(std::filesystem::path to) {
  if (rearranged == false)
    rearrange();

  if (filesystem::exists(to) == false)
    filesystem::create_directories(to);
  else
    SPDLOG_WARN("Overwriting...");

  write_edges_to_txt(to, transfers);
}
