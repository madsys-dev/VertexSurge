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

// Person
LDBCFinBenchImporter::Person LDBCFinBenchImporter::Person::load_from_ldbc_fin(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  Person p;
  bool check[1] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "personId") {
      p.raw_id = std::stoull(values[i]);
      check[0] = true;
    }
  }
  for (size_t i = 0; i < 1; i++) {
    if (!check[i]) {
      SPDLOG_ERROR("Person missing field {}", i);
      throw std::runtime_error("Person missing field");
    }
  }
  return p;
}

// Account
LDBCFinBenchImporter::Account LDBCFinBenchImporter::Account::load_from_ldbc_fin(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  Account a;
  bool check[1] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "accountId") {
      a.raw_id = std::stoull(values[i]);
      check[0] = true;
    }
  }
  for (size_t i = 0; i < 1; i++) {
    if (!check[i]) {
      SPDLOG_ERROR("Account missing field {}", i);
      throw std::runtime_error("Account missing field");
    }
  }
  return a;
}

// Medium
LDBCFinBenchImporter::Medium LDBCFinBenchImporter::Medium::load_from_ldbc_fin(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  Medium m;
  bool check[2] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "mediumId") {
      m.raw_id = std::stoull(values[i]);
      check[0] = true;
    }
    if (titles[i] == "isBlocked") {
      m.isBlocked = values[i] == "false" ? 0 : 1;
      check[1] = true;
    }
  }
  for (size_t i = 0; i < 2; i++) {
    if (!check[i]) {
      SPDLOG_ERROR("Medium missing field {}", i);
      throw std::runtime_error("Medium missing field");
    }
  }
  return m;
}

// Loan
LDBCFinBenchImporter::Loan LDBCFinBenchImporter::Loan::load_from_ldbc_fin(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  Loan l;
  bool check[2] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "loanId") {
      l.raw_id = std::stoull(values[i]);
      check[0] = true;
    }
    if (titles[i] == "balance") {
      l.balance = std::stoll(values[i]);
      check[1] = true;
    }
  }
  for (size_t i = 0; i < 2; i++) {
    if (!check[i]) {
      SPDLOG_ERROR("Loan missing field {}", i);
      throw std::runtime_error("Loan missing field");
    }
  }
  return l;
}

// Own
LDBCFinBenchImporter::own LDBCFinBenchImporter::own::load_from_ldbc_fin(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  own o;
  bool check[2] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "personId") {
      o.raw_src_id = std::stoull(values[i]);
      check[0] = true;
    }
    if (titles[i] == "accountId") {
      o.raw_dst_id = std::stoull(values[i]);
      check[1] = true;
    }
  }
  for (size_t i = 0; i < 2; i++) {
    if (!check[i]) {
      SPDLOG_ERROR("Own missing field {}", i);
      throw std::runtime_error("Own missing field");
    }
  }
  return o;
}

// Transfer
LDBCFinBenchImporter::transfer
LDBCFinBenchImporter::transfer::load_from_ldbc_fin(
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
  for (size_t i = 0; i < 2; i++) {
    if (!check[i]) {
      SPDLOG_ERROR("Transfer missing field {}", i);
      throw std::runtime_error("Transfer missing field");
    }
  }
  return t;
}

// Withdraw
LDBCFinBenchImporter::withdraw
LDBCFinBenchImporter::withdraw::load_from_ldbc_fin(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  withdraw w;
  bool check[2] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "fromId") {
      w.raw_src_id = std::stoull(values[i]);
      check[0] = true;
    }
    if (titles[i] == "toId") {
      w.raw_dst_id = std::stoull(values[i]);
      check[1] = true;
    }
  }
  for (size_t i = 0; i < 2; i++) {
    if (!check[i]) {
      SPDLOG_ERROR("Withdraw missing field {}", i);
      throw std::runtime_error("Withdraw missing field");
    }
  }
  return w;
}

// SignIn
LDBCFinBenchImporter::signIn LDBCFinBenchImporter::signIn::load_from_ldbc_fin(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  signIn s;
  bool check[2] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "mediumId") {
      s.raw_src_id = std::stoull(values[i]);
      check[0] = true;
    }
    if (titles[i] == "accountId") {
      s.raw_dst_id = std::stoull(values[i]);
      check[1] = true;
    }
  }
  for (size_t i = 0; i < 2; i++) {
    if (!check[i]) {
      SPDLOG_ERROR("SignIn missing field {}", i);
      throw std::runtime_error("SignIn missing field");
    }
  }
  return s;
}

// deposit
LDBCFinBenchImporter::deposit LDBCFinBenchImporter::deposit::load_from_ldbc_fin(
    const std::vector<std::string> &titles, std::vector<std::string> &&values) {
  deposit d;
  bool check[2] = {};
  for (size_t i = 0; i < titles.size(); i++) {
    if (titles[i] == "loanId") {
      d.raw_src_id = std::stoull(values[i]);
      check[0] = true;
    }
    if (titles[i] == "accountId") {
      d.raw_dst_id = std::stoull(values[i]);
      check[1] = true;
    }
  }
  for (size_t i = 0; i < 2; i++) {
    if (!check[i]) {
      SPDLOG_ERROR("Deposit missing field {}", i);
      throw std::runtime_error("Deposit missing field");
    }
  }
  return d;
}

LDBCFinBenchImporter::tcr1_param
LDBCFinBenchImporter::tcr1_param::load_from_ldbc_fin(
    const std::vector<std::string> &, std::vector<std::string> &&values) {
  assert(values.size() == 5);
  tcr1_param t;
  t.raw_id = std::stoull(values[0]);
  return t;
}

LDBCFinBenchImporter::tcr2_param
LDBCFinBenchImporter::tcr2_param::load_from_ldbc_fin(
    const std::vector<std::string> &, std::vector<std::string> &&values) {
  assert(values.size() == 5);
  tcr2_param t;
  t.raw_id = std::stoull(values[0]);
  return t;
}

LDBCFinBenchImporter ::tcr3_param
LDBCFinBenchImporter::tcr3_param::load_from_ldbc_fin(
    const std::vector<std::string> &, std::vector<std::string> &&values) {
  assert(values.size() == 4);
  tcr3_param t;
  t.raw_id1 = std::stoull(values[0]);
  t.raw_id2 = std::stoull(values[1]);
  return t;
}

LDBCFinBenchImporter::tcr6_param
LDBCFinBenchImporter::tcr6_param::load_from_ldbc_fin(
    const std::vector<std::string> &, std::vector<std::string> &&values) {
  assert(values.size() == 7);
  tcr6_param t;
  t.raw_id = std::stoull(values[0]);
  return t;
}

LDBCFinBenchImporter::tcr8_param
LDBCFinBenchImporter::tcr8_param::load_from_ldbc_fin(
    const std::vector<std::string> &, std::vector<std::string> &&values) {
  assert(values.size() == 6);
  tcr8_param t;
  t.raw_id = std::stoull(values[0]);
  return t;
}

void LDBCFinBenchImporter::load_from_ldbc_fin(std::filesystem::path dir) {
  SPDLOG_INFO("Loading LDBC_FIN from {}", dir);

  persons = load_from_csv_with_f<LDBCFinBenchImporter::Person>(
      dir / "snapshot" / "Person.csv", '|', Person::load_from_ldbc_fin);
  accounts = load_from_csv_with_f<LDBCFinBenchImporter::Account>(
      dir / "snapshot" / "Account.csv", '|', Account::load_from_ldbc_fin);
  mediums = load_from_csv_with_f<LDBCFinBenchImporter::Medium>(
      dir / "snapshot" / "Medium.csv", '|', Medium::load_from_ldbc_fin);
  loans = load_from_csv_with_f<LDBCFinBenchImporter::Loan>(
      dir / "snapshot" / "Loan.csv", '|', Loan::load_from_ldbc_fin);

  owns = load_from_csv_with_f<LDBCFinBenchImporter::own>(
      dir / "snapshot" / "PersonOwnAccount.csv", '|', own::load_from_ldbc_fin);
  transfers = load_from_csv_with_f<LDBCFinBenchImporter::transfer>(
      dir / "snapshot" / "AccountTransferAccount.csv", '|',
      transfer::load_from_ldbc_fin);
  withdraws = load_from_csv_with_f<LDBCFinBenchImporter::withdraw>(
      dir / "snapshot" / "AccountWithdrawAccount.csv", '|',
      withdraw::load_from_ldbc_fin);
  signIns = load_from_csv_with_f<LDBCFinBenchImporter::signIn>(
      dir / "snapshot" / "MediumSignInAccount.csv", '|',
      signIn::load_from_ldbc_fin);
  deposits = load_from_csv_with_f<LDBCFinBenchImporter::deposit>(
      dir / "snapshot" / "LoanDepositAccount.csv", '|',
      deposit::load_from_ldbc_fin);

  SPDLOG_INFO("Load {} persons", persons.size());
  SPDLOG_INFO("Load {} accounts", accounts.size());
  SPDLOG_INFO("Load {} mediums", mediums.size());
  SPDLOG_INFO("Load {} loans", loans.size());
  SPDLOG_INFO("Load {} person owns account", owns.size());
  SPDLOG_INFO("Load {} account transfers account", transfers.size());
  SPDLOG_INFO("Load {} account withdraws account", withdraws.size());
  SPDLOG_INFO("Load {} medium sign in account", signIns.size());
  SPDLOG_INFO("Load {} loan deposit account", deposits.size());
  SPDLOG_INFO("LDBC FinBench load complete");
}

void LDBCFinBenchImporter::load_params_from_ldbc_fin(
    std::filesystem::path dir) {
  SPDLOG_INFO("Loading LDBC_FIN params from {}", dir);

  tcr1_params = load_from_csv_with_f<LDBCFinBenchImporter::tcr1_param>(
      dir / "complex_1_param.csv", '|', tcr1_param::load_from_ldbc_fin);
  tcr2_params = load_from_csv_with_f<LDBCFinBenchImporter::tcr2_param>(
      dir / "complex_2_param.csv", '|', tcr2_param::load_from_ldbc_fin);
  tcr3_params = load_from_csv_with_f<LDBCFinBenchImporter::tcr3_param>(
      dir / "complex_3_param.csv", '|', tcr3_param::load_from_ldbc_fin);
  tcr6_params = load_from_csv_with_f<LDBCFinBenchImporter::tcr6_param>(
      dir / "complex_6_param.csv", '|', tcr6_param::load_from_ldbc_fin);
  tcr8_params = load_from_csv_with_f<LDBCFinBenchImporter::tcr8_param>(
      dir / "complex_8_param.csv", '|', tcr8_param::load_from_ldbc_fin);

  SPDLOG_INFO("Load {} TCR1 params", tcr1_params.size());
  SPDLOG_INFO("Load {} TCR2 params", tcr2_params.size());
  SPDLOG_INFO("Load {} TCR3 params", tcr3_params.size());
  SPDLOG_INFO("Load {} TCR6 params", tcr6_params.size());
  SPDLOG_INFO("Load {} TCR8 params", tcr8_params.size());
  SPDLOG_INFO("LDBC FinBench params load complete");
}

void LDBCFinBenchImporter::init_graph_schema() {
  graph_schema.vertex.push_back(Person::gen_schema());
  graph_schema.vertex.push_back(Account::gen_schema());
  graph_schema.vertex.push_back(Medium::gen_schema());
  graph_schema.vertex.push_back(Loan::gen_schema());

  graph_schema.edge.push_back(own::gen_schema());
  graph_schema.edge.push_back(transfer::gen_schema());
  graph_schema.edge.push_back(withdraw::gen_schema());
  graph_schema.edge.push_back(signIn::gen_schema());
  graph_schema.edge.push_back(deposit::gen_schema());

  graph_schema.graph_name = "LDBC-FinBench";
  graph_schema.graph_type = "LDBC-FinBench";

  for (auto &es : graph_schema.edge) {
    // es.store_info = edge_storage_info;
    if (es.type == "own") {
      es.store_info.add_src_dst(std::nullopt);
      es.store_info.add_src_dst(pcnt);
    } else if (es.type == "transfer") {
      es.store_info.add_src_dst(std::nullopt);
      es.store_info.add_src_dst(pcnt);
      es.store_info.add_hilbert(std::nullopt);
      es.store_info.add_hilbert(pcnt);

    } else if (es.type == "withdraw") {
      es.store_info.add_src_dst(std::nullopt);
      es.store_info.add_src_dst(pcnt);
      es.store_info.add_hilbert(std::nullopt);
      es.store_info.add_hilbert(pcnt);
    } else if (es.type == "signIn") {
      es.store_info.add_src_dst(std::nullopt);
      es.store_info.add_src_dst(pcnt);
    } else if (es.type == "deposit") {
      es.store_info.add_src_dst(std::nullopt);
      es.store_info.add_src_dst(pcnt);
      es.store_info.add_hilbert(std::nullopt);
      es.store_info.add_hilbert(pcnt);
    } else {
      throw std::runtime_error("Unknown edge type");
    }
  }
}

void LDBCFinBenchImporter::rearrange() {
  init_graph_schema();

  std::sort(persons.begin(), persons.end(), compare_by_raw_id<Person>);
  std::sort(accounts.begin(), accounts.end(), compare_by_raw_id<Account>);
  std::sort(mediums.begin(), mediums.end(), compare_by_raw_id<Medium>);
  std::sort(loans.begin(), loans.end(), compare_by_raw_id<Loan>);

  SPDLOG_INFO("Rearranging new id index");
  std::unordered_map<uint64_t, VID> person_raw_to_new =
      unify_vertex_and_collect_statistics(
          persons, graph_schema.get_vertex_schema("Person"));
  std::unordered_map<uint64_t, VID> account_raw_to_new =
      unify_vertex_and_collect_statistics(
          accounts, graph_schema.get_vertex_schema("Account"));
  std::unordered_map<uint64_t, VID> medium_raw_to_new =
      unify_vertex_and_collect_statistics(
          mediums, graph_schema.get_vertex_schema("Medium"));
  std::unordered_map<uint64_t, VID> loan_raw_to_new =
      unify_vertex_and_collect_statistics(
          loans, graph_schema.get_vertex_schema("Loan"));

  map_edge_id(owns, person_raw_to_new, account_raw_to_new);
  map_edge_id(transfers, account_raw_to_new, account_raw_to_new);
  map_edge_id(withdraws, account_raw_to_new, account_raw_to_new);
  map_edge_id(signIns, medium_raw_to_new, account_raw_to_new);
  map_edge_id(deposits, loan_raw_to_new, account_raw_to_new);

  map_params_id(tcr1_params, account_raw_to_new);
  map_params_id(tcr2_params, person_raw_to_new);
  map_params_2_id(tcr3_params, account_raw_to_new);
  map_params_id(tcr6_params, account_raw_to_new);
  map_params_id(tcr8_params, loan_raw_to_new);

  unify_all_edges(graph_schema, owns, transfers, withdraws, signIns, deposits);

  SPDLOG_INFO("Rearranging complete");
  rearranged = true;
}

void LDBCFinBenchImporter::output_vlgpm(std::filesystem::path to) {
  if (rearranged == false) {
    rearrange();
  }
  if (filesystem::exists(to) == false) {
    filesystem::create_directories(to);
  } else {
    SPDLOG_WARN("Overwriting...");
  }

  SPDLOG_INFO("Writing LDBC FinBench to {}", to);

  SPDLOG_INFO("Writing Vertex");

  VERTEX_DISK_ARRAY_INIT(Person, id, VID, persons);
  VERTEX_DISK_ARRAY_INIT(Account, id, VID, accounts);
  VERTEX_DISK_ARRAY_INIT(Medium, id, VID, accounts);
  VERTEX_DISK_ARRAY_INIT(Medium, isBlocked, TagValue8, mediums);
  VERTEX_DISK_ARRAY_INIT(Loan, id, VID, loans);
  VERTEX_DISK_ARRAY_INIT(Loan, balance, int64_t, loans);

  SPDLOG_INFO("Writing Edges");
  gen_and_write_all_edges(to, graph_schema, owns, transfers, withdraws, signIns,
                          deposits);

  std::filesystem::create_directory(to / "params");
  write_params_to_csv(to / "params", tcr1_params, tcr2_params, tcr3_params,
                      tcr6_params, tcr8_params);

  json schema = graph_schema;
  std::ofstream schema_file(to / "schema.json");
  schema_file << setw(4) << schema << std::endl;
  SPDLOG_INFO("Wrote graph schema to {}", to / "schema.json");
}

void LDBCFinBenchImporter::output_kuzu_csv(std::filesystem::path to) {
  if (rearranged == false) {
    rearrange();
  }

  if (filesystem::exists(to) == false) {
    filesystem::create_directories(to);
  } else {
    SPDLOG_WARN("Overwriting...");
  }

  SPDLOG_INFO("Writing LDBC FinBench in CSV to {}", to);
  write_vertices_to_csv(to, persons, accounts, mediums, loans);
  write_edges_to_csv(to, owns, transfers, withdraws, signIns, deposits);
  std::filesystem::create_directory(to / "params");
  write_params_to_csv(to / "params", tcr1_params, tcr2_params, tcr3_params,
                      tcr6_params, tcr8_params);

  json schema = graph_schema;
  std::ofstream schema_file(to / "schema.json");
  schema_file << setw(4) << schema << std::endl;
  SPDLOG_INFO("Wrote graph schema to {}", to / "schema.json");
}

void LDBCFinBenchImporter::output_label_graph(std::filesystem::path to) {
  if (rearranged == false)
    rearrange();

  if (filesystem::exists(to) == false)
    filesystem::create_directories(to);
  else
    SPDLOG_WARN("Overwriting...");

  SPDLOG_INFO(
      "Writing LDBC FinBench in Label Graph to {}, Only write Account transfer",
      to);
  write_edges_to_txt(to, transfers);
}
