#pragma once
#include "common.hh"
#include "execution/flat_table.hh"
#include "execution/matrix_table.hh"
#include "profiler.hh"
#include "storage/graph_store.hh"
#include <atomic>
#include <memory>
#include <unordered_set>

class BigOperator;
/*

call run

*/

class PhysicalOperator {
  static std::atomic_size_t operator_count;

  Profiler prof;
  bool prof_merged = false;

  bool need_total_time = false;
  std::vector<std::unique_ptr<PhysicalOperator>> children;

  inline virtual std::string short_description() { return ""; };

  inline virtual bool before_running_child() {
    return true;
  } // return false, if we do not want to run child
  inline virtual bool after_child([[maybe_unused]] size_t i) {
    return true;
  } // return false, if we do not want to run remaining child
  virtual void run_internal();

protected:
  size_t id;
  size_t batch_size = 10000;
  static std::filesystem::path get_tmp_dir(PhysicalOperator *op);
  std::string name_content(std::string content);

  bool early_end = false; // true if this big operator did not execute child.

public:
  PhysicalOperator();
  virtual ~PhysicalOperator() = default;
  Profiler &get_prof();
  Profiler get_merged_prof();

  void run();

  void addChild(std::unique_ptr<PhysicalOperator> child);
  inline void set_total_time_on() { need_total_time = true; }
  static std::string get_physical_operator_name(PhysicalOperator *op);
  BigOperator *down_to_big_operator();

  static Graph *graph_store;
  static std::filesystem::path root_tmp_dir;

  friend BigOperator;
};

/*
before_running_child
before_execution
  child -> run
  after_child -> true
run_internal
children.clear

*/

class BigOperator;
using SharedBigOperator = std::shared_ptr<BigOperator>;

class BigOperator : public PhysicalOperator {

  bool before_running_child() override;
  // Running after all deps, before running child
  virtual bool before_execution();
  void run_internal() override;

protected:
  struct FlatVariableRef {
    std::shared_ptr<DiskArray<NoType>> data;
    ValueSchema vs;

    template <typename T> T *data_as() { return data->as_ptr<T>(); }
    inline size_t size() { return data->size(); }
  };

  std::vector<std::shared_ptr<BigOperator>> deps;
  std::vector<std::shared_ptr<Table>> dep_results;

  std::optional<FlatVariableRef>
  get_flat_variable_from_dep_results(std::string name);

  std::optional<FlatVariableRef>
  get_flat_variable_from_my_result(std::string name);

  // Function to write the node and its edges.
  bool doted = false;
  void dot(std::ofstream &file);

public:
  std::shared_ptr<Table> results;
  TableSchema results_schema;

  // whether to report output size
  bool record_output_size = false;

  BigOperator() = default;
  virtual ~BigOperator() = default;

  void addDependency(std::shared_ptr<BigOperator> dependency);
  virtual void finishAddingDependency(){};

  void addLastDependency(std::shared_ptr<BigOperator> dependency);

  void export_plan_dot(std::filesystem::path path);
  void export_plan_png(std::filesystem::path path);

  void test_run(std::string id, bool only_count);
};
