

#include <atomic>
#include <filesystem>
#include <omp.h>
#include <thread>

#include "operator/physical_operator.hh"

using namespace std;

atomic_size_t PhysicalOperator::operator_count(0);
Graph *PhysicalOperator::graph_store = nullptr;

filesystem::path PhysicalOperator::root_tmp_dir = "/tmp";

PhysicalOperator::PhysicalOperator()
    : prof(omp_get_max_threads()),
      id(operator_count.fetch_add(1, std::memory_order_relaxed)) {}

Profiler &PhysicalOperator::get_prof() { return prof; }

Profiler PhysicalOperator::get_merged_prof() {
  Profiler re(omp_get_max_threads());
  if (prof_merged)
    return re;
  prof_merged = true;

  // if(merged_id_set.contains(id))
  //   return re;
  // SPDLOG_WARN("Merging {}", get_physical_operator_name(this));

  for (auto &c : children) {
    auto tmp = c->get_merged_prof();
    re.merge(tmp);
  }
  re.merge(prof);
  // prof.reset();
  return re;
}

std::string PhysicalOperator::get_physical_operator_name(PhysicalOperator *op) {
  return string(typeid(*op).name()) + to_string_right(op->id, 2) + ":" +
         op->short_description();
}

filesystem::path PhysicalOperator::get_tmp_dir(PhysicalOperator *op) {
  auto re =
      root_tmp_dir / (string(typeid(*op).name()) + "-" + to_string(op->id));
  if (filesystem::exists(re) == false) {
    filesystem::create_directory(re);
  }
  return re;
}

string PhysicalOperator::name_content(string content) {
  return get_physical_operator_name(this) + ":" + content;
}

void PhysicalOperator::run_internal() {}

void PhysicalOperator::run() {
  if (need_total_time)
    prof.start_global_timer(get_physical_operator_name(this) + "[Total]");

  if (before_running_child() == true) {
    for (size_t i = 0; i < children.size(); i++) {
      SPDLOG_DEBUG("{} Run Child {}", get_physical_operator_name(this), i);
      // cout<< fmt::format("{} Run Child
      // {}",get_physical_operator_name(this),i) <<endl;
      children[i]->run();
      if (after_child(i) == false) {
        SPDLOG_INFO("{} After Child {} break", get_physical_operator_name(this),
                    i);
        break;
      }
    }
  } else {
    SPDLOG_INFO("{} Early Exit", get_physical_operator_name(this));
    early_end = true;
  }
  prof.start_global_timer(get_physical_operator_name(this));
  SPDLOG_INFO("{} Running internal", get_physical_operator_name(this));
  run_internal();
  prof.stop_global_timer(get_physical_operator_name(this));

  children.clear(); // gc

  if (need_total_time)
    prof.stop_global_timer(get_physical_operator_name(this) + "[Total]");
  auto bop = this->down_to_big_operator();
  if (bop != nullptr && bop->record_output_size) {
    // SPDLOG_WARN("Collecting Records Count {} : {}",
    //             get_physical_operator_name(this),
    //             bop->results->tuple_count());

    prof.get_global_counter(get_physical_operator_name(this) +
                            " Results Size") += bop->results->tuple_count();
    bop->record_output_size = false;
  }
}

void PhysicalOperator::addChild(unique_ptr<PhysicalOperator> child) {
  children.push_back(std::move(child));
}

BigOperator *PhysicalOperator::down_to_big_operator() {
  return dynamic_cast<BigOperator *>(this);
}

bool BigOperator::before_execution() {
  SPDLOG_DEBUG("{} Run default before_execution()",
               get_physical_operator_name(this));
  return true;
}

void BigOperator::run_internal() {
  results_schema = dep_results[0]->schema;
  results = std::move(dep_results[0]);
}

void BigOperator::addDependency(std::shared_ptr<BigOperator> dependency) {
  deps.push_back(std::move(dependency));
  dep_results.push_back(nullptr);
}

void BigOperator::addLastDependency(std::shared_ptr<BigOperator> dependency) {
  addDependency(std::move(dependency));
  finishAddingDependency();
}

bool BigOperator::before_running_child() {
  for (size_t i = 0; i < deps.size(); i++) {
    SPDLOG_DEBUG("{} Dependency {} start", get_physical_operator_name(this), i);
    deps[i]->run();
    dep_results[i] = deps[i]->results;
    unordered_set<size_t> merged_id_set;
    auto dp = deps[i]->get_merged_prof();
    prof.merge(dp);
    SPDLOG_INFO("{} Dependency {} finished, results size: {}, schema: {}",
                PhysicalOperator::get_physical_operator_name(this), i,
                dep_results[i]->tuple_count(), dep_results[i]->schema.columns);
    SPDLOG_DEBUG("{}", i);
  }
  SPDLOG_DEBUG("{} WHAT HAPPEND?", get_physical_operator_name(this));
  // for(auto& x:deps){
  //   SPDLOG_DEBUG("{} use count
  //   {}",PhysicalOperator::get_physical_operator_name(x.get()),
  //   x.use_count());
  // }

  // deps.clear(); // gc // TODO, I do not know why this clear will somtimes
  // cause the operator not working

  // SPDLOG_DEBUG("{} WHAT HAPPEND? 2",get_physical_operator_name(this));
  bool x = before_execution();
  SPDLOG_DEBUG("{} Before execution {}",
               PhysicalOperator::get_physical_operator_name(this), x);
  return x;
  // return before_execution();
}

std::optional<BigOperator::FlatVariableRef>
BigOperator::get_flat_variable_from_dep_results(std::string name) {
  for (const auto &dep_result : dep_results) {
    auto vs = dep_result->schema.get_schema(name);
    if (vs.has_value()) {
      size_t idx = vs->second;
      auto table = dep_result->down_to_flat_table();
      auto data = table->get_col_diskarray(idx);
      return FlatVariableRef{data, vs->first};
    }
  }
  return std::nullopt;
}

std::optional<BigOperator::FlatVariableRef>
BigOperator::get_flat_variable_from_my_result(std::string name) {
  auto x = results->schema.get_schema(name);
  if (x.has_value() == false) {
    return std::nullopt;
  }
  auto &[vs, idx] = x.value();
  auto data = results->down_to_flat_table()->get_col_diskarray(idx);
  return FlatVariableRef{data, vs};
}

void BigOperator::dot(std::ofstream &file) {
  if (doted)
    return;
  doted = true;
  // Write this node with a label that includes the name and schema
  file << "    node" << this->id << " [shape = box ,label=\""
       << PhysicalOperator::get_physical_operator_name(this) << "\\n"
       << fmt::format("{}", results_schema.columns) << "\"];\n";

  // Iterate through the children
  for (auto dep : deps) {
    // SPDLOG_INFO("dep count {}",dep.use_count());
    // Write an edge from this node to each child
    file << "    node" << this->id << " -> node" << dep->id << ";\n";
    // Recursively write the child node and its edges
    dep->dot(file);
  }
}

void BigOperator::export_plan_dot(std::filesystem::path path) {
  ofstream file(path);
  file << "digraph G {\n";
  dot(file);
  file << "}\n";
}

void BigOperator::export_plan_png(std::filesystem::path path) {
  if (filesystem::exists(path.parent_path()) == false) {
    filesystem::create_directories(path.parent_path());
  }
  auto dot_path = path;
  dot_path.replace_extension(".dot");
  export_plan_dot(dot_path);
  auto cmd = fmt::format("dot -Tpng {} -o {}", dot_path, path);
  assert(system(cmd.c_str()) == 0);
}

void BigOperator::test_run(std::string id, bool only_count) {
  auto root = this;
  root->export_plan_png(fmt::format("resource/{}/plan.png", id));
  // return 0;
  root->set_total_time_on();
  root->run();

  root->get_merged_prof();
  root->get_prof().report();
  SPDLOG_INFO("{}, {} tuples", root->results_schema.columns,
              root->results->tuple_count());

  SPDLOG_INFO(
      "QueryUsedTime: {}",
      root->get_prof()
          .get_global_timer(PhysicalOperator::get_physical_operator_name(root) +
                            "[Total]")
          .elapsedMs());
  if (only_count) {
    SPDLOG_INFO("QueryResults: {}",
                root->results->down_to_flat_table()->tuple_to_string(0));
  }
}