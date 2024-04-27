#pragma once

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "timer.hh"
#include "utils.hh"

class Profiler {

  size_t thread_cnt;
  std::vector<std::map<std::string, Timer>> timer_map;
  std::vector<std::map<std::string, size_t>> counter_map;
  std::vector<std::map<std::string, double>> double_map;

  std::map<std::string, Timer> global_timer_map;
  std::map<std::string, size_t> global_counter;
  std::map<std::string, double> global_double;

  size_t entry_max_length = 0, title_max_length = 0, time_max_length = 0;

public:
  inline Profiler(size_t thread_cnt) : thread_cnt(thread_cnt) {
    timer_map.resize(thread_cnt);
    counter_map.resize(thread_cnt);
  };

  inline void start_timer(size_t tid, const std::string &&name) {
    timer_map[tid][name].start();
  };

  inline void stop_timer(size_t tid, const std::string &&name) {
    timer_map[tid][name].stop();
  }

  inline Timer &get_global_timer(const std::string &&name) {
    return global_timer_map[name];
  }

  inline void start_global_timer(const std::string &&name) {
    // std::cout << "Start Timer on " << name << std::endl;
    global_timer_map[name].start();
  }
  inline void stop_global_timer(const std::string &&name) {
    global_timer_map[name].stop();
    // std::cout << global_timer_map[name].elapsedTime() << std::endl;
  }

  inline size_t &get_counter(size_t tid, const std::string &&name) {
    return counter_map[tid][name];
  }
  inline size_t &get_global_counter(const std::string &&name) {
    return global_counter[name];
  };
  inline auto get_global_counters() -> decltype(global_counter) & {
    return global_counter;
  }

  inline double &get_global_double(const std::string &&name) {
    return global_double[name];
  };

  inline void report() {
    entry_max_length = 0;
    for (auto &timer : global_timer_map) {
      entry_max_length = std::max(entry_max_length, timer.first.size());
    }
    for (auto &counter : global_counter) {
      entry_max_length = std::max(entry_max_length, counter.first.size());
    }
    for (auto &d : global_double) {
      entry_max_length = std::max(entry_max_length, d.first.size());
    }

    for (auto &timer_map : timer_map) {
      for (auto &timer : timer_map) {
        entry_max_length = std::max(entry_max_length, timer.first.size());
      }
    }
    for (auto &counter_map : counter_map) {
      for (auto &counter : counter_map) {
        entry_max_length = std::max(entry_max_length, counter.first.size());
      }
    }
    for (auto &d_map : double_map) {
      for (auto &d : d_map) {
        entry_max_length = std::max(entry_max_length, d.first.size());
      }
    }

    title_max_length = entry_max_length;
    time_max_length = 30;
    entry_max_length = title_max_length + time_max_length + 3;
    report_timer();
    report_counter();
    report_double();
    std::cout << fill_to_length("", entry_max_length, '-') << std::endl;
  }

  inline void report_double() {
    std::cout << fill_to_length("Global Double", entry_max_length, '-')
              << std::endl;
    for (auto &value : global_double) {
      std::cout << '|'
                << fill_to_length(value.first, title_max_length, ' ',
                                  PrintAlign::Left)
                << " "
                << fill_to_length(std::to_string(value.second), time_max_length,
                                  ' ', PrintAlign::Right)
                << '|' << std::endl;
    }
    std::cout << fill_to_length("Thread Double", entry_max_length, '-')
              << std::endl;

    std::map<std::string, double> double_sum;
    for (size_t i = 0; i < thread_cnt; i++) {
      for (auto &[key, timer] : timer_map[i]) {
        double_sum[key] += timer.elapsedNs();
      }
    }
    for (auto &[key, value] : double_sum) {
      std::cout << '|'
                << fill_to_length(key, title_max_length, ' ', PrintAlign::Left)
                << " "
                << fill_to_length(std::to_string(value / thread_cnt),
                                  time_max_length, ' ', PrintAlign::Right)
                << '|' << std::endl;
    }
  }
  inline void collect_counter() {
    for (auto &map : counter_map) {
      for (auto &[k, v] : map) {
        global_counter[k] += v;
      }
      map.clear();
    }
  }

  inline void report_timer() {
    std::cout << fill_to_length("Global Timer", entry_max_length, '-')
              << std::endl;
    for (auto &timer : global_timer_map) {
      std::cout
          << '|'
          << fill_to_length(timer.first, title_max_length, ' ',
                            PrintAlign::Left)
          << " "
          // << fill_to_length(timer.second.elapsedTime(), time_max_length,
          //                   ' ', PrintAlign::Right)
          << fill_to_length(fmt::format("{} ms", timer.second.elapsedMs()),
                            time_max_length, ' ', PrintAlign::Right)
          << '|' << std::endl;
    }
    std::cout << fill_to_length("Thread Timer", entry_max_length, '-')
              << std::endl;

    std::map<std::string, double> timer_sum;
    for (size_t i = 0; i < thread_cnt; i++) {
      for (auto &[key, timer] : timer_map[i]) {
        timer_sum[key] += timer.elapsedNs();
      }
    }
    for (auto &[key, time] : timer_sum) {
      std::cout << '|'
                << fill_to_length(key, title_max_length, ' ', PrintAlign::Left)
                << " "
                << fill_to_length(Timer::ns_to_string(time / thread_cnt),
                                  time_max_length, ' ', PrintAlign::Right)
                << '|' << std::endl;
    }
  }
  inline void report_counter() {
    std::cout << fill_to_length("Counter", entry_max_length, '-') << std::endl;
    std::map<std::string, size_t> counter_sum = global_counter;
    for (size_t i = 0; i < thread_cnt; i++) {
      for (auto &[key, counter] : counter_map[i]) {
        counter_sum[key] += counter;
      }
    }
    for (auto &[key, counter] : counter_sum) {
      std::cout << '|'
                << fill_to_length(key, title_max_length, ' ', PrintAlign::Left)
                << " "
                << fill_to_length(std::to_string(counter), time_max_length, ' ',
                                  PrintAlign::Right)
                << '|' << std::endl;
    }
  }

  void reset() {
    timer_map.clear();
    timer_map.resize(thread_cnt);
    double_map.clear();
    double_map.resize(thread_cnt);
    counter_map.clear();
    counter_map.resize(thread_cnt);

    global_double.clear();
    global_counter.clear();
    global_timer_map.clear();
  }

  inline void merge(Profiler &prof) {
    prof.collect_counter();
    for (auto &[key, timer] : prof.global_timer_map) {
      if (global_timer_map.count(key) == 0)
        global_timer_map[key] = timer;
      else
        global_timer_map[key].merge(timer);
    }
    for (auto &[key, counter] : prof.global_counter) {
      global_counter[key] += counter;
    }
    for (auto &[key, d] : prof.global_double) {
      global_double[key] += d;
    }
  }
};
