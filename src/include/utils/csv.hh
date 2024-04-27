#pragma once

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "common.hh"

template <class T>
concept ExportableToCSV = requires(T t) {
  { T::csv_header() } -> std::same_as<std::string>;
  { t.to_csv_line() } -> std::same_as<std::string>;
};

template <class T>
concept LoadableFromCSV = requires(T t, const std::vector<std::string> &titles,
                                   std::vector<std::string> &&values) {
  { T::load_from_tvs(titles, std::move(values)) } -> std::same_as<T>;
};

template <typename T>
concept BuildableFromKVs = requires(std::vector<std::string> titles,
                                    std::vector<std::string> values) {
  {T(titles, std::move(values))};
};

inline std::vector<std::string> split(const std::string &input,
                                      char delimiter) {
  std::vector<std::string> re;
  size_t last_i = 0;
  for (size_t i = 0; i < input.size(); i++) {
    if (input[i] == delimiter) {
      re.push_back(input.substr(last_i, i - last_i));
      last_i = i + 1;
    }
  }
  re.push_back(input.substr(last_i, input.size() - last_i));
  return re;
}

template <typename T, typename F>
std::vector<T> load_from_csv_with_f(const std::filesystem::path &csv_path,
                                    char delimiter, F &f) {

  std::ifstream file(csv_path);
  if (file.is_open() == false) {
    SPDLOG_ERROR("Open csv file {} failed.", csv_path);
    exit(0);
  }

  std::vector<T> re;
  std::string line;
  getline(file, line);

  std::vector<std::string> titles = split(std::move(line), delimiter);
  bool check_value_count = true;
  if (titles[0] == "...") {
    check_value_count = false;
  }

  while (getline(file, line)) {

    auto values = split(std::move(line), delimiter);
    if (check_value_count)
      assert(titles.size() == values.size());
    re.emplace_back(f(titles, std::move(values)));
  }
  return re;
}

template <typename T, typename F>
std::vector<T>
load_from_multiple_csv_with_f(std::vector<std::filesystem::path> paths,
                              char delimiter, F &f) {
  std::vector<T> re;
  for (const auto &csv_file : paths) {
    auto p = load_from_csv_with_f<T>(csv_file, delimiter, f);
    re.insert(re.end(), p.begin(), p.end());
  }
  return re;
}

template <LoadableFromCSV T>
std::vector<T> load_from_csv(std::filesystem::path csv_path, char delimiter) {
  std::ifstream file(csv_path);
  if (file.is_open() == false) {
    SPDLOG_ERROR("Open csv file {} failed.", csv_path);
    exit(0);
  }

  std::vector<T> re;
  std::string line;
  getline(file, line);

  std::vector<std::string> titles = split(std::move(line), delimiter);
  bool check_value_count = true;
  if (titles[0] == "...") {
    check_value_count = false;
  }

  while (getline(file, line)) {

    auto values = split(std::move(line), delimiter);
    if (check_value_count)
      assert(titles.size() == values.size());
    re.emplace_back(T::load_from_tvs(titles, std::move(values)));
  }
  SPDLOG_INFO("Load {} records from {}", re.size(), csv_path.string());
  return re;
}

inline std::vector<std::filesystem::path>
get_csr_filepath_in(const std::filesystem::path &dir) {
  std::vector<std::filesystem::path> csvFiles;
  if (!std::filesystem::exists(dir) || !std::filesystem::is_directory(dir)) {
    throw std::runtime_error("Invalid directory");
  }
  for (const auto &entry : std::filesystem::directory_iterator(dir)) {
    if (entry.path().extension() == ".csv") {
      csvFiles.push_back(entry.path().string());
    }
  }
  return csvFiles;
}

template <ExportableToCSV T>
void save_to_csv(const std::filesystem::path &csv_path, std::vector<T> &data,
                 bool output_title = true) {
  std::ofstream file(csv_path);
  if (file.is_open() == false) {
    SPDLOG_ERROR("Open csv file {} failed.", csv_path);
    exit(0);
  }
  if (output_title)
    file << T::csv_header() << std::endl;
  for (auto &d : data) {
    file << d.to_csv_line() << std::endl;
  }
}
