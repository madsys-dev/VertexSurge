#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common.hh"
#include "meta/schema.hh"
#include "meta/type.hh"
#include "storage/bitmap.hh"
#include "storage/concurrent_disk_array.hh"
#include "storage/disk_array.hh"
#include "tuple.hh"

// impl libfmt for TableType
template <> struct fmt::formatter<TableType> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(TableType t, FormatContext &ctx) {
    switch (t) {
    case TableType::Flat:
      return format_to(ctx.out(), "Flat");
    case TableType::CSR:
      return format_to(ctx.out(), "CSR");
    case TableType::VarRowMatrix:
      return format_to(ctx.out(), "VarRowMatrix");
    case TableType::ColMainMatrix:
      return format_to(ctx.out(), "ColMainMatrix");
    case TableType::Factorized:
      return format_to(ctx.out(), "Factorized");
    case TableType::MultipleColMainMatrix:
      return format_to(ctx.out(), "MultipleColMainMatrix");
    case TableType::MultipleVarRowMatrix:
      return format_to(ctx.out(), "MultipleVarRowMatrix");
    case TableType::Multiple:
      return format_to(ctx.out(), "Multiple");
    }
    assert(0);
  }
};

class FlatTable;

class Table {
public:
  TableType type;
  TableSchema schema;
  Table(TableSchema schema, TableType type);

  FlatTable *down_to_flat_table();
  VarRowMatrixTable *down_to_var_row_matrix_table();
  ColMainMatrixTable *down_to_col_main_matrix_table();
  template <typename T> T *down_to_table() {
    if (std::is_same<T, FlatTable>::value ||
        std::is_same<T, VarRowMatrixTable>::value ||
        std::is_same<T, MultipleMatrixTable>::value ||
        std::is_same<T, MultipleTable>::value ||
        std::is_same<T, ColMainMatrixTable>::value) {
      return dynamic_cast<T *>(this);
    } else {
      assert(0);
    }
  }

  // Read
  virtual size_t tuple_count() = 0;
  virtual void *get_raw_ptr(size_t i, size_t j) = 0;
  virtual void dump_to_json(std::filesystem::path path) = 0;
  virtual void print(std::optional<size_t> limit) = 0;
};

class MultipleTable : public Table {
  ValueSchema key_schema;

  std::vector<TupleValue> keys;
  std::vector<std::shared_ptr<Table>> tables;

public:
  TableSchema subtable_schema;

  MultipleTable(TableSchema schema, VariableName key_name);
  void add_table(void *key, std::shared_ptr<Table> table);
  std::shared_ptr<Table> new_empty_subflat(bool is_concurrent);

  size_t tuple_count() override;
  void *get_raw_ptr(size_t i, size_t j) override;
  void dump_to_json(std::filesystem::path path) override;
  void print(std::optional<size_t> limit) override;
};