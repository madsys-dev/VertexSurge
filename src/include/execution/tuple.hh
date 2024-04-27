#pragma once

#include <optional>
#include <variant>
#include <xxhash.h>

#include "meta/type.hh"

enum class TableType {
  Flat,
  CSR,
  VarRowMatrix,
  ColMainMatrix,
  Factorized,
  MultipleVarRowMatrix,
  MultipleColMainMatrix,
  Multiple
};

bool isMatrixTable(TableType type);

bool isMultipleMatrixTable(TableType type);

TableType TypeOfMultipleMatrix(TableType type);

TableType MultipleTypeOfMatrix(TableType type);

union TupleValue {
  VID vid;
  VIDPair vidp;
  Offset offset;
  Int32 i32;
  Int64 i64;
  ShortString shortstring;
  TagValue8 tagvalue8;
  TagValue16 tagvalue16;

  TupleValue() { memset(this, 0, sizeof(TupleValue)); };
  TupleValue(Int32 x) : TupleValue() { i32 = x; }
  TupleValue(std::initializer_list<Int32>);

  std::string to_string(Type t) const;
  void *value_ptr(Type type);

  static std::size_t hash_value(const TupleValue &tv, Type t);
  static TupleValue construct_tuple_value(Type t, void *from);
  static bool eq(const TupleValue &a, const TupleValue &b, Type t);

  static TupleValue zero(Type t);
  static TupleValue add(TupleValue a, TupleValue b, Type t);
};
constexpr size_t TVSize = sizeof(TupleValue);
static_assert(TVSize == 16);

// We do not know type of value, so set type to Type before hash, lock is used
// to protect type.
// TODO: change this to runtime hash
struct TupleValueHash {
  inline static std::mutex lock;
  inline static Type type;
  static void set_type([[maybe_unused]] Type type) {
    // assert(TupleValueHash::lock.try_lock());
    // TupleValueHash::type = type;
  }

  static void release_type() {
    // lock.unlock();
  }

  static size_t hash(const TupleValue &tv) {
    // return TupleValue::hash_value(tv, type);
    return XXH64(&tv, sizeof(TupleValue), 0);
  }
  static bool equal(const TupleValue &a, const TupleValue &b) {
    return TupleValue::eq(a, b, type);
  }
};
struct TupleValueRuntimeHashForTBB {
  Type type;

  TupleValueRuntimeHashForTBB(Type type) : type(type) {}
  size_t hash(const TupleValue &tv) const {
    // return TupleValue::hash_value(tv, type);
    return XXH64(&tv, sizeof(TupleValue), 0);
  }
  bool equal(const TupleValue &a, const TupleValue &b) const {
    return TupleValue::eq(a, b, type);
  }
};

struct TupleValueRuntimeHash {
  static const size_t BUCKET_SIZE = 256;
  Type type;
  TupleValueRuntimeHash(Type type) : type(type){};
  size_t operator()(const TupleValue &tv) const {
    return XXH64(&tv, sizeof(TupleValue), 0);
  }
};

// KeyEqual
struct TupleValueKeyEqual {
  Type type;
  TupleValueKeyEqual(Type type) : type(type) {}
  bool operator()(const TupleValue &lhs, const TupleValue &rhs) const {
    return TupleValue::eq(lhs, rhs, type);
  }
};

// NoType tuple

class Tuple {
  static constexpr size_t InlineTupleValueCount = 14;
  TupleValue inlined[InlineTupleValueCount];
  uint8_t inlined_count;
  std::vector<TupleValue> overflow;

public:
  Tuple() = default;
  Tuple(size_t size);
  // initialize list
  Tuple(std::initializer_list<TupleValue> list);

  size_t size() const;
  void push_back(TupleValue v);
  TupleValue &operator[](size_t i);

} __attribute__((aligned(128)));
constexpr size_t TupleSize = sizeof(Tuple);
static_assert(sizeof(Tuple) == 256);

struct TableSchema {
  std::vector<ValueSchema> columns;

  std::optional<std::pair<ValueSchema, size_t>>
  get_schema(std::string name) const;

  bool has_duplicate_variable();

  std::vector<size_t> map_to(const TableSchema &other) const;

  void add_non_duplicated_variable(ValueSchema vs);
  void remove_variable(ValueSchema vs);

  void merge_schema(const TableSchema &other);

  static TableSchema join_schema(const TableSchema &a, const TableSchema &b,
                                 std::optional<std::string> key);

  inline bool operator==(TableSchema &b) { return columns == b.columns; }

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(TableSchema, columns);
};

// impl libfmt for TableSchema
template <> struct fmt::formatter<TableSchema> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const TableSchema &p, FormatContext &ctx) -> decltype(ctx.out()) {
    return format_to(ctx.out(), "{}", p.columns);
  }
};

template <const size_t N> class FixedTuple {

public:
  TupleValue data[N];
  class Hash {
  public:
    static size_t hash(const FixedTuple<N> &ft) {
      uint64_t seed = 0;
      seed = XXH64(ft.data, sizeof(TupleValue) * N, seed);
      return seed;
    }
    size_t operator()(const FixedTuple<N> &ft) const { return hash(ft); }
  };

  class Equal {
  public:
    static bool equal(const FixedTuple<N> &a, const FixedTuple<N> &b) {
      return std::memcmp(a.data, b.data, sizeof(TupleValue) * N) == 0;
    }
    bool operator()(const FixedTuple<N> &a, const FixedTuple<N> &b) const {
      return equal(a, b);
    }
  };

  FixedTuple() = default;
  TupleValue &operator[](size_t i) { return data[i]; }

  std::string to_string(TableSchema &schema) {
    std::string re;
    for (size_t i = 0; i < N; i++) {
      re += data[i].to_string(schema.columns[i].type);
      re += i == N - 1 ? "" : ",";
    }
    return re;
  }
};

// // impl libfmt for TableSchema
// template<> struct fmt::formatter<TableSchema> {
//   constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

//   template<typename FormatContext>
//   auto format(const TableSchema& p, FormatContext& ctx) ->
//   decltype(ctx.out()){
//     return format_to(ctx.out(), "{},{}", p.type,p.columns);
//   }
// };

struct DiskTupleRef {
  static constexpr size_t InlinedRefCount = 8;
  void *data[InlinedRefCount];

  void *&operator[](const size_t i) { return data[i]; }

  void *get(const Type type, size_t i, size_t j);

  void *get(const TableSchema &schema, size_t i, size_t j);

  template <typename T>
  T *get_as(const TableSchema &schema, size_t i, size_t j) {
    return reinterpret_cast<T *>(get(schema, i, j));
  }

  template <typename T> T *get_as(const Type type, size_t i, size_t j) {
    return reinterpret_cast<T *>(get(type, i, j));
  }

  TupleValue copy_out_at(const TableSchema &schema, const size_t i,
                         const size_t j);

  void inc(const std::vector<ValueSchema> &schema, size_t x = 1);

  DiskTupleRef offset(const TableSchema &schema, size_t x) const;

  std::string to_string(const TableSchema &schema) const;

} __attribute__((aligned(128)));
