#pragma once
#include "common.hh"
#include "utils/utils.hh"

#include <array>
#include <random>

const size_t ShortStringMax = 16;

struct VIDPair {
  VID first;
  VID second;
};

// Basic Type
using VID = VID;
using VIDPair = VIDPair;
using Offset = Offset;
using Int32 = int32_t;
using Int64 = int64_t;
using ShortString = std::array<char, ShortStringMax>;
using TagValue8 = uint8_t;
using TagValue16 = uint16_t;

inline ShortString short_string_from_string(const std::string &s) {
  size_t len = std::min(s.size() + 1, ShortStringMax);
  ShortString re = {};
  memcpy(re.begin(), s.data(), len);
  return re;
}

inline std::string short_string_to_string(const ShortString &s) {

  std::string re;
  for (const auto c : s) {
    if (c == 0)
      break;
    re.push_back(c);
  }
  return re;
}
inline std::string short_string_to_string(void *s) {
  char *x = reinterpret_cast<char *>(s);
  std::string re;
  while (*x != '\0' && re.size() < ShortStringMax) {
    re.push_back(*x);
    x++;
  }
  return re;
}

inline ShortString random_letter_short_string(std::mt19937 &gen) {
  ShortString re;
  std::uniform_int_distribution<char> dis(65, 91);

  for (auto &c : re) {
    c = dis(gen);
  }
  return re;
}

enum class Type {
  VID,
  VIDPair,
  Offset,
  Int32,
  Int64,
  ShortString,
  TagValue8,
  TagValue16,
};

inline void swap_with_type(Type type, void *a, void *b) {
  switch (type) {
  case Type::VID:
    std::swap(*reinterpret_cast<VID *>(a), *reinterpret_cast<VID *>(b));
    break;
  case Type::VIDPair:
    std::swap(*reinterpret_cast<VIDPair *>(a), *reinterpret_cast<VIDPair *>(b));
    break;
  case Type::Offset:
    std::swap(*reinterpret_cast<Offset *>(a), *reinterpret_cast<Offset *>(b));
    break;
  case Type::Int32:
    std::swap(*reinterpret_cast<Int32 *>(a), *reinterpret_cast<Int32 *>(b));
    break;
  case Type::Int64:
    std::swap(*reinterpret_cast<Int64 *>(a), *reinterpret_cast<Int64 *>(b));
    break;
  case Type::ShortString:
    std::swap(*reinterpret_cast<ShortString *>(a),
              *reinterpret_cast<ShortString *>(b));
    break;
  case Type::TagValue8:
    std::swap(*reinterpret_cast<TagValue8 *>(a),
              *reinterpret_cast<TagValue8 *>(b));
    break;
  case Type::TagValue16:
    std::swap(*reinterpret_cast<TagValue16 *>(a),
              *reinterpret_cast<TagValue16 *>(b));
    break;
  }
}

inline void to_json(nlohmann::json &j, const Type &type) {
  switch (type) {
  case Type::VID:
    j = "VID";
    break;
  case Type::VIDPair:
    j = "VIDPair";
    break;
  case Type::Offset:
    j = "Offset";
    break;
  case Type::Int32:
    j = "Int32";
    break;
  case Type::Int64:
    j = "Int64";
    break;
  case Type::ShortString:
    j = "ShortString";
    break;
  case Type::TagValue8:
    j = "TagValue8";
    break;
  case Type::TagValue16:
    j = "TagValue16";
    break;
  }
}

inline void from_json(const nlohmann::json &j, Type &type) {
  assert(j.is_string());
  std::string s = j;
  if (s == "VID") {
    type = Type::VID;
  } else if (s == "VIDPair") {
    type = Type::VIDPair;
  } else if (s == "Offset") {
    type = Type::Offset;
  } else if (s == "Int32") {
    type = Type::Int32;
  } else if (s == "Int64") {
    type = Type::Int64;
  } else if (s == "ShortString") {
    type = Type::ShortString;
  } else if (s == "TagValue8") {
    type = Type::TagValue8;
  } else if (s == "TagValue16") {
    type = Type::TagValue16;
  } else {
    throw std::runtime_error(fmt::format("Unknown type: {}", s));
  }
}

size_t sizeof_type(Type t);

using ValueFilterFn = bool (*)(void *v);

using VariableName = std::string;
using TypeName = std::string;

inline VariableName vertex_property_name(TypeName vertex,
                                         VariableName property_name) {
  return fmt::format("{}.{}", vertex, property_name);
}

using FlatTupleCompareFn = bool (*)(DiskTupleRef &a_ref, DiskTupleRef &b_ref,
                                    size_t a, size_t b);

using FlatTupleOrderFn = size_t (*)(DiskTupleRef &ref, size_t a, void *p);

struct ValueSchema {
  VariableName name;
  Type type;
  inline void *value_at(void *start, size_t at) {
    return (void *)((char *)start + at * sizeof_type(type));
  }
  inline size_t type_size() const { return sizeof_type(type); }
  inline void swap(void *a, void *b) const { swap_with_type(type, a, b); }
  inline std::string to_string(void *data) {
    switch (type) {
    case Type::VID:
      return std::to_string(*reinterpret_cast<VID *>(data));
    case Type::VIDPair:
      return fmt::format("({}, {})", reinterpret_cast<VIDPair *>(data)->first,
                         reinterpret_cast<VIDPair *>(data)->second);
    case Type::Offset:
      return std::to_string(*reinterpret_cast<Offset *>(data));
    case Type::Int32:
      return std::to_string(*reinterpret_cast<Int32 *>(data));
    case Type::Int64:
      return std::to_string(*reinterpret_cast<Int64 *>(data));
    case Type::ShortString:
      return short_string_to_string(data);
    case Type::TagValue8:
      return std::to_string(*reinterpret_cast<TagValue8 *>(data));
    case Type::TagValue16:
      return std::to_string(*reinterpret_cast<TagValue16 *>(data));
    }
    assert(0);
  }

  inline nlohmann::json to_json(void *data) {
    switch (type) {
    case Type::VID:
      return *reinterpret_cast<VID *>(data);
    case Type::VIDPair:
      return {reinterpret_cast<VIDPair *>(data)->first,
              reinterpret_cast<VIDPair *>(data)->second};
    case Type::Offset:
      return *reinterpret_cast<Offset *>(data);
    case Type::Int32:
      return *reinterpret_cast<Int32 *>(data);
    case Type::Int64:
      return *reinterpret_cast<Int64 *>(data);
    case Type::ShortString:
      return short_string_to_string(data);
    case Type::TagValue8:
      return *reinterpret_cast<TagValue8 *>(data);
    case Type::TagValue16:
      return *reinterpret_cast<TagValue16 *>(data);
    }
    assert(0);
  }

  friend bool operator==(const ValueSchema &a, const ValueSchema &b) {
    return a.name == b.name && a.type == b.type;
  }

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(ValueSchema, name, type);
};

// implement libfmt for ValueSchema
template <> struct fmt::formatter<ValueSchema> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const ValueSchema &p, FormatContext &ctx) const
      -> format_context::iterator {
    return format_to(ctx.out(), "{}:{}", p.name, p.type);
  }
};

enum class FilterType {
  NO_FILTER,
  ID, // NO Support
  TAG,
  USER_FUNCTION,
};

struct VariableFilter { // only filter on tag
  ValueSchema vas;
  FilterType filter_type = FilterType::USER_FUNCTION;

  Range<TagValue8> tag; // only support tag8
  ValueFilterFn filter;

  inline VariableFilter(ValueSchema vas)
      : vas(vas), filter_type(FilterType::NO_FILTER) {}

  inline VariableFilter(ValueSchema vas, ValueFilterFn filter)
      : vas(vas), filter_type(FilterType::USER_FUNCTION), filter(filter) {}

  inline VariableFilter(ValueSchema vas, Range<TagValue8> tag)
      : vas(vas), filter_type(FilterType::TAG), tag(tag) {}

  inline bool filter_at(void *start, size_t at) {
    switch (filter_type) {

    case FilterType::NO_FILTER:
      return true;
    case FilterType::ID:
      assert(0);
    case FilterType::TAG:
      return tag.in(*reinterpret_cast<TagValue8 *>(vas.value_at(start, at)));
    case FilterType::USER_FUNCTION:
      return filter(vas.value_at(start, at));
    default:
      assert(0);
    }
  }
};

struct VariableFilters {
  std::vector<VariableFilter> filters;

  inline VariableFilters() = default;

  inline VariableFilters &add_filter(VariableFilter vf) {
    filters.push_back(vf);
    return *this;
  }

  inline VariableFilters &add_in_range_filter(ValueSchema vas,
                                              Range<TagValue8> tag) {
    filters.push_back(VariableFilter(vas, tag));
    return *this;
  }

  inline VariableFilters &add_in_tagvalue8_range_filter(VariableName name,
                                                        Range<TagValue8> tag) {
    ValueSchema vas;
    vas.name = name;
    vas.type = Type::TagValue8;
    filters.push_back(VariableFilter(vas, tag));
    return *this;
  }

  inline VariableFilters &add_tagvalue8_filter(VariableName name,
                                               ValueFilterFn f) {
    ValueSchema vas;
    vas.name = name;
    vas.type = Type::TagValue8;
    filters.push_back(VariableFilter(vas, f));
    return *this;
  }
};

struct TypeLocation {
  Type type;
  size_t location;
};

// Type from std::string
inline Type type_from_string(const std::string &s) {
  if (s == "VID")
    return Type::VID;
  if (s == "VIDPair")
    return Type::VIDPair;
  if (s == "Offset")
    return Type::Offset;
  if (s == "Int32")
    return Type::Int32;
  if (s == "Int64")
    return Type::Int64;
  if (s == "ShortString")
    return Type::ShortString;
  if (s == "TagValue8")
    return Type::TagValue8;
  if (s == "TagValue16")
    return Type::TagValue16;

  throw std::runtime_error(fmt::format("Unknown type: {}", s));
}

// implement libfmt for Type
template <> struct fmt::formatter<Type> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext> auto format(Type t, FormatContext &ctx) {
    switch (t) {
    case Type::VID:
      return format_to(ctx.out(), "VID");
    case Type::VIDPair:
      return format_to(ctx.out(), "VIDPair");
    case Type::Offset:
      return format_to(ctx.out(), "Offset");
    case Type::Int32:
      return format_to(ctx.out(), "Int32");
    case Type::Int64:
      return format_to(ctx.out(), "Int64");
    case Type::ShortString:
      return format_to(ctx.out(), "ShortString");
    case Type::TagValue8:
      return format_to(ctx.out(), "TagValue8");
    case Type::TagValue16:
      return format_to(ctx.out(), "TagValue16");
    }
    assert(0);
  }
};

// implement libfmt for TypeLocation
template <> struct fmt::formatter<TypeLocation> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const TypeLocation &p, FormatContext &ctx) const
      -> format_context::iterator {
    return format_to(ctx.out(), "{}:{}", p.type, p.location);
  }
};

// Get sizeof Type
inline size_t sizeof_type(Type t) {
  switch (t) {
  case Type::VID:
    return sizeof(VID);
  case Type::VIDPair:
    return sizeof(VIDPair);
  case Type::Offset:
    return sizeof(Offset);
  case Type::Int32:
    return sizeof(Int32);
  case Type::Int64:
    return sizeof(Int64);
  case Type::ShortString:
    return sizeof(ShortString);
  case Type::TagValue8:
    return sizeof(TagValue8);
  case Type::TagValue16:
    return sizeof(TagValue16);
  default:
    throw std::runtime_error(fmt::format("Unknown type: {}", t));
  }
}
