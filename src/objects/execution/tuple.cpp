#include <xxhash.h>

#include "execution/tuple.hh"

using namespace std;

bool isMatrixTable(TableType type) {
  switch (type) {
  case TableType::VarRowMatrix:
    return true;
  case TableType::ColMainMatrix:
    return true;
  default:
    return false;
  }
}

bool isMultipleMatrixTable(TableType type) {
  switch (type) {
  case TableType::MultipleColMainMatrix:
    return true;
  case TableType::MultipleVarRowMatrix:
    return true;
  default:
    return false;
  }
}

TableType TypeOfMultipleMatrix(TableType type) {
  switch (type) {
  case TableType::MultipleColMainMatrix:
    return TableType::ColMainMatrix;
  case TableType::MultipleVarRowMatrix:
    return TableType::VarRowMatrix;
  default:
    assert(0);
  }
}

TableType MultipleTypeOfMatrix(TableType type) {
  switch (type) {
  case TableType::ColMainMatrix:
    return TableType::MultipleColMainMatrix;
  case TableType::VarRowMatrix:
    return TableType::MultipleVarRowMatrix;
  default:
    assert(0);
  }
}

TupleValue TupleValue::construct_tuple_value(Type t, void *from) {
  TupleValue v;
  switch (t) {
  case Type::VID:
    v.vid = *reinterpret_cast<VID *>(from);
    break;
  case Type::VIDPair:
    v.vidp = *reinterpret_cast<VIDPair *>(from);
    break;
  case Type::Offset:
    v.offset = *reinterpret_cast<Offset *>(from);
    break;
  case Type::Int32:
    v.i32 = *reinterpret_cast<Int32 *>(from);
    break;
  case Type::Int64:
    v.i64 = *reinterpret_cast<Int64 *>(from);
    break;
  case Type::ShortString:
    v.shortstring = *reinterpret_cast<ShortString *>(from);
    break;
  case Type::TagValue8:
    v.tagvalue8 = *reinterpret_cast<TagValue8 *>(from);
    break;
  case Type::TagValue16:
    v.tagvalue16 = *reinterpret_cast<TagValue16 *>(from);
    break;
  default:
    assert(false);
  }
  return v;
}

TupleValue::TupleValue(initializer_list<Int32> l) : TupleValue() {
  i32 = *l.begin();
}

uint64_t TupleValue::hash_value(const TupleValue &tv, Type t) {
  uint64_t seed = 0;
  XXH64(&tv, sizeof_type(t), seed);
  return seed;
}

string TupleValue::to_string(Type t) const {
  switch (t) {
  case Type::VID:
    return std::to_string(vid);
  case Type::VIDPair:
    return fmt::format("({}, {})", vidp.first, vidp.second);
  case Type::Offset:
    return std::to_string(offset);
  case Type::Int32:
    return std::to_string(i32);
  case Type::Int64:
    return std::to_string(i64);
  case Type::ShortString:
    return short_string_to_string(shortstring);
  case Type::TagValue8:
    return std::to_string(tagvalue8);
  case Type::TagValue16:
    return std::to_string(tagvalue16);
  default:
    assert(0);
  }
}

void *TupleValue::value_ptr(Type type) {
  switch (type) {
  case Type::VID:
    return &vid;
  case Type::VIDPair:
    return &vidp;
  case Type::Offset:
    return &offset;
  case Type::Int32:
    return &i32;
  case Type::Int64:
    return &i64;
  case Type::ShortString:
    return &shortstring;
  case Type::TagValue8:
    return &tagvalue8;
  case Type::TagValue16:
    return &tagvalue16;
  default:
    assert(0);
  }
}

bool TupleValue::eq(const TupleValue &a, const TupleValue &b, Type t) {
  switch (t) {
  case Type::VID:
    return a.vid == b.vid;
  case Type::VIDPair:
    return (a.vidp.first == b.vidp.first) && (a.vidp.second == b.vidp.second);
  case Type::Offset:
    return a.offset == b.offset;
  case Type::Int32:
    return a.i32 == b.i32;
  case Type::Int64:
    return a.i64 == b.i64;
  case Type::ShortString:
    return a.shortstring == b.shortstring;
  case Type::TagValue8:
    return a.tagvalue8 == b.tagvalue8;
  case Type::TagValue16:
    return a.tagvalue16 == b.tagvalue16;
  default:
    assert(false);
  }
}

TupleValue TupleValue::zero(Type t) {
  TupleValue re;
  switch (t) {
  case Type::VID:
    re.vid = 0;
    break;
  case Type::VIDPair:
    re.vidp = {0, 0};
    break;
  case Type::Offset:
    re.offset = 0;
    break;
  case Type::Int32:
    re.i32 = 0;
    break;
  case Type::Int64:
    re.i64 = 0;
    break;
  case Type::ShortString:
    re.shortstring = {0};
    break;
  case Type::TagValue8:
    re.tagvalue8 = 0;
    break;
  case Type::TagValue16:
    re.tagvalue16 = 0;
    break;
  default:
    assert(false);
  }
  return re;
}

TupleValue TupleValue::add(TupleValue a, TupleValue b, Type t) {
  TupleValue re;
  switch (t) {
  case Type::VID:
    re.vid = a.vid + b.vid;
    break;
  case Type::VIDPair:
    re.vidp = {a.vidp.first + b.vidp.first, a.vidp.second + b.vidp.second};
    break;
  case Type::Offset:
    re.offset = a.offset + b.offset;
    break;
  case Type::Int32:
    re.i32 = a.i32 + b.i32;
    break;
  case Type::Int64:
    re.i64 = a.i64 + b.i64;
    break;
  case Type::ShortString:
    assert(false);
    break;
  case Type::TagValue8:
    re.tagvalue8 = a.tagvalue8 + b.tagvalue8;
    break;
  case Type::TagValue16:
    re.tagvalue16 = a.tagvalue16 + b.tagvalue16;
    break;
  default:
    assert(false);
  }
  return re;
}

Tuple::Tuple(size_t size) {
  if (size > InlineTupleValueCount) {
    overflow.resize(size - InlineTupleValueCount);
    inlined_count = InlineTupleValueCount;
  } else {
    inlined_count = size;
  }
}

Tuple::Tuple(std::initializer_list<TupleValue> list)
    : inlined_count(list.size()) {
  assert(list.size() <= InlineTupleValueCount);
  size_t i = 0;
  for (auto v : list) {
    inlined[i] = v;
    i++;
  }
}

size_t Tuple::size() const { return inlined_count + overflow.size(); }

void Tuple::push_back(TupleValue v) {
  if (inlined_count < InlineTupleValueCount) {
    inlined[inlined_count] = v;
    inlined_count++;
  } else {
    overflow.push_back(v);
  }
}

TupleValue &Tuple::operator[](size_t i) {
  if (i < InlineTupleValueCount) {
    return inlined[i];
  } else {
    return overflow[i - InlineTupleValueCount];
  }
}

std::optional<std::pair<ValueSchema, size_t>>
TableSchema::get_schema(std::string name) const {

  for (size_t i = 0; i < columns.size(); i++) {
    if (columns[i].name == name) {
      return {{columns[i], i}};
    }
  }
  return {};
}

bool TableSchema::has_duplicate_variable() {
  std::vector<std::string> names;
  for (auto &c : columns) {
    if (std::find(names.begin(), names.end(), c.name) != names.end()) {
      return true;
    }
    names.push_back(c.name);
  }
  return false;
}

auto TableSchema::map_to(const TableSchema &other) const -> vector<size_t> {
  vector<size_t> re;
  for (auto &c : columns) {
    auto s = other.get_schema(c.name);
    if (s.has_value()) {
      re.push_back(s.value().second);
    } else {
      re.push_back(-1);
    }
  }
  return re;
}

void TableSchema::add_non_duplicated_variable(ValueSchema vs) {
  columns.push_back(vs);
  assert(has_duplicate_variable() == false);
}

void TableSchema::remove_variable(ValueSchema vs) {
  auto it = std::find(columns.begin(), columns.end(), vs);
  assert(it != columns.end());
  columns.erase(it);
}

void TableSchema::merge_schema(const TableSchema &other) {
  columns.insert(columns.end(), other.columns.begin(), other.columns.end());
}

TableSchema TableSchema::join_schema(const TableSchema &a, const TableSchema &b,
                                     std::optional<std::string> key) {
  TableSchema re;
  if (key.has_value()) {
    if (a.get_schema(key.value()).has_value() == false) {
      SPDLOG_ERROR("Key {} not found in schema {}", key.value(), a.columns);
      assert(0);
    }
    if (b.get_schema(key.value()).has_value() == false) {
      SPDLOG_ERROR("Key {} not found in schema {}", key.value(), b.columns);
      assert(0);
    }

    re.columns = a.columns;
    for (size_t i = 0; i < b.columns.size(); i++) {
      if (b.columns[i].name != key.value()) {
        re.columns.push_back(b.columns[i]);
      }
    }
  } else {
    re.columns = a.columns;
    re.columns.insert(re.columns.end(), b.columns.begin(), b.columns.end());
  }
  assert(re.has_duplicate_variable() == false);
  return re;
}

void *DiskTupleRef::get(const TableSchema &schema, size_t i, size_t j) {
  return reinterpret_cast<char *>(data[j]) + i * schema.columns[j].type_size();
}

void *DiskTupleRef::get(const Type type, size_t i, size_t j) {
  return reinterpret_cast<char *>(data[j]) + i * sizeof_type(type);
}

TupleValue DiskTupleRef::copy_out_at(const TableSchema &schema, const size_t i,
                                     const size_t j) {
  return TupleValue::construct_tuple_value(
      schema.columns[j].type,
      reinterpret_cast<char *>(data[j]) + i * schema.columns[j].type_size());
}

void DiskTupleRef::inc(const std::vector<ValueSchema> &schema, size_t j) {
  for (size_t i = 0; i < schema.size(); i++) {
    data[i] = reinterpret_cast<char *>(data[i]) + i * schema[j].type_size();
  }
}

DiskTupleRef DiskTupleRef::offset(const TableSchema &schema, size_t x) const {
  DiskTupleRef re;
  for (size_t i = 0; i < schema.columns.size(); i++) {
    re.data[i] =
        reinterpret_cast<char *>(data[i]) + x * schema.columns[i].type_size();
  }
  return re;
}

auto DiskTupleRef::to_string(const TableSchema &schema) const -> string {
  string re;
  for (size_t i = 0; i < schema.columns.size(); i++) {
    auto t = schema.columns[i].type;
    re += fmt::format(
        "{}", TupleValue::construct_tuple_value(t, data[i]).to_string(t));
    re += i == schema.columns.size() - 1 ? "" : ",";
  }
  return re;
}