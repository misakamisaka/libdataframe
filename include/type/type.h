#ifndef MORTRED_TYPE_H
#define MORTRED_TYPE_H

#include <memory>
#include <vector>
#include "column.h"
#include "type/type_visitor.h"
#include "util.h"

namespace mortred {

class Column;
class TypeVisitor;

enum class Type : int {
    // A degenerate NULL type represented as 0 bytes/bits
    NA,

    // A boolean value represented as 1 bit
    BOOL,

    // Little-endian integer types
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,

    // 4-byte floating point value
    FLOAT,
    // 8-byte floating point value
    DOUBLE,

    // UTF8 variable-length string as List<Char>
    STRING,
    // Variable-length bytes (no guarantee of UTF8-ness)
    BINARY,

    DATE,
    // Exact timestamp encoded with int64 since UNIX epoch
    // Default unit millisecond
    TIMESTAMP,
    // YEAR_MONTH or DAY_TIME interval in SQL style
    INTERVAL,

    // Precision- and scale-based decimal type. Storage type depends on the
    // parameters.
    DECIMAL,

    // A list of some logical data type
    LIST,

    // Struct of logical types
    STRUCT,

    // Dictionary aka Category type
    MAP
};

struct DataType;
using TypePtr = std::shared_ptr<DataType>;

struct DataType {
  Type type;

  std::vector<std::shared_ptr<Column>> children_;

  explicit DataType(Type type) : type(type) {}

  virtual ~DataType();

  // Return whether the types are equal
  //
  // Types that are logically convertable from one to another e.g. List<UInt8>
  // and Binary are NOT equal).
  virtual bool Equals(const DataType& other) const;
  bool Equals(const TypePtr& other) const;

  std::shared_ptr<Column> child(int i) const { return children_[i]; }

  const std::vector<std::shared_ptr<Column>>& children() const { return children_; }

  int num_children() const { return static_cast<int>(children_.size()); }

  virtual void Accept(TypeVisitor* visitor) const = 0;

  virtual std::string ToString() const = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(DataType);
};

enum class IntervalUnit : char;

struct DataTypes {
  static TypePtr MakeNullType();
  static TypePtr MakeBooleanType();
  static TypePtr MakeUInt8Type();
  static TypePtr MakeInt8Type();
  static TypePtr MakeUInt16Type();
  static TypePtr MakeInt16Type();
  static TypePtr MakeUInt32Type();
  static TypePtr MakeInt32Type();
  static TypePtr MakeUInt64Type();
  static TypePtr MakeInt64Type();
  static TypePtr MakeFloatType();
  static TypePtr MakeDoubleType();
  static TypePtr MakeStringType();
  static TypePtr MakeBinaryType();
  static TypePtr MakeDateType();

  static TypePtr MakeDecimalType(int precision, int scale);
  static TypePtr MakeTimestampType();
  static TypePtr MakeIntervalType(IntervalUnit unit);
  static TypePtr MakeMapType(const TypePtr& key_type, const TypePtr& value_type, bool value_contains_null = false);
  static TypePtr MakeListType(const TypePtr& value_type);
  static TypePtr MakeListType(const std::shared_ptr<Column>& column);
  static TypePtr MakeStructType(const std::vector<std::shared_ptr<Column>>& columns);

  static TypePtr FindTightesetCommonType(TypePtr type1, TypePtr type2);

  static bool IsInteger(Type type);
  static bool IsFloating(Type type);
  static bool IsPrimitive(Type type);
};


struct AtomicType : public DataType {
  using DataType::DataType;
};

struct PrimitiveCType : public AtomicType {
  using AtomicType::AtomicType;

  virtual int bit_width() const = 0;
};

struct Integer : public PrimitiveCType {
  using PrimitiveCType::PrimitiveCType;
  virtual bool is_signed() const = 0;
};

struct Fractional : public PrimitiveCType {
  using PrimitiveCType::PrimitiveCType;
};

struct NestedType : public DataType {
  using DataType::DataType;
};

template <typename DERIVED, typename BASE, Type TYPE_ID, typename C_TYPE>
struct CTypeImpl : public BASE {
  using c_type = C_TYPE;
  static constexpr Type type_id = TYPE_ID;

  CTypeImpl() : BASE(TYPE_ID) {}

  int bit_width() const override { return static_cast<int>(sizeof(C_TYPE)); }

  void Accept(TypeVisitor* visitor) const override {
    return visitor->Visit(*static_cast<const DERIVED*>(this));
  }

  std::string ToString() const override { return std::string(DERIVED::name()); }
};

struct NoExtraMeta {};

struct NullType : public DataType, public NoExtraMeta {
  static constexpr Type type_id = Type::NA;

  NullType() : DataType(Type::NA) {}

  void Accept(TypeVisitor* visitor) const override { return visitor->Visit(*this); }
  std::string ToString() const override { return name(); }

  static std::string name() { return "null"; }
};

template <typename DERIVED, Type TYPE_ID, typename C_TYPE>
struct IntegerTypeImpl : public CTypeImpl<DERIVED, Integer, TYPE_ID, C_TYPE> {
  bool is_signed() const override { return std::is_signed<C_TYPE>::value; }
};

struct BooleanType : public AtomicType, public NoExtraMeta {
  static constexpr Type type_id = Type::BOOL;
  using c_type = bool;

  BooleanType() : AtomicType(Type::BOOL) {}

  void Accept(TypeVisitor* visitor) const override { return visitor->Visit(*this); }
  std::string ToString() const override {return name();}

  int bit_width() const { return 1; }
  static std::string name() { return "bool"; }
};

struct UInt8Type : public IntegerTypeImpl<UInt8Type, Type::UINT8, uint8_t> {
  static std::string name() { return "uint8"; }
};

struct Int8Type : public IntegerTypeImpl<Int8Type, Type::INT8, int8_t> {
  static std::string name() { return "int8"; }
};

struct UInt16Type
    : public IntegerTypeImpl<UInt16Type, Type::UINT16, uint16_t> {
  static std::string name() { return "uint16"; }
};

struct Int16Type : public IntegerTypeImpl<Int16Type, Type::INT16, int16_t> {
  static std::string name() { return "int16"; }
};

struct UInt32Type
    : public IntegerTypeImpl<UInt32Type, Type::UINT32, uint32_t> {
  static std::string name() { return "uint32"; }
};

struct Int32Type : public IntegerTypeImpl<Int32Type, Type::INT32, int32_t> {
  static std::string name() { return "int32"; }
};

struct UInt64Type
    : public IntegerTypeImpl<UInt64Type, Type::UINT64, uint64_t> {
  static std::string name() { return "uint64"; }
};

struct Int64Type : public IntegerTypeImpl<Int64Type, Type::INT64, int64_t> {
  static std::string name() { return "int64"; }
};

struct FloatType
    : public CTypeImpl<FloatType, Fractional, Type::FLOAT, float> {
  static std::string name() { return "float"; }
};

struct DoubleType
    : public CTypeImpl<DoubleType, Fractional, Type::DOUBLE, double> {
  static std::string name() { return "double"; }
};

struct ListType : public NestedType {
  static constexpr Type type_id = Type::LIST;

  // List can contain any other logical value type
  explicit ListType(const TypePtr& value_type)
      : ListType(std::make_shared<Column>("item", value_type)) {}

  explicit ListType(const std::shared_ptr<Column>& value_column) : NestedType(Type::LIST) {
    children_ = {value_column};
  }

  std::shared_ptr<Column> value_column() const { return children_[0]; }

  TypePtr value_type() const { return children_[0]->data_type(); }

  void Accept(TypeVisitor* visitor) const override { return visitor->Visit(*this); }
  std::string ToString() const override { return std::string("list<") + value_column()->ToString() + ">"; }

  static std::string name() { return "list"; }
};

struct BinaryType : public AtomicType, public NoExtraMeta {
  static constexpr Type type_id = Type::BINARY;

  BinaryType() : BinaryType(Type::BINARY) {}

  void Accept(TypeVisitor* visitor) const override { return visitor->Visit(*this); }
  std::string ToString() const override { return name(); }
  static std::string name() { return "binary"; }
 protected:
  explicit BinaryType(Type logical_type) : AtomicType(logical_type) {}
};

struct StringType : public BinaryType {
  static constexpr Type type_id = Type::STRING;

  StringType() : BinaryType(Type::STRING) {}

  void Accept(TypeVisitor* visitor) const override { return visitor->Visit(*this); }
  std::string ToString() const override { return name(); }
  static std::string name() { return "string"; }
};

struct StructType : public NestedType {
  static constexpr Type type_id = Type::STRUCT;

  explicit StructType(const std::vector<std::shared_ptr<Column>>& columns)
      : NestedType(Type::STRUCT) {
    children_ = columns;
  }

  void Accept(TypeVisitor* visitor) const override { return visitor->Visit(*this); }
  std::string ToString() const override;
  static std::string name() { return "struct"; }
};

struct DecimalType : public AtomicType {
  static constexpr Type type_id = Type::DECIMAL;

  explicit DecimalType(int precision_, int scale_)
      : AtomicType(Type::DECIMAL), precision(precision_), scale(scale_) {}
  int precision;
  int scale;

  void Accept(TypeVisitor* visitor) const override { return visitor->Visit(*this); }
  bool IsWiderThan(TypePtr type);
  static TypePtr ForType(TypePtr type);
  std::string ToString() const override { return std::string("decimal(") + std::to_string(precision) + ", " + std::to_string(scale) + ")"; }
  static std::string name() { return "decimal"; }
};

struct DateType : public AtomicType, public NoExtraMeta {
  static constexpr Type type_id = Type::DATE;

  using c_type = int32_t;
  explicit DateType()
    : AtomicType(Type::DATE) {}

  int bit_width() const { return static_cast<int>(sizeof(c_type)); }

  void Accept(TypeVisitor* visitor) const override { return visitor->Visit(*this); }
  std::string ToString() const override { return name(); }
  static std::string name() { return "date"; }
};

struct TimestampType : public AtomicType {
  using c_type = int64_t;
  static constexpr Type type_id = Type::TIMESTAMP;

  int bit_width() const { return static_cast<int>(sizeof(c_type)); }

  explicit TimestampType()
      : AtomicType(Type::TIMESTAMP) {}

  void Accept(TypeVisitor* visitor) const override { return visitor->Visit(*this); }
  std::string ToString() const override { return name(); }
  static std::string name() { return "timestamp"; }
};

enum class IntervalUnit : char { MONTH = 0, MICROSECOND = 1 };
struct IntervalType : public AtomicType {

  using c_type = int64_t;
  static constexpr Type type_id = Type::INTERVAL;

  int bit_width() const { return static_cast<int>(sizeof(c_type)); }

  IntervalUnit unit;

  explicit IntervalType(IntervalUnit unit = IntervalUnit::MONTH)
      : AtomicType(Type::INTERVAL), unit(unit) {}

  void Accept(TypeVisitor* visitor) const override { return visitor->Visit(*this); }
  std::string ToString() const override { return name(); }
  static std::string name() { return "interval"; }
};

// ----------------------------------------------------------------------

class MapType : public NestedType {
 public:
  static constexpr Type type_id = Type::MAP;

  MapType(const TypePtr& key_type,
      const TypePtr& value_type, bool value_contains_null = false)
    : NestedType(Type::MAP), key_type_(key_type), value_type_(value_type),
    value_contains_null_(value_contains_null) { }

  TypePtr key_type() const { return key_type_; }

  TypePtr value_type() const { return value_type_; }

  bool value_contains_null() const { return value_contains_null_; }

  void Accept(TypeVisitor* visitor) const override { return visitor->Visit(*this); }
  std::string ToString() const override;

 private:
  TypePtr key_type_;
  TypePtr value_type_;
  bool value_contains_null_;
};

} //namespace mortred

#endif
