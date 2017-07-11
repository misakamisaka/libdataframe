#ifndef MORTRED_TYPE_H
#define MORTRED_TYPE_H

namespace mortred {

class Column;

enum class Type : int8_t {
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

struct DataType {
  Type::type type;

  std::vector<std::shared_ptr<Column>> children_;

  explicit DataType(Type::type type) : type(type) {}

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

  virtual Status Accept(TypeVisitor* visitor) const = 0;

  virtual std::string ToString() const = 0;

  virtual IsWiderThan(std::shared_ptr<DataType> data_type) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(DataType);
};

typedef std::shared_ptr<DataType> TypePtr;

struct DataTypes {
  static TypePtr NullType();
  static TypePtr BooleanType();
  static TypePtr UInt8Type();
  static TypePtr Int8Type();
  static TypePtr UInt16Type();
  static TypePtr Int16Type();
  static TypePtr UInt32Type();
  static TypePtr Int32Type();
  static TypePtr UInt64Type();
  static TypePtr Int64Type();
  static TypePtr FloatType();
  static TypePtr DoubleType();
  static TypePtr StringType();
  static TypePtr BinaryType();
  static TypePtr DateType();
  static TypePtr TimestampType();
  static TypePtr IntervalType();

  //
  //static TypePtr DecimalType();
  //static TypePtr ListType();
  //static TypePtr StructType();
  //static TypePtr MapType();

  static TypePtr FindTightesetCommonType(TypePtr type1, TypePtr type2);
  static bool IsInteger(Type::type type_id);
  static bool IsFloating(Type::type type_id);
};


struct AtomicType : public DataType {
  using DataType::DataType;

  virtual int bit_width() const = 0;
};

struct PrimitiveCType : public AtomicType {
  using AtomicType::AtomicType;
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

template <typename DERIVED, typename BASE, Type::type TYPE_ID, typename C_TYPE>
struct CTypeImpl : public BASE {
  using c_type = C_TYPE;
  static constexpr Type::type type_id = TYPE_ID;

  CTypeImpl() : BASE(TYPE_ID) {}

  int bit_width() const override { return static_cast<int>(sizeof(C_TYPE) * CHAR_BIT); }

  Status Accept(TypeVisitor* visitor) const override {
    return visitor->Visit(*static_cast<const DERIVED*>(this));
  }

  std::string ToString() const override { return std::string(DERIVED::name()); }
};

struct NoExtraMeta {};

struct NullType : public DataType, public NoExtraMeta {
  static constexpr Type::type type_id = Type::NA;

  NullType() : DataType(Type::NA) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  static std::string name() { return "null"; }
};

template <typename DERIVED, Type::type TYPE_ID, typename C_TYPE>
struct IntegerTypeImpl : public CTypeImpl<DERIVED, Integer, TYPE_ID, C_TYPE> {
  bool is_signed() const override { return std::is_signed<C_TYPE>::value; }
};

struct BooleanType : public AtomicType, public NoExtraMeta {
  static constexpr Type::type type_id = Type::BOOL;

  BooleanType() : AtomicType(Type::BOOL) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  int bit_width() const override { return 1; }
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
  static constexpr Type::type type_id = Type::LIST;

  // List can contain any other logical value type
  explicit ListType(const TypePtr& value_type)
      : ListType(std::make_shared<Column>("item", value_type)) {}

  explicit ListType(const std::shared_ptr<Column>& value_column) : NestedType(Type::LIST) {
    children_ = {value_column};
  }

  std::shared_ptr<Column> value_column() const { return children_[0]; }

  TypePtr value_type() const { return children_[0]->type; }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  static std::string name() { return "list"; }
};

// BinaryType type is represents lists of 1-byte values.
struct BinaryType : public AtomicType, public NoExtraMeta {
  static constexpr Type::type type_id = Type::BINARY;

  BinaryType() : BinaryType(Type::BINARY) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "binary"; }

 protected:
  // Allow subclasses to change the logical type.
  explicit BinaryType(Type::type logical_type) : DataType(logical_type) {}
};

// UTF-8 encoded strings
struct StringType : public BinaryType {
  static constexpr Type::type type_id = Type::STRING;

  StringType() : BinaryType(Type::STRING) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "utf8"; }
};

struct StructType : public NestedType {
  static constexpr Type::type type_id = Type::STRUCT;

  explicit StructType(const std::vector<std::shared_ptr<Column>>& columns)
      : NestedType(Type::STRUCT) {
    children_ = columns;
  }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "struct"; }
};

struct DecimalType : public AtomicType {
  static constexpr Type::type type_id = Type::DECIMAL;

  explicit DecimalType(int precision_, int scale_)
      : DataType(Type::DECIMAL), precision(precision_), scale(scale_) {}
  int precision;
  int scale;

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "decimal"; }
};

// ----------------------------------------------------------------------
// Date and time types

/// Date as int32_t days since UNIX epoch
struct DateType : public AtomicType, public NoExtraMeta {
  static constexpr Type::type type_id = Type::DATE;

  using c_type = int32_t;
  DateType();

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
};

enum class TimeUnit : char { SECOND = 0, MILLI = 1, MICRO = 2, NANO = 3 };

static inline std::ostream& operator<<(std::ostream& os, TimeUnit unit) {
  switch (unit) {
    case TimeUnit::SECOND:
      os << "s";
      break;
    case TimeUnit::MILLI:
      os << "ms";
      break;
    case TimeUnit::MICRO:
      os << "us";
      break;
    case TimeUnit::NANO:
      os << "ns";
      break;
  }
  return os;
}

struct TimestampType : public AtomicType {
  using Unit = TimeUnit;

  typedef int64_t c_type;
  static constexpr Type::type type_id = Type::TIMESTAMP;

  int bit_width() const override { return static_cast<int>(sizeof(int64_t) * CHAR_BIT); }

  explicit TimestampType(TimeUnit unit = TimeUnit::MILLI)
      : AtomicType(Type::TIMESTAMP), unit(unit) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "timestamp"; }

  TimeUnit unit;
};

struct IntervalType : public AtomicType {
  enum class Unit : char { YEAR_MONTH = 0, DAY_TIME = 1 };

  using c_type = int64_t;
  static constexpr Type::type type_id = Type::INTERVAL;

  int bit_width() const override { return static_cast<int>(sizeof(int64_t) * CHAR_BIT); }

  Unit unit;

  explicit IntervalType(Unit unit = Unit::YEAR_MONTH)
      : AtomicType(Type::INTERVAL), unit(unit) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override { return name(); }
  static std::string name() { return "date"; }
};

// ----------------------------------------------------------------------

class MapType : public NestedType {
 public:
  static constexpr Type::type type_id = Type::Map;

  MapType(const TypePtr& key_type,
      const TypePtr& value_type, bool value_contains_null = false);

  int bit_width() const override;

  TypePtr key_type() const { return key_type_; }

  TypePtr value_type() const { return value_type_; }

  TypePtr value_contains_null() const { return value_contains_null_; }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

 private:
  TypePtr key_type_;
  TypePtr value_type_;
  bool value_contains_null_;
};

} //namespace mortred

#endif
