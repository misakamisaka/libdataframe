#include "type.h"

namespace mortred {

#define TYPE_FACTORY(NAME)                                              \
  TypePtr DataTypes::NAME() {                                           \
    static std::shared_ptr<DataType> result = std::make_shared<NAME>(); \
    return result;                                                      \
  }

TYPE_FACTORY(NullType);
TYPE_FACTORY(BooleanType);
TYPE_FACTORY(Int8Type);
TYPE_FACTORY(UInt8Type);
TYPE_FACTORY(Int16Type);
TYPE_FACTORY(UInt16Type);
TYPE_FACTORY(Int32Type);
TYPE_FACTORY(UInt32Type);
TYPE_FACTORY(Int64Type);
TYPE_FACTORY(UInt64Type);
TYPE_FACTORY(FloatType);
TYPE_FACTORY(DoubleType);
TYPE_FACTORY(StringType);
TYPE_FACTORY(BinaryType);
TYPE_FACTORY(DateType);

TypePtr DataTypes::DecimalType(int precision, int scale) {
  return std::make_shared<DecimalType>(precision, scale);
}

TypePtr DataTypes::DecimalType(TimeUnit unit) {
  return std::make_shared<IntervalType>(unit);
}
TypePtr DataTypes::IntervalType(Unit unit) {
  return std::make_shared<IntervalType>(unit);
}

TypePtr DataTypes::MapType(const TypePtr& key_type,
      const TypePtr& value_type, bool value_contains_null_ = false) {
  return std::make_shared<MapType>(key_type, value_type, value_contains_null);
}

TypePtr DataTypes::ListType(const TypePtr& value_type) {
  return std::make_shared<ListType>(value_type);
}

TypePtr DataTypes::ListType(const std::shared_ptr<Column>& column) {
  return std::make_shared<ListType>(column);
}

TypePtr DataTypes::StructType(const std::vector<std::shared_ptr<Column>>& columns) {
  return std::make_shared<StructType>(columns);
}

DataType::~DataType() {}

bool DataType::Equals(const DataType& other) const {
  bool are_equal = false;
  Status error = TypeEquals(*this, other, &are_equal);
  if (!error.ok()) { DCHECK(false) << "Types not comparable: " << error.ToString(); }
  return are_equal;
}

bool DataType::Equals(const TypePtr<DataType>& other) const {
  if (!other) { return false; }
  return Equals(*other.get());
}

void TypeEquals(const DataType& left, const DataType& right, bool* are_equal) {
  // The arrays are the same object
  if (&left == &right) {
    *are_equal = true;
  } else if (left.type != right.type) {
    *are_equal = false;
  } else {
    TypeEqualsVisitor visitor(right);
    VisitTypeInline(left, &visitor);
    *are_equal = visitor.result();
  }
}
} //namespace mortred
