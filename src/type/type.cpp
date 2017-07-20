#include "type/type.h"
#include <sstream>
#include <glog/logging.h>
#include "type/type_visitor_inline.h"
#include "type/type_equal_visitor.h"

namespace mortred {

void TypeEquals(const DataType& left, const DataType& right, bool* are_equal) {
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

DataType::~DataType() {}

bool DataType::Equals(const DataType& other) const {
  bool are_equal = false;
  try {
    TypeEquals(*this, other, &are_equal);
  } catch (TypeException& e){
    LOG(INFO) << "Types not comparable, error[" << e.what() << "]";
  }
  return are_equal;
}

bool DataType::Equals(const TypePtr& other) const {
  if (!other) { return false; }
  return Equals(*other.get());
}

#define TYPE_FACTORY(NAME)                                              \
  TypePtr DataTypes::Make##NAME() {                                           \
    static std::shared_ptr<DataType> result = std::make_shared<NAME>(); \
    return result;                                                      \
  }

TYPE_FACTORY(NullType)
TYPE_FACTORY(BooleanType)
TYPE_FACTORY(Int8Type)
TYPE_FACTORY(UInt8Type)
TYPE_FACTORY(Int16Type)
TYPE_FACTORY(UInt16Type)
TYPE_FACTORY(Int32Type)
TYPE_FACTORY(UInt32Type)
TYPE_FACTORY(Int64Type)
TYPE_FACTORY(UInt64Type)
TYPE_FACTORY(FloatType)
TYPE_FACTORY(DoubleType)
TYPE_FACTORY(StringType)
TYPE_FACTORY(BinaryType)
TYPE_FACTORY(DateType)
TYPE_FACTORY(TimestampType)

TypePtr DataTypes::MakeDecimalType(int precision, int scale) {
  return std::make_shared<DecimalType>(precision, scale);
}

TypePtr DataTypes::MakeIntervalType(IntervalUnit unit) {
  return std::make_shared<IntervalType>(unit);
}

TypePtr DataTypes::MakeMapType(const TypePtr& key_type,
      const TypePtr& value_type, bool value_contains_null) {
  return std::make_shared<MapType>(key_type, value_type, value_contains_null);
}

TypePtr DataTypes::MakeListType(const TypePtr& value_type) {
  return std::make_shared<ListType>(value_type);
}

TypePtr DataTypes::MakeListType(const std::shared_ptr<Column>& column) {
  return std::make_shared<ListType>(column);
}

TypePtr DataTypes::MakeStructType(const std::vector<std::shared_ptr<Column>>& columns) {
  return std::make_shared<StructType>(columns);
}

TypePtr DataTypes::FindTightesetCommonType(TypePtr type1, TypePtr type2) {
  if (type1->Equals(type2)) {
    return type1;
  } else if (IsPrimitive(type1->type) && IsPrimitive(type2->type)) {
    if (IsInteger(type1->type) && IsInteger(type2->type)) {
      return MakeInt64Type();
    } else {
      return MakeDoubleType();
    }
  } else if (IsInteger(type1->type) && type2->type == Type::DECIMAL && std::static_pointer_cast<DecimalType>(type2)->IsWiderThan(type1)) {
    return type2;
  } else if (IsInteger(type2->type) && type1->type == Type::DECIMAL && std::static_pointer_cast<DecimalType>(type1)->IsWiderThan(type2)) {
    return type1;
  }else {
    return MakeNullType();
  }
}

bool DecimalType::IsWiderThan(TypePtr type) {
  if (type->type == Type::DECIMAL) {
    std::shared_ptr<DecimalType> dt = std::static_pointer_cast<DecimalType>(type);
    return ((precision - scale) >= (dt->precision - dt->scale)) && (scale >= dt->scale);
  } else if (DataTypes::IsInteger(type->type)) {
    return IsWiderThan(ForType(type));
  } else {
    return false;
  }
}

TypePtr DecimalType::ForType(TypePtr type) {
  switch (type->type) {
    case Type::INT8:
    case Type::UINT8:
      return DataTypes::MakeDecimalType(3, 0);
    case Type::INT16:
    case Type::UINT16:
      return DataTypes::MakeDecimalType(5, 0);
    case Type::INT32:
    case Type::UINT32:
      return DataTypes::MakeDecimalType(10, 0);
    case Type::INT64:
    case Type::UINT64:
      return DataTypes::MakeDecimalType(20, 0);
    case Type::FLOAT:
      return DataTypes::MakeDecimalType(14, 7);
    case Type::DOUBLE:
      return DataTypes::MakeDecimalType(30, 15);
    default:
      throw InvalidCast(std::string("DecimalType can not convert from type[") + 
          std::to_string(static_cast<typename std::underlying_type<Type>::type>(type->type)) + "]");
  }
  return DataTypes::MakeDecimalType(10, 0);
}
bool DataTypes::IsInteger(Type type) {
  switch (type) {
    case Type::UINT8:
    case Type::INT8:
    case Type::UINT16:
    case Type::INT16:
    case Type::UINT32:
    case Type::INT32:
    case Type::UINT64:
    case Type::INT64:
      return true;
    default:
      break;
  }
  return false;
}
bool DataTypes::IsFloating(Type type) {
  switch (type) {
    case Type::FLOAT:
    case Type::DOUBLE:
      return true;
    default:
      break;
  }
  return false;
}
bool DataTypes::IsPrimitive(Type type) {
  return IsInteger(type) || IsFloating(type);
}

std::string StructType::ToString() const {
  std::stringstream s;
  s << "struct<";
  for (int i = 0; i < this->num_children(); ++i) {
    if (i > 0) { s << ", "; }
    std::shared_ptr<Column> column = this->child(i);
    s << column->name() << ": " << column->data_type()->ToString();
  }
  s << ">";
  return s.str();
}
std::string MapType::ToString() const {
  std::stringstream s;
  s << "map<" << key_type()->ToString()
     << ", " << value_type()->ToString() << ">";
  return s.str();
}

} //namespace mortred
