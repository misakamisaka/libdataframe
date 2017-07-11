#ifndef MORTRED_TYPE_VISITOR_H
#define MORTRED_TYPE_VISITOR_H
#include "type/type.h"

namespace mortred {

class TypeVisitor {
 public:
  virtual ~TypeVisitor() = default;

  virtual void Visit(const NullType& type);
  virtual void Visit(const BooleanType& type);
  virtual void Visit(const Int8Type& type);
  virtual void Visit(const Int16Type& type);
  virtual void Visit(const Int32Type& type);
  virtual void Visit(const Int64Type& type);
  virtual void Visit(const UInt8Type& type);
  virtual void Visit(const UInt16Type& type);
  virtual void Visit(const UInt32Type& type);
  virtual void Visit(const UInt64Type& type);
  virtual void Visit(const FloatType& type);
  virtual void Visit(const DoubleType& type);
  virtual void Visit(const StringType& type);
  virtual void Visit(const BinaryType& type);
  virtual void Visit(const DateType& type);
  virtual void Visit(const TimestampType& type);
  virtual void Visit(const IntervalType& type);
  virtual void Visit(const DecimalType& type);
  virtual void Visit(const ListType& type);
  virtual void Visit(const StructType& type);
  virtual void Visit(const MapType& type);
};


#define TYPE_VISIT_INLINE(TYPE_CLASS) \
  case TYPE_CLASS::type_id:           \
    visitor->Visit(static_cast<const TYPE_CLASS&>(type)); \
    return;

template <typename VISITOR>
inline void VisitTypeInline(const DataType& type, VISITOR* visitor) {
  switch (type.type) {
    TYPE_VISIT_INLINE(NullType);
    TYPE_VISIT_INLINE(BooleanType);
    TYPE_VISIT_INLINE(Int8Type);
    TYPE_VISIT_INLINE(UInt8Type);
    TYPE_VISIT_INLINE(Int16Type);
    TYPE_VISIT_INLINE(UInt16Type);
    TYPE_VISIT_INLINE(Int32Type);
    TYPE_VISIT_INLINE(UInt32Type);
    TYPE_VISIT_INLINE(Int64Type);
    TYPE_VISIT_INLINE(UInt64Type);
    TYPE_VISIT_INLINE(FloatType);
    TYPE_VISIT_INLINE(DoubleType);
    TYPE_VISIT_INLINE(StringType);
    TYPE_VISIT_INLINE(BinaryType);
    TYPE_VISIT_INLINE(DateType);
    TYPE_VISIT_INLINE(TimestampType);
    TYPE_VISIT_INLINE(IntervalType);
    TYPE_VISIT_INLINE(DecimalType);
    TYPE_VISIT_INLINE(ListType);
    TYPE_VISIT_INLINE(StructType);
    TYPE_VISIT_INLINE(MapType);
    default:
      break;
  }
  throw NotImplementedType("Type not implemented, type[" + type.type + "]");
}

class TypeEqualsVisitor {
 public:
  explicit TypeEqualsVisitor(const DataType& right) : right_(right), result_(false) {}

  void VisitChildren(const DataType& left) {
    if (left.num_children() != right_.num_children()) {
      result_ = false;
      return;
    }

    for (int i = 0; i < left.num_children(); ++i) {
      if (!left.child(i)->Equals(right_.child(i))) {
        result_ = false;
        return;
      }
    }
    result_ = true;
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<NoExtraMeta, T>::value ||
                            std::is_base_of<PrimitiveCType, T>::value,
      void>::type
  Visit(const T& type) {
    result_ = true;
  }

  void Visit(const TimestampType& left) {
    const auto& right = static_cast<const TimestampType&>(right_);
    result_ = left.unit == right.unit;
  }

  void Visit(const IntervalType& left) {
    const auto& right = static_cast<const IntervalType&>(right_);
    result_ = left.unit == right.unit;
  }

  Status Visit(const DecimalType& left) {
    const auto& right = static_cast<const DecimalType&>(right_);
    result_ = left.precision == right.precision && left.scale == right.scale;
  }

  Status Visit(const ListType& left) { VisitChildren(left); }

  Status Visit(const StructType& left) { VisitChildren(left); }

  Status Visit(const MapType& left) {
    const auto& right = static_cast<const MapType&>(right_);
    result_ = left.key_type()->Equals(right.key_type()) &&
              left.value_type()->Equals(right.value_type());
  }

  bool result() const { return result_; }

 protected:
  const DataType& right_;
  bool result_;
};

} //namespace mortred

#endif //MORTRED_TYPE_VISITOR_H
