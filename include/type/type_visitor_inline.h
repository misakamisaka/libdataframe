#ifndef MORTRED_TYPE_VISITOR_INLINE_H
#define MORTRED_TYPE_VISITOR_INLINE_H

#include "type.h"
#include "type_exception.h"

namespace mortred {
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
  throw NotImplementedType(std::string("Type not implemented, type[") + std::to_string(static_cast<typename std::underlying_type<Type>::type>(type.type)) + "]");
}
}

#endif
