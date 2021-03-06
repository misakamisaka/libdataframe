#include "type/type_visitor.h"
#include "type/type.h"
#include "type/type_exception.h"

namespace mortred {

#define TYPE_VISITOR_DEFAULT(TYPE_CLASS)            \
  void TypeVisitor::Visit(const TYPE_CLASS& type) { \
    throw NotImplementedType(type.ToString());      \
  }

TYPE_VISITOR_DEFAULT(NullType);
TYPE_VISITOR_DEFAULT(BooleanType);
TYPE_VISITOR_DEFAULT(Int8Type);
TYPE_VISITOR_DEFAULT(Int16Type);
TYPE_VISITOR_DEFAULT(Int32Type);
TYPE_VISITOR_DEFAULT(Int64Type);
TYPE_VISITOR_DEFAULT(UInt8Type);
TYPE_VISITOR_DEFAULT(UInt16Type);
TYPE_VISITOR_DEFAULT(UInt32Type);
TYPE_VISITOR_DEFAULT(UInt64Type);
TYPE_VISITOR_DEFAULT(FloatType);
TYPE_VISITOR_DEFAULT(DoubleType);
TYPE_VISITOR_DEFAULT(StringType);
TYPE_VISITOR_DEFAULT(BinaryType);
TYPE_VISITOR_DEFAULT(DateType);
TYPE_VISITOR_DEFAULT(TimestampType);
TYPE_VISITOR_DEFAULT(IntervalType);
TYPE_VISITOR_DEFAULT(DecimalType);
TYPE_VISITOR_DEFAULT(ListType);
TYPE_VISITOR_DEFAULT(StructType);
TYPE_VISITOR_DEFAULT(MapType);

}
