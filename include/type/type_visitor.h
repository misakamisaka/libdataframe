#ifndef MORTRED_TYPE_VISITOR_H
#define MORTRED_TYPE_VISITOR_H

namespace mortred {

class DataType;
class NoExtraMeta;
class PrimitiveCType;
class NullType;
class BooleanType;
class Int8Type;
class Int16Type;
class Int32Type;
class Int64Type;
class UInt8Type;
class UInt16Type;
class UInt32Type;
class UInt64Type;
class FloatType;
class DoubleType;
class StringType;
class BinaryType;
class DateType;
class TimestampType;
class IntervalType;
class DecimalType;
class ListType;
class StructType;
class MapType;

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
} //namespace mortred

#endif //MORTRED_TYPE_VISITOR_H
