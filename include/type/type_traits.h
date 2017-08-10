#ifndef MORTRED_TYPE_TRAITS_H
#define MORTRED_TYPE_TRAITS_H

#include "type.h"

namespace mortred {

template<typename TypeClass>
class NumericConverter;

using BooleanConverter = NumericConverter<BooleanType>;
using UInt8Converter = NumericConverter<UInt8Type>;
using Int8Converter = NumericConverter<Int8Type>;
using UInt16Converter = NumericConverter<UInt16Type>;
using Int16Converter = NumericConverter<Int16Type>;
using UInt32Converter = NumericConverter<UInt32Type>;
using Int32Converter = NumericConverter<Int32Type>;
using UInt64Converter = NumericConverter<UInt64Type>;
using Int64Converter = NumericConverter<Int64Type>;
using FloatConverter = NumericConverter<FloatType>;
using DoubleConverter = NumericConverter<DoubleType>;

class DateConverter;
class TimestampConverter;
class IntervalConverter;
class DecimalConverter;
class StringConverter;
using BinaryConverter = StringConverter;

template <typename T>
struct TypeTraits {};

template <>
struct TypeTraits<NullType> {
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<UInt8Type> {
  using ConverterType = UInt8Converter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeUInt8Type(); }
};

template <>
struct TypeTraits<Int8Type> {
  using ConverterType = Int8Converter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeInt8Type(); }
};

template <>
struct TypeTraits<UInt16Type> {
  using ConverterType = UInt16Converter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeUInt16Type(); }
};

template <>
struct TypeTraits<Int16Type> {
  using ConverterType = Int16Converter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeInt16Type(); }
};

template <>
struct TypeTraits<UInt32Type> {
  using ConverterType = UInt32Converter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeUInt32Type(); }
};

template <>
struct TypeTraits<Int32Type> {
  using ConverterType = Int32Converter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeInt32Type(); }
};

template <>
struct TypeTraits<UInt64Type> {
  using ConverterType = UInt64Converter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeUInt64Type(); }
};

template <>
struct TypeTraits<Int64Type> {
  using ConverterType = Int64Converter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeInt64Type(); }
};

template <>
struct TypeTraits<DateType> {
  using ConverterType = DateConverter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeDateType(); }
};

template <>
struct TypeTraits<TimestampType> {
  using ConverterType = TimestampConverter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeTimestampType(); }
};

template <>
struct TypeTraits<IntervalType> {
  using ConverterType = IntervalConverter;
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<FloatType> {
  using ConverterType = FloatConverter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeFloatType(); }
};

template <>
struct TypeTraits<DoubleType> {
  using ConverterType = DoubleConverter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeDoubleType(); }
};

template <>
struct TypeTraits<BooleanType> {
  using ConverterType = BooleanConverter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeBooleanType(); }
};

template <>
struct TypeTraits<StringType> {
  using ConverterType = StringConverter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeStringType(); }
};

template <>
struct TypeTraits<BinaryType> {
  using ConverterType = BinaryConverter;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return DataTypes::MakeBinaryType(); }
};

template <>
struct TypeTraits<ListType> {
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<StructType> {
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<MapType> {
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<DecimalType> {
  using ConverterType = DecimalConverter;
  constexpr static bool is_parameter_free = false;
};
}

#endif
