#ifndef MORTRED_TYPE_CONVERTER_H
#define MORTRED_TYPE_CONVERTER_H

#include <string>
#include <boost/any.hpp>
#include "type.h"
#include "type_exception.h"
#include "type_traits.h"
#include "data_field.h"

namespace mortred {

class BaseConverter {
 public:
  virtual std::shared_ptr<DataField> ConvertFrom(const std::shared_ptr<DataField>& src) = 0;
  virtual std::shared_ptr<DataField> ConvertFrom(const DataField& src) = 0;
};

template<typename T>
class NumericConverter : public BaseConverter {
 public:
  explicit NumericConverter(const std::shared_ptr<T>& data_type) : data_type_(data_type) { }
  virtual std::shared_ptr<DataField> ConvertFrom(const std::shared_ptr<DataField>& src) override;
  virtual std::shared_ptr<DataField> ConvertFrom(const DataField& src);
 private:
  std::shared_ptr<T> data_type_;
};

template<typename T>
T my_string_cast(const std::string& data) {
  return T(data);
}
template<>
inline bool my_string_cast<bool>(const std::string& data) {
  char* endptr;
  bool ret = static_cast<bool>(strtol(data.c_str(), &endptr, 10));
  if (endptr != data.c_str() + data.size()) {
    throw InvalidCast("unsupported cast to bool, data:" + data);
  }
  return ret;
}
template<>
inline int8_t my_string_cast<int8_t>(const std::string& data) {
  char* endptr;
  long ret = strtol(data.c_str(), &endptr, 10);
  if (endptr != data.c_str() + data.size()) {
    throw InvalidCast("unsupported cast to int8_t, data:" + data);
  } else if (errno==ERANGE) {
    throw InvalidCast("unsupported cast to int8_t, data:" + data);
  } else if (ret > std::numeric_limits<int8_t>::max() ||
      ret < std::numeric_limits<int8_t>::min()) {
    throw InvalidCast("unsupported cast to int8_t, data:" + data);
  }
  return static_cast<int8_t>(ret);
}
template<>
inline uint8_t my_string_cast<uint8_t>(const std::string& data) {
  char* endptr;
  unsigned long ret = strtoul(data.c_str(), &endptr, 10);
  if (endptr != data.c_str() + data.size()) {
    throw InvalidCast("unsupported cast to uint8_t, data:" + data);
  } else if (errno==ERANGE) {
    throw InvalidCast("unsupported cast to uint8_t, data:" + data);
  } else if (ret > std::numeric_limits<uint8_t>::max() ||
      ret < std::numeric_limits<uint8_t>::min()) {
    throw InvalidCast("unsupported cast to uint8_t, data:" + data);
  }
  return static_cast<uint8_t>(ret);
}
template<>
inline int16_t my_string_cast<int16_t>(const std::string& data) {
  char* endptr;
  long ret = strtol(data.c_str(), &endptr, 10);
  if (endptr != data.c_str() + data.size()) {
    throw InvalidCast("unsupported cast to int16_t, data:" + data);
  } else if (errno==ERANGE) {
    throw InvalidCast("unsupported cast to int16_t, data:" + data);
  } else if (ret > std::numeric_limits<int16_t>::max() ||
      ret < std::numeric_limits<int16_t>::min()) {
    throw InvalidCast("unsupported cast to int16_t, data:" + data);
  }
  return static_cast<int16_t>(ret);
}
template<>
inline uint16_t my_string_cast<uint16_t>(const std::string& data) {
  char* endptr;
  unsigned long ret = strtoul(data.c_str(), &endptr, 10);
  if (endptr != data.c_str() + data.size()) {
    throw InvalidCast("unsupported cast to uint16_t, data:" + data);
  } else if (errno==ERANGE) {
    throw InvalidCast("unsupported cast to uint16_t, data:" + data);
  } else if (ret > std::numeric_limits<uint16_t>::max() ||
      ret < std::numeric_limits<uint16_t>::min()) {
    throw InvalidCast("unsupported cast to uint16_t, data:" + data);
  }
  return static_cast<uint16_t>(ret);
}
template<>
inline int32_t my_string_cast<int32_t>(const std::string& data) {
  char* endptr;
  long ret = strtol(data.c_str(), &endptr, 10);
  if (endptr != data.c_str() + data.size()) {
    throw InvalidCast("unsupported cast to int32_t, data:" + data);
  } else if (errno==ERANGE) {
    throw InvalidCast("unsupported cast to int32_t, data:" + data);
  } else if (ret > std::numeric_limits<int32_t>::max() ||
      ret < std::numeric_limits<int32_t>::min()) {
    throw InvalidCast("unsupported cast to int32_t, data:" + data);
  }
  return static_cast<int32_t>(ret);
}
template<>
inline uint32_t my_string_cast<uint32_t>(const std::string& data) {
  char* endptr;
  unsigned long ret = strtoul(data.c_str(), &endptr, 10);
  if (endptr != data.c_str() + data.size()) {
    throw InvalidCast("unsupported cast to uint32_t, data:" + data);
  } else if (errno==ERANGE) {
    throw InvalidCast("unsupported cast to uint32_t, data:" + data);
  } else if (ret > std::numeric_limits<uint32_t>::max() ||
      ret < std::numeric_limits<uint32_t>::min()) {
    throw InvalidCast("unsupported cast to uint32_t, data:" + data);
  }
  return static_cast<uint32_t>(ret);
}
template<>
inline int64_t my_string_cast<int64_t>(const std::string& data) {
  char* endptr;
  long long ret = strtoll(data.c_str(), &endptr, 10);
  if (endptr != data.c_str() + data.size()) {
    throw InvalidCast("unsupported cast to int64_t, data:" + data);
  } else if (errno==ERANGE) {
    throw InvalidCast("unsupported cast to int64_t, data:" + data);
  } else if (ret > std::numeric_limits<int64_t>::max() ||
      ret < std::numeric_limits<int64_t>::min()) {
    throw InvalidCast("unsupported cast to int64_t, data:" + data);
  }
  return static_cast<int64_t>(ret);
}
template<>
inline uint64_t my_string_cast<uint64_t>(const std::string& data) {
  char* endptr;
  unsigned long long ret = strtoull(data.c_str(), &endptr, 10);
  if (endptr != data.c_str() + data.size()) {
    throw InvalidCast("unsupported cast to uint64_t, data:" + data);
  } else if (errno==ERANGE) {
    throw InvalidCast("unsupported cast to uint64_t, data:" + data);
  } else if (ret > std::numeric_limits<uint64_t>::max() ||
      ret < std::numeric_limits<uint64_t>::min()) {
    throw InvalidCast("unsupported cast to uint64_t, data:" + data);
  }
  return static_cast<uint64_t>(ret);
}
template<>
inline double my_string_cast<double>(const std::string& data) {
  char* endptr;
  double ret = strtod(data.c_str(), &endptr);
  if (endptr != data.c_str() + data.size()) {
    throw InvalidCast("unsupported cast to double, data:" + data);
  } else if (errno==ERANGE) {
    throw InvalidCast("unsupported cast to double, data:" + data);
  }
  return ret;
}
template<>
inline float my_string_cast<float>(const std::string& data) {
  char* endptr;
  float ret = strtof(data.c_str(), &endptr);
  if (endptr != data.c_str() + data.size()) {
    throw InvalidCast("unsupported cast to float, data:" + data);
  } else if (errno==ERANGE) {
    throw InvalidCast("unsupported cast to float, data:" + data);
  }
  return ret;
}

template<typename T>
std::shared_ptr<DataField> NumericConverter<T>::ConvertFrom(const std::shared_ptr<DataField>& src) {
  return ConvertFrom(*src);
}
template<typename T>
std::shared_ptr<DataField> NumericConverter<T>::ConvertFrom(const DataField& src) {
  std::shared_ptr<DataField> ret = std::make_shared<DataField>();
  ret->data_type = data_type_;
  bool is_null = src.cell->is_null();
  boost::any value;

#define NUMERIC_CASE(FIELD_TYPE, IN_TYPE) \
  case FIELD_TYPE:                        \
    value = boost::any(static_cast<typename T::c_type>(boost::any_cast<IN_TYPE>(src.cell->value()))); \
    break;

  switch (src.data_type->type) {
    NUMERIC_CASE(Type::BOOL, bool)
      NUMERIC_CASE(Type::UINT8, uint8_t)
      NUMERIC_CASE(Type::INT8, int8_t)
      NUMERIC_CASE(Type::UINT16, uint16_t)
      NUMERIC_CASE(Type::INT16, int16_t)
      NUMERIC_CASE(Type::UINT32, uint32_t)
      NUMERIC_CASE(Type::INT32, int32_t)
      NUMERIC_CASE(Type::UINT64, uint64_t)
      NUMERIC_CASE(Type::INT64, int64_t)
      NUMERIC_CASE(Type::FLOAT, float)
      NUMERIC_CASE(Type::DOUBLE, double)

    case Type::STRING:
    case Type::BINARY:
      value = boost::any(my_string_cast<typename T::c_type>(boost::any_cast<std::string>(src.cell->value())));
      break;

    default:
      throw InvalidCast("unsupported cast [" + std::to_string(static_cast<typename std::underlying_type<Type>::type>(src.data_type->type)) + "] in numeric converter");
  }
#undef NUMERIC_CASE
  ret->cell = std::make_shared<Cell>(is_null, value);
  return ret;
}

class StringConverter : public BaseConverter {
 public:
  explicit StringConverter(const std::shared_ptr<StringType>& data_type) : data_type_(data_type) { }
  explicit StringConverter(const std::shared_ptr<BinaryType>& data_type) : data_type_(data_type) { }
  virtual std::shared_ptr<DataField> ConvertFrom(const DataField& src) override {
    std::shared_ptr<DataField> ret = std::make_shared<DataField>();
    ret->data_type = data_type_;
    bool is_null = src.cell->is_null();
    boost::any value;

#define NUMERIC_CASE(FIELD_TYPE, IN_TYPE) \
    case FIELD_TYPE:                      \
      value = boost::any(std::to_string(boost::any_cast<IN_TYPE>(src.cell->value()))); \
      break;

    switch (src.data_type->type) {
      NUMERIC_CASE(Type::BOOL, bool)
        NUMERIC_CASE(Type::UINT8, uint8_t)
        NUMERIC_CASE(Type::INT8, int8_t)
        NUMERIC_CASE(Type::UINT16, uint16_t)
        NUMERIC_CASE(Type::INT16, int16_t)
        NUMERIC_CASE(Type::UINT32, uint32_t)
        NUMERIC_CASE(Type::INT32, int32_t)
        NUMERIC_CASE(Type::UINT64, uint64_t)
        NUMERIC_CASE(Type::INT64, int64_t)
        NUMERIC_CASE(Type::FLOAT, float)
        NUMERIC_CASE(Type::DOUBLE, double)

      case Type::STRING:
      case Type::BINARY:
        value = src.cell->value();
        break;

      default:
        throw InvalidCast("unsupported cast [" + std::to_string(static_cast<typename std::underlying_type<Type>::type>(src.data_type->type)) + "] in string converter");
    }
#undef NUMERIC_CASE
    ret->cell = std::make_shared<Cell>(is_null, value);
    return ret;
  }
  virtual std::shared_ptr<DataField> ConvertFrom(const std::shared_ptr<DataField>& src) override {
      return ConvertFrom(*src);
    }
 private:
  std::shared_ptr<DataType> data_type_;
};

template<typename T>
std::shared_ptr<DataField> type_cast(const std::shared_ptr<T>& data_type, const std::shared_ptr<DataField>& data_field) {
  typename TypeTraits<T>::ConverterType converter(data_type);
  return converter.ConvertFrom(data_field);
}
inline std::shared_ptr<DataField> type_cast(const std::shared_ptr<DataType>& data_type, const std::shared_ptr<DataField>& data_field) {
  switch (data_type->type) {
    case Type::BOOL:
      return type_cast(std::static_pointer_cast<BooleanType>(data_type), data_field);
    case Type::UINT8:
      return type_cast(std::static_pointer_cast<UInt8Type>(data_type), data_field);
    case Type::INT8:
      return type_cast(std::static_pointer_cast<Int8Type>(data_type), data_field);
    case Type::UINT16:
      return type_cast(std::static_pointer_cast<UInt16Type>(data_type), data_field);
    case Type::INT16:
      return type_cast(std::static_pointer_cast<Int16Type>(data_type), data_field);
    case Type::UINT32:
      return type_cast(std::static_pointer_cast<UInt32Type>(data_type), data_field);
    case Type::INT32:
      return type_cast(std::static_pointer_cast<Int32Type>(data_type), data_field);
    case Type::UINT64:
      return type_cast(std::static_pointer_cast<UInt64Type>(data_type), data_field);
    case Type::INT64:
      return type_cast(std::static_pointer_cast<Int64Type>(data_type), data_field);
    case Type::FLOAT:
      return type_cast(std::static_pointer_cast<FloatType>(data_type), data_field);
    case Type::DOUBLE:
      return type_cast(std::static_pointer_cast<DoubleType>(data_type), data_field);
    case Type::STRING:
      return type_cast(std::static_pointer_cast<StringType>(data_type), data_field);
    case Type::BINARY:
      return type_cast(std::static_pointer_cast<BinaryType>(data_type), data_field);
    default:
      throw InvalidCast("unsupported cast [" + std::to_string(static_cast<typename std::underlying_type<Type>::type>(data_type->type)) + "] in type_cast");
  }
  return nullptr;
}

} //namespace mortred

#endif
