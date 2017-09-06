#ifndef MORTRED_DATA_FIELD_H
#define MORTRED_DATA_FIELD_H

#include <memory>
#include <boost/any.hpp>
#include "cell.h"

namespace mortred {

class DataType;

struct DataField {
  DataField() {}
  explicit DataField(const std::string& value);
  DataField(std::shared_ptr<Cell> in_cell, const std::shared_ptr<DataType>& in_data_type)
    :cell(in_cell), data_type(in_data_type) {}
  std::shared_ptr<Cell> cell;
  std::shared_ptr<DataType> data_type;
  std::string ToString();
  bool LessThan(const std::shared_ptr<DataField>& data_field);
  bool GreaterThan(const std::shared_ptr<DataField>& data_field);
  bool Equal(const std::shared_ptr<DataField>& data_field);
  bool LessThan(const DataField& data_field);
  bool GreaterThan(const DataField& data_field);
  bool Equal(const DataField& data_field);
};

}

#endif
