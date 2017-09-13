#ifndef MORTRED_DATA_FIELD_H
#define MORTRED_DATA_FIELD_H

#include <memory>
#include <boost/any.hpp>
#include "cell.h"
#include "type/type.h"

namespace mortred {

struct DataField {
  DataField() : cell_(std::make_shared<Cell>()), data_type_(DataTypes::MakeNullType()) {}
  explicit DataField(const std::string& value);
  DataField(std::shared_ptr<Cell> cell, const std::shared_ptr<DataType>& data_type)
    :cell_(cell), data_type_(data_type) {}
  std::string ToString() const;
  std::shared_ptr<Cell> cell() const { return cell_; }
  std::shared_ptr<DataType> data_type() const { return data_type_; }
  bool LessThan(const std::shared_ptr<DataField>& data_field);
  bool GreaterThan(const std::shared_ptr<DataField>& data_field);
  bool Equal(const std::shared_ptr<DataField>& data_field);
  bool LessThan(const DataField& data_field);
  bool GreaterThan(const DataField& data_field);
  bool Equal(const DataField& data_field);
 private:
  std::shared_ptr<Cell> cell_;
  std::shared_ptr<DataType> data_type_;
};

}

#endif
