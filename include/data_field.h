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
  std::shared_ptr<Cell> cell;
  std::shared_ptr<DataType> data_type;
  std::string ToString();
};

}

#endif
