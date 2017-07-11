#ifndef MORTRED_DATA_FIELD_H
#define MORTRED_DATA_FIELD_H

#include <boost/any.hpp>

namespace mortred {

struct DataField {
  std::shared_ptr<Cell> cell;
  std::shared_ptr<DataType> data_type;
};

}

#endif
