#include "data_field.h"
#include "type/type.h"

namespace mortred {

DataField::DataField(const std::string& value) {
  data_type = DataTypes::MakeStringType();
  cell = std::make_shared<Cell>();
  cell->is_null = false;
  cell->value = value;
}

std::string DataField::ToString() {
}

}
