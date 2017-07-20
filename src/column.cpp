#include "column.h"
#include "type/type.h"

namespace mortred {

bool Column::Equals(const Column& other) const {
  return (this == &other) ||
    (this->name_ == other.name_ && this->nullable_ == other.nullable_ &&
     this->data_type_->Equals(*other.data_type_.get())); 
}
bool Column::Equals(const std::shared_ptr<Column>& other) const {
  return Equals(*other);
}
}
