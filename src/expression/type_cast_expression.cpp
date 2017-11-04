#include "expression/type_cast_expression.h"
#include <glog/logging.h>
#include "column.h"
#include "expression/expression_exception.h"
#include "row.h"
#include "schema.h"
#include "type/type_converter.h"

namespace mortred {
namespace expression{

void TypeCastExpr::Resolve(const std::shared_ptr<Schema>& schema) {
  child_->Resolve(schema);
  nullable_ = child_->nullable();
  resolved_ = true;
}
std::shared_ptr<DataField> TypeCastExpr::Eval(const std::shared_ptr<Row>& row) const {
  return type_cast(data_type_, child_->Eval(row));
}
} //namespace mortred
} //namespace expression
