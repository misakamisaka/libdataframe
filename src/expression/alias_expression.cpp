#include "expression/alias_expression.h"
#include <glog/logging.h>
#include "column.h"
#include "expression/expression_exception.h"
#include "row.h"
#include "schema.h"
#include "type/type_converter.h"

namespace mortred {
namespace expression{

void ColumnExpr::Resolve(const std::shared_ptr<Schema>& schema) {
  LeafExpression::Resolve(schema);
  index_ = schema->GetIndexByName(column_name_);
  data_type_ = schema->GetColumnByIndex(index_)->data_type();
  nullable_ = schema->GetColumnByIndex(index_)->nullable();
}
std::shared_ptr<DataField> ColumnExpr::Eval(const std::shared_ptr<Row>& row) const {
  std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
  return std::make_shared<DataField>(row->at(index_), data_type_);
}
std::shared_ptr<DataField> AliasExpr::Eval(const std::shared_ptr<Row>& row) const {
  return child_->Eval(row);
}
void ConstantExpr::Resolve(const std::shared_ptr<Schema>& schema) {
  LeafExpression::Resolve(schema);
}
std::shared_ptr<DataField> ConstantExpr::Eval(const std::shared_ptr<Row>&) const {
  if (is_null_) {
    return std::make_shared<DataField>(std::make_shared<Cell>(), data_type_);
  } else {
    return type_cast(data_type_, std::make_shared<DataField>(value_str_));
  }
}
} //namespace mortred
} //namespace expression
