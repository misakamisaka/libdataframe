#include "expression/alias_expression.h"
#include <glog/logging.h>
#include "column.h"
#include "expression/expression_exception.h"
#include "row.h"
#include "schema.h"
#include "type/type_converter.h"

namespace mortred {
namespace expression{

void ColumnExpr::Resolve(std::shared_ptr<Schema> schema) {
  LeafExpression::Resolve(schema);
  index_ = schema->GetIndexByName(column_name_);
  data_type_ = schema->GetColumnByIndex(index_)->data_type();
  nullable_ = schema->GetColumnByIndex(index_)->nullable();
}
std::shared_ptr<DataField> ColumnExpr::Eval(std::shared_ptr<Row> row) {
  std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
  data_field->data_type = data_type_;
  data_field->cell = row->at(index_);
  return data_field;
}
std::shared_ptr<DataField> AliasExpr::Eval(std::shared_ptr<Row> row) {
  return child_->Eval(row);
}
void ConstantExpr::Resolve(std::shared_ptr<Schema> schema) {
  LeafExpression::Resolve(schema);
}
std::shared_ptr<DataField> ConstantExpr::Eval(std::shared_ptr<Row>) {
  std::shared_ptr<DataField> data_field = std::make_shared<DataField>();
  if (is_null_) {
    data_field->data_type = data_type_;
    data_field->cell = std::make_shared<Cell>();
  } else {
    data_field = type_cast(data_type_, std::make_shared<DataField>(value_str_));
  }
  return data_field;
}
} //namespace mortred
} //namespace expression
