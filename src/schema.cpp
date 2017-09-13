#include "schema.h"
#include "column.h"
#include "schema_exception.h"

namespace mortred {
Schema::Schema(const std::vector<std::shared_ptr<Column>>& columns, size_t group_column_size)
  :columns_(columns), group_column_size_(group_column_size) {
  for (size_t i = 0; i < columns_.size(); ++i) {
    name_to_index_[columns_[i]->name()] = static_cast<int>(i);
  }
}
std::shared_ptr<Column>& Schema::GetColumnByName(const std::string& name) {
  auto it = name_to_index_.find(name);
  if (it == name_to_index_.end()) {
    throw SchemaException("schema has not column named[" + name + "]");
  } else {
    return columns_[it->second];
  }
}

size_t Schema::GetIndexByName(const std::string& name) {
  auto it = name_to_index_.find(name);
  if (it == name_to_index_.end()) {
    throw SchemaException("schema has not column named[" + name + "]");
  } else {
    return it->second;
  }
}
bool Schema::Equals(std::shared_ptr<Schema> schema) {
  if (columns_.size() != schema->columns_.size()) {
    return false;
  }
  for (size_t i = 0; i < columns_.size(); ++i) {
    if (!columns_[i]->Equals(schema->columns_[i])) {
      return false;
    }
  }
  return true;
}
}
