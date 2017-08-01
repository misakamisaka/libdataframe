#include "schema.h"
#include "column.h"
#include "schema_exception.h"

namespace mortred {
Schema::Schema(const std::vector<std::shared_ptr<Column>>& columns)
  :columns_(columns) {
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

int Schema::GetIndexByName(const std::string& name) {
  auto it = name_to_index_.find(name);
  if (it == name_to_index_.end()) {
    throw SchemaException("schema has not column named[" + name + "]");
  } else {
    return it->second;
  }
}
}
