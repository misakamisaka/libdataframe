#ifndef MORTRED_SCHEMA_H
#define MORTRED_SCHEMA_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace mortred {

class Column;

//schema is const
class Schema {
 public:
  explicit Schema(const std::vector<std::shared_ptr<Column>>& columns);

  const std::vector<std::shared_ptr<Column>>& Columns() const { return columns_; }

  std::shared_ptr<Column>& GetColumnByIndex(size_t index) { return columns_[index]; }
  const std::shared_ptr<Column>& GetColumnByIndex(size_t index) const { return columns_[index]; }
  std::shared_ptr<Column>& GetColumnByName(const std::string& name);
  int GetIndexByName(const std::string& name);
 private:
  std::vector<std::shared_ptr<Column>> columns_;
  std::unordered_map<std::string, size_t> name_to_index_;
};

}

#endif
