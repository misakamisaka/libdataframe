#ifndef MORTRED_ROW_H
#define MORTRED_ROW_H

#include <memory>
#include <vector>
#include "cell.h"

namespace mortred {
class Row {
 public:
  Row(const std::vector<std::shared_ptr<Cell>>& cells)
    :cells_(cells) {}
  std::shared_ptr<Cell> at(size_t index) const { return cells_.at(index); }
  size_t size() const { return cells_.size(); }
 private:
  std::vector<std::shared_ptr<Cell>> cells_;
};
}

#endif
