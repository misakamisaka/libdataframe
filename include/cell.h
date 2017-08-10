#ifndef MORTRED_CELL_H
#define MORTRED_CELL_H

#include <boost/any.hpp>

namespace mortred {

//cell is const
class Cell {
 public:
  Cell() 
  :is_null_(true), value_(boost::any()) {}
  Cell(bool is_null, const boost::any& value) 
  :is_null_(is_null), value_(value) {}
  bool is_null() const { return is_null_; }
  boost::any value() const { return value_; }
 private:
    bool is_null_;
    boost::any value_;
};

}

#endif
