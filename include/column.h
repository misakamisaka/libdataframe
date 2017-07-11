#ifndef MORTRED_SCHEMA_H
#define MORTRED_SCHEMA_H

#include "type.h"

namespace mortred {
class Column {
public:
    Column(const std::string& name, const DataType& type, bool nullable = true)
        :name_(name), type_(type), nullable_(nullable) {}
    Column()
        :name_(""), nullable_(true) {}
    bool Equals(const Column& other) const;
    bool Equals(const std::shared_ptr<Column>& other) const;

    std::string ToString() const;

    std::shared_ptr<DataType> data_type() { return data_type_; }
private:
    std::string name_;
    std::shared_ptr<DataType> data_type_;
    bool nullable_;
};
}
#endif
