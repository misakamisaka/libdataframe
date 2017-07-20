#ifndef MORTRED_COLUMN_H
#define MORTRED_COLUMN_H

#include <memory>
#include <string>

namespace mortred {

class DataType;

class Column {
public:
    Column(const std::string& name, const std::shared_ptr<DataType>& data_type, bool nullable = true)
        :name_(name), data_type_(data_type), nullable_(nullable) {}
    Column()
        :name_(""), nullable_(true) {}
    bool Equals(const Column& other) const;
    bool Equals(const std::shared_ptr<Column>& other) const;

    std::string ToString() const {return "";}

    std::shared_ptr<DataType> data_type() { return data_type_; }

    std::string name() const { return name_; }
    std::shared_ptr<DataType> data_type() const { return data_type_; }
private:
    std::string name_;
    std::shared_ptr<DataType> data_type_;
    bool nullable_;
};
}
#endif
