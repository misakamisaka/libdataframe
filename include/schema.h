#ifndef MORTRED_SCHEMA_H
#define MORTRED_SCHEMA_H

namespace mortred {

class Column;
class Schema {

private:
    std::vector<std::shared_ptr<Column>> columns_;
};

}

#endif
