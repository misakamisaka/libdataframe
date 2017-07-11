#ifndef MORTRED_CELL_H
#define MORTRED_CELL_H

#include <boost/any.hpp>

namespace mortred {

struct Cell {
    bool is_null;
    boost::any value;
};

}

#endif
