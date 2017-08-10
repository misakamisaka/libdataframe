#ifndef DATA_FRAME_EXCEPTION_H
#define DATA_FRAME_EXCEPTION_H

#include "mortred_exception.h"

namespace mortred {
class DataFrameException : public MortredException {
public:
    DataFrameException(const std::string& what_str) 
        : MortredException(what_str) {
        exception_name_ = "DataFrameException:";
    }   
    virtual ~DataFrameException() throw() {}
    virtual const char* what() const throw() {
        return (exception_name_+ err_msg_).c_str();
    }
private:
  std::string exception_name_;
};
}

#endif  //DATA_FRAME_EXCEPTION_H
