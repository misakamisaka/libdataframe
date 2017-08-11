#ifndef DATA_FIELD_EXCEPTION_H
#define DATA_FIELD_EXCEPTION_H

#include "mortred_exception.h"

namespace mortred {
class DataFieldException : public MortredException {
public:
    DataFieldException(const std::string& what_str) 
        : MortredException(what_str) {
        exception_name_ = "DataFieldException:";
    }   
    virtual ~DataFieldException() throw() {}
    virtual const char* what() const throw() {
        return (exception_name_+ err_msg_).c_str();
    }
private:
  std::string exception_name_;
};
}

#endif  //DATA_FIELD_EXCEPTION_H
