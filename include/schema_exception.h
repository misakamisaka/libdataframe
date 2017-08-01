#ifndef SCHEMA_EXCEPTION_H
#define SCHEMA_EXCEPTION_H

#include "mortred_exception.h"

namespace mortred {
class SchemaException : public MortredException {
public:
    SchemaException(const std::string& what_str) 
        : MortredException(what_str) {
        exception_name_ = "SchemaException:";
    }   
    virtual ~SchemaException() throw() {}
    virtual const char* what() const throw() {
        return (exception_name_+ err_msg_).c_str();
    }
private:
  std::string exception_name_;
};

}

#endif  //SCHEMA_EXCEPTION_H
