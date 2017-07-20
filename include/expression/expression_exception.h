#ifndef EXPRESSION_EXCEPTION_H
#define EXPRESSION_EXCEPTION_H

#include "mortred_exception.h"

namespace mortred {
class ExpressionException : public MortredException {
public:
    ExpressionException(const std::string& what_str) 
        : MortredException(what_str) {
        exception_name_ = "TypeException:";
    }   
    virtual ~ExpressionException() throw() {}
    virtual const char* what() const throw() {
        return (exception_name_+ err_msg_).c_str();
    }
private:
  std::string exception_name_;
};

}

#endif  //EXPRESSION_EXCEPTION_H
