#ifndef TYPE_EXCEPTION_H
#define TYPE_EXCEPTION_H

#include "mortred_exception.h"

namespace mortred {
class TypeException : public MortredException {
public:
    TypeException(const std::string& what_str) 
        : MortredException(what_str) {
        exception_name_ = "TypeException:";
    }   
    virtual ~TypeException() throw() {}
    virtual const char* what() const throw() {
        return (exception_name_+ err_msg_).c_str();
    }
private:
  std::string exception_name_;
};

class NotImplementedType : public TypeException {
public:
    NotImplementedType(const std::string& what_str) 
        : TypeException(what_str) {
        exception_name_ = "NotImplementedType:";
    }   
    virtual ~NotImplementedType() throw() {}
    virtual const char* what() const throw() {
        return (exception_name_+ err_msg_).c_str();
    }
private:
  std::string exception_name_;
};

class InvalidCast : public TypeException {
 public:
  InvalidCast(const std::string& what_str) 
    : TypeException(what_str) {
      exception_name_ = "InvalidCast:";
    }   
  virtual ~InvalidCast() throw() {}
  virtual const char* what() const throw() {
    return (exception_name_+ err_msg_).c_str();
  }
 private:
  std::string exception_name_;
};
}

#endif  //TYPE_EXCEPTION_H
