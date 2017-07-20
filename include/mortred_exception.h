#ifndef  MORTRED_EXCEPTION_H
#define  MORTRED_EXCEPTION_H

#include <exception>
#include <string>

namespace mortred {

class MortredException : public std::exception {
public:
    MortredException(const std::string& what_str)
        :err_msg_(what_str) {}
    virtual ~MortredException() throw() {}
    virtual const char* what() {
        return err_msg_.c_str();
    }   
protected:
    std::string err_msg_;
};
}

#endif  //MORTRED_EXCEPTION_H
