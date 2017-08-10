#ifndef MORTRED_DECIMAL_H
#define MORTRED_DECIMAL_H

namespace mortred {
class Decimal {
 public:
  std::string ToString();
 private:
  int64_t precision_num_;
  int64_t scale_num_; 
};

} // namespace mortred

#endif //MORTRED_DECIMAL_H
