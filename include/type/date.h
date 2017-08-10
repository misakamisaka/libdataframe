#ifndef MORTRED_DATE_H
#define MORTRED_DATE_H

namespace mortred {
class Date {
 public:
  std::string ToString();
 private:
  //             year      |m   |d
  //00000000000000000000000|0000|00000
  int32_t date_;
};

} // namespace mortred

#endif //MORTRED_DATE_H
