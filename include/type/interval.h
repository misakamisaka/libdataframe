#ifndef MORTRED_INTERVAL_H
#define MORTRED_INTERVAL_H

namespace mortred {
class Interval {
 public:
  std::string ToString();
 private:
  int64_t interval_;
};

} // namespace mortred

#endif //MORTRED_INTERVAL_H
