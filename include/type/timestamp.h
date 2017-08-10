#ifndef MORTRED_TIMESTAMP_H
#define MORTRED_TIMESTAMP_H

namespace mortred {
class Timestamp {
 public:
  std::string ToString();
 private:
  int64_t milli_seconds_from_epoch_;
};

} // namespace mortred

#endif //MORTRED_TIMESTAMP_H
