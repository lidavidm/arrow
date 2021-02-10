#include <chrono>
#include <iostream>
#include <string>
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {
namespace span {

class ARROW_EXPORT Span {
public:
// TODO: somehow require a &'static str
explicit Span(const std::string& name, const std::string& tag, const int64_t size) : name_(name), tag_(tag), size_(size), start_(std::chrono::steady_clock::now().time_since_epoch().count()) {
}
~Span();

private:
std::string name_;
std::string tag_;
int64_t size_;
int64_t start_;
};

}
}
}
