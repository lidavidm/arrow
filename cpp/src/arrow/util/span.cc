#include "arrow/util/span.h"
#include <iostream>
#include <thread>

#include "arrow/util/logging.h"

namespace arrow {
namespace util {
namespace span {

Span::~Span() {
  const auto end = std::chrono::steady_clock::now().time_since_epoch().count();
  ARROW_LOG(DEBUG) << "span name=" << name_ << " thread_id=" << std::this_thread::get_id() << " tag=" << tag_ << " size=" << size_ << " start=" << start_ << " end=" << end;
}

}
}
}
