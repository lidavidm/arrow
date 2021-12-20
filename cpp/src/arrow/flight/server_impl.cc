// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/flight/server_impl.h"

#include <unordered_map>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace flight {
namespace internal {

/// A registry of transport implementations.
class ServerTransportImplRegistry::Impl {
 public:
  arrow::Result<std::unique_ptr<ServerTransportImpl>> GetImplForScheme(
      const std::string& scheme) {
    auto it = factories_.find(scheme);
    if (it == factories_.end()) {
      return Status::KeyError("No transport implementation for", scheme);
    }
    return it->second();
  }
  Status RegisterImpl(const std::string& scheme, Factory factory) {
    auto it = factories_.insert({scheme, std::move(factory)});
    if (!it.second) {
      return Status::Invalid("Transport already registered for ", scheme);
    }
    return Status::OK();
  }

 private:
  std::unordered_map<std::string, ServerTransportImplRegistry::Factory> factories_;
};

ServerTransportImplRegistry::ServerTransportImplRegistry() {
  impl_ = arrow::internal::make_unique<Impl>();
}
arrow::Result<std::unique_ptr<ServerTransportImpl>>
ServerTransportImplRegistry::GetImplForScheme(const std::string& scheme) {
  return impl_->GetImplForScheme(scheme);
}
Status ServerTransportImplRegistry::RegisterImpl(const std::string& scheme,
                                                 Factory factory) {
  return impl_->RegisterImpl(scheme, std::move(factory));
}

ServerTransportImplRegistry* GetDefaultServerTransportImplRegistry() {
  static std::unique_ptr<ServerTransportImplRegistry> kRegistry =
      arrow::internal::make_unique<ServerTransportImplRegistry>();
  return kRegistry.get();
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
