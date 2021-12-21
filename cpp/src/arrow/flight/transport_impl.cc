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

#include "arrow/flight/transport_impl.h"

#include <unordered_map>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace flight {
namespace internal {

class TransportImplRegistry::Impl {
 public:
  arrow::Result<std::unique_ptr<ClientTransportImpl>> MakeClientImpl(
      const std::string& scheme) {
    auto it = client_factories_.find(scheme);
    if (it == client_factories_.end()) {
      return Status::KeyError("No transport implementation for", scheme);
    }
    return it->second();
  }
  arrow::Result<std::unique_ptr<ServerTransportImpl>> MakeServerImpl(
      const std::string& scheme) {
    auto it = server_factories_.find(scheme);
    if (it == server_factories_.end()) {
      return Status::KeyError("No transport implementation for", scheme);
    }
    return it->second();
  }
  Status RegisterClient(const std::string& scheme, ClientFactory factory) {
    auto it = client_factories_.insert({scheme, std::move(factory)});
    if (!it.second) {
      return Status::Invalid("Transport already registered for ", scheme);
    }
    return Status::OK();
  }
  Status RegisterServer(const std::string& scheme, ServerFactory factory) {
    auto it = server_factories_.insert({scheme, std::move(factory)});
    if (!it.second) {
      return Status::Invalid("Transport already registered for ", scheme);
    }
    return Status::OK();
  }

 private:
  std::unordered_map<std::string, TransportImplRegistry::ClientFactory> client_factories_;
  std::unordered_map<std::string, TransportImplRegistry::ServerFactory> server_factories_;
};

TransportImplRegistry::TransportImplRegistry() {
  impl_ = arrow::internal::make_unique<Impl>();
}
arrow::Result<std::unique_ptr<ClientTransportImpl>> TransportImplRegistry::MakeClientImpl(
    const std::string& scheme) {
  return impl_->MakeClientImpl(scheme);
}
arrow::Result<std::unique_ptr<ServerTransportImpl>> TransportImplRegistry::MakeServerImpl(
    const std::string& scheme) {
  return impl_->MakeServerImpl(scheme);
}
Status TransportImplRegistry::RegisterClient(const std::string& scheme,
                                             ClientFactory factory) {
  return impl_->RegisterClient(scheme, std::move(factory));
}
Status TransportImplRegistry::RegisterServer(const std::string& scheme,
                                             ServerFactory factory) {
  return impl_->RegisterServer(scheme, std::move(factory));
}

TransportImplRegistry* GetDefaultTransportImplRegistry() {
  static std::unique_ptr<TransportImplRegistry> kRegistry =
      arrow::internal::make_unique<TransportImplRegistry>();
  return kRegistry.get();
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
