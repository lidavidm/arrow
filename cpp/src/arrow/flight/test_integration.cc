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

#include "arrow/flight/test_integration.h"
#include "arrow/flight/client_middleware.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/test_util.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/dictionary.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace arrow {
namespace flight {

Status AssertEqual(const arrow::Schema& expected, const arrow::Schema& actual) {
  if (!expected.Equals(actual)) {
    std::cerr << "Expected schema: " << expected << std::endl;
    std::cerr << "Actual schema  : " << actual << std::endl;
    return Status::Invalid("Schema mismatch");
  }
  return Status::OK();
}

/// \brief The server for the basic auth integration test.
class AuthBasicProtoServer : public FlightServerBase {
  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    // Respond with the authenticated username.
    std::shared_ptr<Buffer> buf;
    RETURN_NOT_OK(Buffer::FromString(context.peer_identity(), &buf));
    *result = std::unique_ptr<ResultStream>(new SimpleResultStream({Result{buf}}));
    return Status::OK();
  }
};

// The expected username for the basic auth integration test.
constexpr auto kAuthUsername = "arrow";
// The expected password for the basic auth integration test.
constexpr auto kAuthPassword = "flight";

/// \brief A scenario testing the basic auth protobuf.
class AuthBasicProtoScenario : public Scenario {
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    server->reset(new AuthBasicProtoServer());
    options->auth_handler =
        std::make_shared<TestServerBasicAuthHandler>(kAuthUsername, kAuthPassword);
    return Status::OK();
  }

  Status MakeClient(FlightClientOptions* options) override { return Status::OK(); }

  Status RunClient(const Location& location,
                   std::unique_ptr<FlightClient> client) override {
    Action action;
    std::unique_ptr<ResultStream> stream;
    std::shared_ptr<FlightStatusDetail> detail;
    const auto& status = client->DoAction(action, &stream);
    detail = FlightStatusDetail::UnwrapStatus(status);
    // This client is unauthenticated and should fail.
    if (detail == nullptr) {
      return Status::Invalid("Expected UNAUTHENTICATED but got ", status.ToString());
    }
    if (detail->code() != FlightStatusCode::Unauthenticated) {
      return Status::Invalid("Expected UNAUTHENTICATED but got ", detail->ToString());
    }

    auto client_handler = std::unique_ptr<ClientAuthHandler>(
        new TestClientBasicAuthHandler(kAuthUsername, kAuthPassword));
    RETURN_NOT_OK(client->Authenticate({}, std::move(client_handler)));
    RETURN_NOT_OK(client->DoAction(action, &stream));
    std::unique_ptr<Result> result;
    RETURN_NOT_OK(stream->Next(&result));
    if (!result) {
      return Status::Invalid("Action result stream ended early");
    }
    const auto username = result->body->ToString();
    if (kAuthUsername != username) {
      return Status::Invalid("Got wrong username; expected", kAuthUsername, "but got",
                             username);
    }
    RETURN_NOT_OK(stream->Next(&result));
    if (result) {
      return Status::Invalid("Action result stream has too many entries");
    }
    return Status::OK();
  }
};

class CancellationServer : public FlightServerBase {
  Status ListFlights(const ServerCallContext& context, const Criteria* criteria,
                     std::unique_ptr<FlightListing>* listings) override {
    // Intentionally do not respond to the request.
    while (!context.IsCancelled()) {
    }
    return Status::OK();
  }
};

class CancellationScenario : public Scenario {
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    server->reset(new CancellationServer());
    return Status::OK();
  }

  Status MakeClient(FlightClientOptions* options) override { return Status::OK(); }

  Status RunClient(const Location& location,
                   std::unique_ptr<FlightClient> client) override {
    FlightCallOptions options;
    options.timeout = TimeoutDuration{3.0};
    Criteria criteria;
    std::unique_ptr<FlightListing> listing;
    const auto& status = client->ListFlights(options, criteria, &listing);
    const auto& detail = FlightStatusDetail::UnwrapStatus(status);
    if (detail == nullptr) {
      return Status::Invalid("Expected TimedOut but got ", status.ToString());
    }
    if (detail->code() != FlightStatusCode::TimedOut) {
      return Status::Invalid("Expected TimedOut but got ", detail->ToString());
    }
    return Status::OK();
  }
};

class ErrorCodesServer : public FlightServerBase {
  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    if (action.type == "UNKNOWN") {
      return Status::UnknownError("Integration test");
    } else if (action.type == "INTERNAL") {
      return MakeFlightError(FlightStatusCode::Internal, "Integration test");
    } else if (action.type == "INVALID_ARGUMENT") {
      return Status::Invalid("Integration test");
    } else if (action.type == "TIMED_OUT") {
      return MakeFlightError(FlightStatusCode::TimedOut, "Integration test");
    } else if (action.type == "NOT_FOUND") {
      return Status::KeyError("Integration test");
    } else if (action.type == "ALREADY_EXISTS") {
      return Status::AlreadyExists("Integration test");
    } else if (action.type == "CANCELLED") {
      return MakeFlightError(FlightStatusCode::Cancelled, "Integration test");
    } else if (action.type == "UNAUTHENTICATED") {
      return MakeFlightError(FlightStatusCode::Unauthenticated, "Integration test");
    } else if (action.type == "UNAUTHORIZED") {
      return MakeFlightError(FlightStatusCode::Unauthorized, "Integration test");
    } else if (action.type == "UNIMPLEMENTED") {
      return Status::NotImplemented("Integration test");
    } else if (action.type == "UNAVAILABLE") {
      return MakeFlightError(FlightStatusCode::Unavailable, "Integration test");
    }
    *result = std::unique_ptr<ResultStream>(new SimpleResultStream({}));
    return Status::OK();
  }
};

class ErrorCodesScenario : public Scenario {
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    server->reset(new ErrorCodesServer());
    return Status::OK();
  }

  Status MakeClient(FlightClientOptions* options) override { return Status::OK(); }

  Status RunClient(const Location& location,
                   std::unique_ptr<FlightClient> client) override {
    struct ExpectedStatus {
      StatusCode code;
      bool has_flight_code;
      FlightStatusCode flight_code;
    };

    const std::vector<std::pair<std::string, ExpectedStatus>> codes{
        {"UNKNOWN", {StatusCode::UnknownError, false, FlightStatusCode::Failed}},
        {"INTERNAL", {StatusCode::IOError, true, FlightStatusCode::Internal}},
        {"INVALID_ARGUMENT", {StatusCode::Invalid, false, FlightStatusCode::Internal}},
        {"TIMED_OUT", {StatusCode::IOError, true, FlightStatusCode::TimedOut}},
        {"NOT_FOUND", {StatusCode::KeyError, false, FlightStatusCode::Failed}},
        {"ALREADY_EXISTS", {StatusCode::AlreadyExists, false, FlightStatusCode::Failed}},
        {"CANCELLED", {StatusCode::IOError, true, FlightStatusCode::Cancelled}},
        {"UNAUTHENTICATED",
         {StatusCode::IOError, true, FlightStatusCode::Unauthenticated}},
        {"UNAUTHORIZED", {StatusCode::IOError, true, FlightStatusCode::Unauthorized}},
        {"UNIMPLEMENTED", {StatusCode::NotImplemented, false, FlightStatusCode::Failed}},
        {"UNAVAILABLE", {StatusCode::IOError, true, FlightStatusCode::Unavailable}},
    };

    Action action;
    std::unique_ptr<ResultStream> stream;
    std::unique_ptr<Result> result;
    for (const auto& error_case : codes) {
      action.type = error_case.first;
      auto status = client->DoAction(action, &stream);
      while (status.ok()) {
        status = stream->Next(&result);
        if (!result) {
          break;
        }
      }
      if (status.code() != error_case.second.code) {
        return Status::Invalid(
            "Expected status ",
            std::to_string(static_cast<int8_t>(error_case.second.code)), " but got ",
            status.ToString(), " (wrong code)");
      }
      if (error_case.second.has_flight_code) {
        const auto detail = FlightStatusDetail::UnwrapStatus(status);
        if (!detail) {
          return Status::Invalid(
              "Expected status ",
              std::to_string(static_cast<int8_t>(error_case.second.flight_code)),
              " but got ", status.ToString(), " (missing Flight code)");
        }
        if (detail->code() != error_case.second.flight_code) {
          return Status::Invalid(
              "Expected status ",
              std::to_string(static_cast<int8_t>(error_case.second.flight_code)),
              " but got ", status.ToString(), " (wrong Flight code)");
        }
      }
    }
    return Status::OK();
  }
};

class TestServerMiddleware : public ServerMiddleware {
 public:
  explicit TestServerMiddleware(std::string received) : received_(received) {}
  void SendingHeaders(AddCallHeaders* outgoing_headers) override {
    outgoing_headers->AddHeader("x-middleware", received_);
  }
  void CallCompleted(const Status& status) override {}

  std::string name() const override { return "GrpcTrailersMiddleware"; }

 private:
  std::string received_;
};

class TestServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  Status StartCall(const CallInfo& info, const CallHeaders& incoming_headers,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>& iter_pair =
        incoming_headers.equal_range("x-middleware");
    std::string received = "";
    if (iter_pair.first != iter_pair.second) {
      const util::string_view& value = (*iter_pair.first).second;
      received = std::string(value);
    }
    *middleware = std::make_shared<TestServerMiddleware>(received);
    return Status::OK();
  }
};

class TestClientMiddleware : public ClientMiddleware {
 public:
  explicit TestClientMiddleware(std::string* received_header)
      : received_header_(received_header) {}

  void SendingHeaders(AddCallHeaders* outgoing_headers) {
    outgoing_headers->AddHeader("x-middleware", "expected value");
  }

  void ReceivedHeaders(const CallHeaders& incoming_headers) {
    const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>& iter_pair =
        incoming_headers.equal_range("x-middleware");
    if (iter_pair.first != iter_pair.second) {
      const util::string_view& value = (*iter_pair.first).second;
      *received_header_ = std::string(value);
    }
  }

  void CallCompleted(const Status& status) {}

 private:
  std::string* received_header_;
};

class TestClientMiddlewareFactory : public ClientMiddlewareFactory {
 public:
  void StartCall(const CallInfo& info, std::unique_ptr<ClientMiddleware>* middleware) {
    *middleware =
        std::unique_ptr<ClientMiddleware>(new TestClientMiddleware(&received_header_));
  }

  std::string received_header_;
};

class MiddlewareServer : public FlightServerBase {
  Status GetFlightInfo(const ServerCallContext& context,
                       const FlightDescriptor& descriptor,
                       std::unique_ptr<FlightInfo>* result) override {
    if (descriptor.type == FlightDescriptor::DescriptorType::CMD &&
        descriptor.cmd == "success") {
      // Don't fail
      std::shared_ptr<Schema> schema = arrow::schema({});
      Location location;
      RETURN_NOT_OK(Location::ForGrpcTcp("localhost", 10010, &location));
      std::vector<FlightEndpoint> endpoints{FlightEndpoint{{"foo"}, {location}}};
      ARROW_ASSIGN_OR_RAISE(auto info,
                            FlightInfo::Make(*schema, descriptor, endpoints, -1, -1));
      *result = std::unique_ptr<FlightInfo>(new FlightInfo(info));
      return Status::OK();
    }
    // Fail the call immediately. In some gRPC implementations, this
    // means that gRPC sends only HTTP/2 trailers and not headers. We want
    // Flight middleware to be agnostic to this difference.
    return Status::UnknownError("Unknown");
  }
};

class MiddlewareScenario : public Scenario {
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    options->middleware.push_back(
        {"grpc_trailers", std::make_shared<TestServerMiddlewareFactory>()});
    server->reset(new MiddlewareServer());
    return Status::OK();
  }

  Status MakeClient(FlightClientOptions* options) override {
    client_middleware_ = std::make_shared<TestClientMiddlewareFactory>();
    options->middleware.push_back(client_middleware_);
    return Status::OK();
  }

  Status RunClient(const Location& location,
                   std::unique_ptr<FlightClient> client) override {
    std::unique_ptr<FlightInfo> info;
    if (client->GetFlightInfo(FlightDescriptor::Command(""), &info).ok()) {
      return Status::Invalid("Expected call to fail");
    }
    if (client_middleware_->received_header_ != "expected value") {
      return Status::Invalid(
          "Expected to receive header x-middleware: expected value, but instead got ",
          client_middleware_->received_header_);
    }
    std::cerr << "Headers received successfully on failing call." << std::endl;

    // This call should succeed
    client_middleware_->received_header_ = "";
    RETURN_NOT_OK(client->GetFlightInfo(FlightDescriptor::Command("success"), &info));
    if (client_middleware_->received_header_ != "expected value") {
      return Status::Invalid(
          "Expected to receive header x-middleware: expected value, but instead got ",
          client_middleware_->received_header_);
    }
    std::cerr << "Headers received successfully on passing call." << std::endl;
    return Status::OK();
  }

  std::shared_ptr<TestClientMiddlewareFactory> client_middleware_;
};

constexpr auto kExpectedCriteria = "criteria";

class RpcMethodsServer : public FlightServerBase {
 public:
  static FlightDescriptor MakeDescriptor1() {
    return FlightDescriptor::Command("\x01\x01\x02\x03\x05");
  }

  static FlightDescriptor MakeDescriptor2() {
    return FlightDescriptor::Path({"foo", "bar"});
  }

  static std::shared_ptr<arrow::Schema> MakeSchema() {
    return arrow::schema({
        arrow::field("timestamp", arrow::timestamp(TimeUnit::NANO), false),
        arrow::field("random", arrow::float64(), true),
        arrow::field("text", arrow::utf8(), true),
    });
  }

  static Ticket MakeTicket() { return Ticket{"foobar"}; }

  static std::vector<ActionType> MakeActions() {
    return {
        {"action1", "Long description"},
        {"action2", "Long description"},
    };
  }

  static arrow::Result<FlightInfo> MakeFlightInfo() {
    Location location;
    RETURN_NOT_OK(Location::ForGrpcTcp("localhost", 9090, &location));
    std::vector<FlightEndpoint> endpoints{FlightEndpoint{MakeTicket(), {location}}};
    return FlightInfo::Make(*MakeSchema(), MakeDescriptor1(), endpoints, -1, -1);
  }

  Status ListFlights(const ServerCallContext& context, const Criteria* criteria,
                     std::unique_ptr<FlightListing>* listings) override {
    if (criteria->expression != kExpectedCriteria) {
      return Status::Invalid("Unexpected criteria:", criteria->expression);
    }
    ARROW_ASSIGN_OR_RAISE(auto info1, MakeFlightInfo());
    ARROW_ASSIGN_OR_RAISE(auto info2, MakeFlightInfo());
    std::vector<FlightInfo> flights{info1, info2};
    *listings = std::unique_ptr<FlightListing>(new SimpleFlightListing(flights));
    return Status::OK();
  }

  Status GetSchema(const ServerCallContext& context, const FlightDescriptor& request,
                   std::unique_ptr<SchemaResult>* schema) override {
    ARROW_ASSIGN_OR_RAISE(*schema, SchemaResult::Make(*MakeSchema()));
    return Status::OK();
  }

  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    // Test serialization/deserialization across languages
    std::string desc1, desc2, info_str, ticket;
    RETURN_NOT_OK(MakeDescriptor1().SerializeToString(&desc1));
    RETURN_NOT_OK(MakeDescriptor2().SerializeToString(&desc2));
    ARROW_ASSIGN_OR_RAISE(auto info, MakeFlightInfo());
    RETURN_NOT_OK(info.SerializeToString(&info_str));
    RETURN_NOT_OK(MakeTicket().SerializeToString(&ticket));

    auto buf1 = Buffer::FromString(std::move(desc1));
    auto buf2 = Buffer::FromString(std::move(desc2));
    auto buf3 = Buffer::FromString(std::move(info_str));
    auto buf4 = Buffer::FromString(std::move(ticket));
    *result = std::unique_ptr<ResultStream>(
        new SimpleResultStream({{buf1}, {buf2}, {buf3}, {buf4}}));
    return Status::OK();
  }

  Status ListActions(const ServerCallContext& context,
                     std::vector<ActionType>* actions) override {
    *actions = MakeActions();
    return Status::OK();
  }
};

class RpcMethodsScenario : public Scenario {
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    server->reset(new RpcMethodsServer());
    return Status::OK();
  }

  Status MakeClient(FlightClientOptions* options) override { return Status::OK(); }

  Status RunClient(const Location& location,
                   std::unique_ptr<FlightClient> client) override {
    std::unique_ptr<SchemaResult> schema_result;
    RETURN_NOT_OK(client->GetSchema(FlightDescriptor::Command(""), &schema_result));
    ipc::DictionaryMemo dictionary_memo;
    std::shared_ptr<Schema> schema;
    RETURN_NOT_OK(schema_result->GetSchema(&dictionary_memo, &schema));
    RETURN_NOT_OK(AssertEqual(*RpcMethodsServer::MakeSchema(), *schema));

    // Test cross-language serialize/deserialize
    Action action;
    std::unique_ptr<ResultStream> stream;
    std::unique_ptr<Result> result;
    std::string result_str;
    FlightDescriptor descriptor;
    RETURN_NOT_OK(client->DoAction(action, &stream));
    RETURN_NOT_OK(stream->Next(&result));
    if (!result) {
      return Status::Invalid("Action result stream ended early");
    }
    result_str = result->body->ToString();
    RETURN_NOT_OK(FlightDescriptor::Deserialize(result_str, &descriptor));
    if (descriptor != RpcMethodsServer::MakeDescriptor1()) {
      return Status::Invalid("Deserialized descriptor does not match!");
    }

    RETURN_NOT_OK(stream->Next(&result));
    if (!result) {
      return Status::Invalid("Action result stream ended early");
    }
    result_str = result->body->ToString();
    RETURN_NOT_OK(FlightDescriptor::Deserialize(result_str, &descriptor));
    if (descriptor != RpcMethodsServer::MakeDescriptor2()) {
      return Status::Invalid("Deserialized descriptor does not match!");
    }

    RETURN_NOT_OK(stream->Next(&result));
    if (!result) {
      return Status::Invalid("Action result stream ended early");
    }
    result_str = result->body->ToString();
    std::unique_ptr<FlightInfo> info;
    RETURN_NOT_OK(FlightInfo::Deserialize(result_str, &info));
    ARROW_ASSIGN_OR_RAISE(auto expected_info, RpcMethodsServer::MakeFlightInfo());
    RETURN_NOT_OK(info->Equals(expected_info));

    RETURN_NOT_OK(stream->Next(&result));
    if (!result) {
      return Status::Invalid("Action result stream ended early");
    }
    result_str = result->body->ToString();
    Ticket ticket;
    RETURN_NOT_OK(Ticket::Deserialize(result_str, &ticket));
    if (ticket != RpcMethodsServer::MakeTicket()) {
      return Status::Invalid("Deserialized ticket does not match!");
    }
    RETURN_NOT_OK(stream->Next(&result));
    if (result) {
      return Status::Invalid("Action result stream has too many entries");
    }

    std::unique_ptr<FlightListing> listing;
    RETURN_NOT_OK(
        client->ListFlights(FlightCallOptions(), {kExpectedCriteria}, &listing));
    RETURN_NOT_OK(listing->Next(&info));
    if (!info) {
      return Status::Invalid("ListFlights stream ends too early!");
    }
    RETURN_NOT_OK(info->Equals(expected_info));
    RETURN_NOT_OK(listing->Next(&info));
    if (!info) {
      return Status::Invalid("ListFlights stream ends too early!");
    }
    RETURN_NOT_OK(info->Equals(expected_info));
    RETURN_NOT_OK(listing->Next(&info));
    if (info) {
      return Status::Invalid("ListFlights stream has too many entries!");
    }

    std::vector<ActionType> actions;
    RETURN_NOT_OK(client->ListActions(&actions));
    if (RpcMethodsServer::MakeActions() != actions) {
      return Status::Invalid("ListActions result does not match");
    }

    return Status::OK();
  }
};

Status GetScenario(const std::string& scenario_name, std::shared_ptr<Scenario>* out) {
  if (scenario_name == "auth:basic_proto") {
    *out = std::make_shared<AuthBasicProtoScenario>();
    return Status::OK();
  } else if (scenario_name == "cancellation") {
    *out = std::make_shared<CancellationScenario>();
    return Status::OK();
  } else if (scenario_name == "error_codes") {
    *out = std::make_shared<ErrorCodesScenario>();
    return Status::OK();
  } else if (scenario_name == "middleware") {
    *out = std::make_shared<MiddlewareScenario>();
    return Status::OK();
  } else if (scenario_name == "rpc_methods") {
    *out = std::make_shared<RpcMethodsScenario>();
    return Status::OK();
  }
  return Status::KeyError("Scenario not found: ", scenario_name);
}

}  // namespace flight
}  // namespace arrow
