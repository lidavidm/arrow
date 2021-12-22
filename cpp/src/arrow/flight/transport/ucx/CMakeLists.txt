# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

add_custom_target(arrow_flight_transport_ucx)
arrow_install_all_headers("arrow/flight/transport/ucx")

# TODO: abstract into Finducx.cmake
find_package(PkgConfig REQUIRED)
pkg_check_modules(UCX REQUIRED ucx)

set(ARROW_FLIGHT_TRANSPORT_UCX_SRCS client_ucx.cc server_ucx.cc ucx.cc)
set(ARROW_FLIGHT_TRANSPORT_UCX_LINK_LIBS)

list(APPEND ARROW_FLIGHT_TRANSPORT_UCX_LINK_LIBS ${UCX_LIBRARIES})

add_arrow_lib(arrow_flight_transport_ucx
              # CMAKE_PACKAGE_NAME
              # ArrowFlightTransportUcx
              # PKG_CONFIG_NAME
              # arrow-flight-transport-ucx
              OUTPUTS
              ARROW_FLIGHT_TRANSPORT_UCX_LIBRARIES
              SOURCES
              ${ARROW_FLIGHT_TRANSPORT_UCX_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:arrow/flight/transport/ucx/pch.h>"
              DEPENDENCIES
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              arrow_shared
              arrow_flight_shared
              STATIC_LINK_LIBS
              arrow_static
              arrow_flight_static)
# TODO: is this right?
target_include_directories(arrow_flight_transport_ucx_shared SYSTEM PRIVATE ${UCX_INCLUDE_DIRS})

# TODO: shared/static
add_arrow_test(flight_transport_ucx_test
  STATIC_LINK_LIBS
  arrow_shared
  arrow_flight_shared
  arrow_flight_testing_shared
  arrow_flight_transport_ucx_shared
  ${ARROW_TEST_LINK_LIBS}
  ${ARROW_FLIGHT_TRANSPORT_UCX_LINK_LIBS}
  LABELS
  "arrow_flight")
