#!/usr/bin/env bash
#
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

set -ex

arrow_dir=${1}
source_dir=${1}/cpp
build_dir=${2}/cpp

export ARROW_TEST_DATA=${arrow_dir}/testing/data
export PARQUET_TEST_DATA=${source_dir}/submodules/parquet-testing/data
export LD_LIBRARY_PATH=${ARROW_HOME}/${CMAKE_INSTALL_LIBDIR:-lib}:${LD_LIBRARY_PATH}

pushd ${build_dir}
case "$(uname)" in
  Linux)
    n_jobs=$(nproc)
    ;;
  Darwin)
    n_jobs=$(sysctl -n hw.ncpu)
    ;;
  *)
    n_jobs=1
    ;;
esac

echo -e 'bt\nquit' > lldb.batch
lldb --batch -K lldb.batch -o run -f $build_dir/debug/arrow-flight-test

echo -e 'bt\nquit' > lldb.batch
lldb --batch -K lldb.batch -o run -f ctest -- --output-on-failure -j${n_jobs} -VV
popd
