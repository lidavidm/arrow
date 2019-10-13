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

from collections import namedtuple


class Scenario(namedtuple("_Scenario", ["name", "description", "skip"])):
    """
    An integration test scenario for Arrow Flight.

    Does not correspond to a particular IPC JSON file.
    """
    __slots__ = ()

    def __new__(cls, name, description, skip=None):
        skip = skip or set()
        return super().__new__(
            cls, name=name, description=description, skip=skip)
