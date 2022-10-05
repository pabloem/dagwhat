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

"""
This package contains a framework to define property tests for Apache Airflow
DAGs.

A property test works by defining a series of invariants and verifying
program behavior against them.

The library contains several building blocks that can be used to define these
invariants. An invariant consists of:

    - A series of assumptions about DAG execution. These assumptions represent
        possible outcomes during the execution of a DAG.
    - A series of expectations about DAG execution. These expectations are
        outcomes that we want to *guarantee* given (and despite) the assumed
        conditions.

## Task selectors

Task selectors are logical statements that allow DAG checks to define families
of task sets or DAGs that will be used to assume outcomes, or to assert
expected outcomes.

"""
import airflow

from dagcheck.api import *  # noqa

airflow.configuration.load_test_config()
