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

# pylint: disable=C

import cProfile
import unittest

from airflow.models import DagBag

from dagcheck import assert_that
from dagcheck import given
from dagcheck import task
from dagcheck import succeeds
from dagcheck import will_run


class TestLargerDagSamples(unittest.TestCase):
    def test_airflow_examples_example_complex_dag(self):

        # This DAG is visualized here:
        #   https://airflow.apache.org/docs/apache-airflow/stable/usage-cli.html#exporting-dag-structure-as-an-image
        example_dag = DagBag().get_dag("example_complex")

        # First check: If we create an entry group, we want to make sure
        # it will be cleaned up.
        assert_that(
            given(example_dag)
            .when(task("create_entry_group"), succeeds())
            .then(task("delete_entry_group"), will_run())
        )

    @unittest.skip("Test is too long. Only experimentation / demonstration.")
    def test_profile_test_airflow_examples_example_compex_dag(self):
        cProfile.runctx(
            "self.test_airflow_examples_example_complex_dag()",
            globals(),
            locals(),
            filename="someout_big",
        )


if __name__ == "__main__":
    unittest.main()
