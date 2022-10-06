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

from parameterized import parameterized  # type: ignore

from dagcheck import assert_that, given, the_dag, task, succeeds, will_run
from dagcheck import returns, does_not_run, fails, may_run
from dagcheck.test.dagcheck_test_example_dags_utils import (
    basic_dag,
    branching_either_or_dag,
    dag_with_branching_operator,
    dag_with_shorting_operator,
)


class DagcheckSimpleApiTests(unittest.TestCase):
    def test_api_base_case(self):
        thedag = basic_dag()

        assert_that(
            given(thedag)
            .when(task("task_1"), succeeds())
            .then(task("task_3"), may_run())
        )

    def test_api_base_case_failure(self):
        thedag = basic_dag()
        with self.assertRaises(AssertionError):
            assert_that(
                given(thedag)
                .when(task("task_1"), fails())
                .then(task("task_3"), may_run())
            )

    def test_api_negative_base_case(self):
        thedag = basic_dag()

        assert_that(
            given(thedag)
            .when(task("task_1"), fails())
            .then(task("task_2"), does_not_run())
        )

    def test_dag_with_branching_operator(self):
        thedag = dag_with_branching_operator()

        assert_that(
            given(thedag)
            .when(task("branching_boi"), returns("task_A"))
            .then(task("task_A"), will_run())
        )

        with self.assertRaises(AssertionError):
            assert_that(
                given(thedag)
                .when(task("branching_boi"), returns("task_A"))
                .then(task("task_B"), will_run())
            )

    def test_dag_with_shorting_operator(self):
        thedag = dag_with_shorting_operator()

        assert_that(
            given(thedag)
            .when(task("shorting_boi"), returns(False))
            .then(task("task_A"), does_not_run())
        )

        with self.assertRaises(AssertionError):
            assert_that(
                given(thedag)
                .when(task("shorting_boi"), returns(False))
                .then(task("task_A"), may_run())
            )

    def test_api_throws_for_unexpected_run(self):
        thedag = basic_dag()

        with self.assertRaises(AssertionError):
            assert_that(
                given(thedag)
                .when(task("task_1"), succeeds())
                .then(task("task_2"), does_not_run())
            )

    def test_dag_failure_or_success_check(self):
        """Test only that the API works as expected."""
        self.skipTest("DAG selection for validations is not supportd yet.")
        thedag = basic_dag()

        with self.assertRaises(AssertionError):
            assert_that(
                given(thedag)
                .when(task("task_1"), succeeds())
                .then(the_dag(), fails())
            )

    def test_api_throws_for_unexpected_not_run(self):
        """Test only that the API works as expected."""
        thedag = basic_dag()

        with self.assertRaises(AssertionError):
            assert_that(
                given(thedag)
                .when(task("task_1"), fails())
                .then(task("task_2"), will_run())
            )

    @unittest.skip("Only use this test for profiling")
    def test_branching_with_profilint(self):
        cProfile.runctx(
            "self.test_branching_dag_with_trigger_conditions()",
            globals(),
            locals(),
            filename="someout1",
        )

    def test_branching_dag_with_trigger_conditions(self):
        thedag = branching_either_or_dag()

        assert_that(
            given(thedag)
            .when(task("task_1"), fails())
            .then(task("task_2"), will_run())
        )

        assert_that(
            given(thedag)
            .when(task("task_1"), fails())
            .then(task("task_3"), will_run())
        )

        with self.assertRaises(AssertionError):
            assert_that(
                given(thedag)
                .when(task("task_1"), succeeds())
                .then(task("task_3"), will_run())
            )

        with self.assertRaises(AssertionError):
            assert_that(
                given(thedag)
                .when(task("task_1"), succeeds())
                .then(task("task_2"), does_not_run())
            )

    def test_necessary_but_not_sufficient(self):
        """Test only that the API works as expected."""
        thedag = basic_dag()

        # TODO(pabloem): Refine the expected exception
        with self.assertRaises(AssertionError):
            assert_that(
                given(thedag).when(
                    task("task_1"), succeeds()
                )  # task_1 succeeding is necessary but not sufficient to
                # ensure that task_3 will run.
                .then(task("task_3"), will_run())
            )

    def test_api_with_multiple_conditions(self):
        self.skipTest("This method / functionality is not yet implemented.")
        thedag = basic_dag()

        assert_that(
            given(thedag)
            .when(task("task_1"), succeeds())
            .and_(task("task_2"), succeeds())
            .then(task("task_3"), will_run())
        )

    def test_api_with_multiple_conditions_branching(self):
        self.skipTest("This method / functionality is not yet implemented.")
        thedag = branching_either_or_dag()

        assert_that(
            given(thedag)
            .when(task("task_1"), fails())
            .then(task("task_2"), will_run())
            .and_(task("task_3"), will_run())
        )


class DagcheckEnsureCorrectUseTest(unittest.TestCase):
    @parameterized.expand(
        [
            (
                lambda thedag: given(thedag)
                .when(task("task_1"), succeeds())
                .then(task("task_2"), will_run()),
            ),
            (lambda thedag: given(thedag).when(task("task_1"), succeeds()),),
            (given,),
        ]
    )
    def test_checks_must_run(self, test_instance):
        unused_test = test_instance(basic_dag())
        with self.assertLogs(level="ERROR") as log:
            del unused_test
        self.assertEqual(
            log.records[0].message,
            "A dagcheck test has been defined, but was not tested.\n\t"
            "Please wrap your test with assert_that to make sure checks"
            " will run.",
        )


if __name__ == "__main__":
    unittest.main()
