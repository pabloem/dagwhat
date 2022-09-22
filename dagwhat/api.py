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


"""The public API definition for Dagwhat."""

from airflow.models import DAG
from dagwhat.base import DagSelector
from dagwhat.base import DagTest
from dagwhat.base import TaskTestCheckBuilder
from dagwhat.base import TaskGroupSelector
from dagwhat.base import TaskOutcome


__all__ = [
    "assert_that",
    "given",
    "task",
    "tasks",
    "any_task",
    "succeeds",
    "fails",
    "runs",
    "returns",
    "the_dag",
    "does_not_run",
    "may_run",
    "will_run",
]

from dagwhat.dagwhat_execution import run_check


def assert_that(test_case: TaskTestCheckBuilder):
    """Execution call for DAG checks. All checks must finish with assert_that.

    A DAG check contains the following four things:
    - A DAG
    - A series of assumptions
    - A series of expectations.
    - An `assert_that` function call that executes the check.
    """
    run_check(test_case.build())


def given(dag: DAG) -> "DagTest":
    """Entry point for DAG checks. All DAG checks initialize with their DAG."""
    return DagTest(dag)


def task(task_id: str):
    """Select an individual task via its ID."""
    return TaskGroupSelector(ids=[task_id], group_is=TaskGroupSelector.ALL)


def tasks(*ids: str):
    """Select a group of tasks via their IDs."""
    return TaskGroupSelector(ids=ids, group_is=TaskGroupSelector.ALL)


def any_task(with_id: str = None, with_operator=None):
    """Select any task matching the input ID or input operator.

    This selector defines the family of task groups with a single task matching
    the ID or the operator.
    """
    return TaskGroupSelector(
        ids=[with_id], operators=[with_operator], group_is=TaskGroupSelector.ANY
    )


def all_tasks(with_id=None, with_operator=None):
    """Select all tasks matching the input ID or input operator.

    This selector defines the family of task groups with every task matching
    the ID or the operator.
    """
    return TaskGroupSelector(
        ids=[with_id], operators=[with_operator], group_is=TaskGroupSelector.ALL
    )


def the_dag():
    """A selector representing the whole DAG execution."""
    return DagSelector


##############################################################################
# END TASK SELECTORS
##############################################################################

##############################################################################
# OUTCOME CHECKERS
##############################################################################


def succeeds() -> TaskOutcome:
    """Assumption: The task(s) in question succeed upon execution."""
    return TaskOutcome.SUCCESS


def fails() -> TaskOutcome:
    """Assumption: The task(s) in question fail upon execution."""
    return TaskOutcome.FAILURE


def runs() -> TaskOutcome:
    """Assumption: The task(s) in question will run. May fail or succeed."""
    return TaskOutcome.RUNS


def returns(value) -> TaskOutcome:
    """Assumption: The task(s) have a function that returns `value`."""
    return TaskOutcome(value)


def does_not_run() -> TaskOutcome:
    """Expectation: The task(s) in question will not run given assumptions."""
    return TaskOutcome.WILL_NOT_RUN


def may_run() -> TaskOutcome:
    """Expectation: The task(s) may run despite the given assumptions."""
    return TaskOutcome.MAY_RUN


def may_not_run() -> TaskOutcome:
    """Expectation: The task(s) may not run despite the given assumptions."""
    return TaskOutcome.MAY_NOT_RUN


def will_run() -> TaskOutcome:
    """Expectation: The task(s) in question will run in assumed conditions."""
    return TaskOutcome.WILL_RUN


##############################################################################
# END OUTCOME CHECKERS
##############################################################################
