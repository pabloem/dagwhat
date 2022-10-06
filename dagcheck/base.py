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

# pylint: disable=R0903

"""The base classes for the implementation of the Dagcheck module.

The classes, functions and constants in this file are the basic
blocks used to build an internal representation for a Dagcheck
property-based test.
"""

from enum import Enum
import logging
import typing

from airflow.models import DAG
from airflow.models import Operator

TaskIdType = str

_LOGGER = logging.getLogger(__name__)


class TaskOutcome:
    """A class and enumeration of task outcomes.

    A task outcome can be: Success, failure, run, not run.

    This enumeration defines also 'checked outcomes', which are will_run,
    may_run, will_not_run, may_not_run, etc.
    """

    def __init__(self, outcome, return_value=None):
        self.outcome = outcome
        self.return_value = return_value

    def __eq__(self, other):
        return isinstance(other, TaskOutcome) and (
            self.outcome,
            self.return_value,
        ) == (
            other.outcome,
            other.return_value,
        )

    def __hash__(self):
        return hash((self.outcome, self.return_value))

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        if self.return_value:
            return f"TaskOutcome({self.outcome}->{self.return_value})"
        return f"TaskOutcome({self.outcome})"

    @classmethod
    def assert_expectable(cls, outcome: "TaskOutcome"):
        """Checks that an outcome value is a valid expectable outcome."""
        if (
            isinstance(outcome, str)
            and outcome not in TaskOutcomes.EXPECTABLE_OUTCOMES
        ):
            raise ValueError(
                f"Task or dag outcome {outcome} cannot be asserted on."
                f" Use one of {TaskOutcomes.EXPECTABLE_OUTCOMES} instead."
            )

    @classmethod
    def assert_assumable(cls, outcome: "TaskOutcome"):
        """Checks that an outcome value is a valid assumable outcome."""
        if (
            isinstance(outcome, str)
            and outcome not in TaskOutcomes.ASSUMABLE_OUTCOMES
        ):
            raise ValueError(
                f"Task or dag outcome {outcome} cannot be used as assumption."
                f" Use one of {TaskOutcomes.ASSUMABLE_OUTCOMES} instead."
            )


class TaskOutcomes:
    """Holder for all possible Task Outcomes."""

    # TODO(pabloem): Support tasks failing and being retried, etc.
    SUCCESS = TaskOutcome("SUCCESS")
    FAILURE = TaskOutcome("FAILURE")
    RUNS = TaskOutcome("RUNS")
    NOT_RUN = TaskOutcome("NOT_RUN")  # TODO(pabloem): Document
    MAY_RUN = TaskOutcome("MAY_RUN")  # TODO(pabloem): Document
    MAY_NOT_RUN = TaskOutcome("MAY_NOT_RUN")  # TODO(pabloem): Document
    WILL_RUN = TaskOutcome("WILL_RUN")
    WILL_NOT_RUN = TaskOutcome("WILL_NOT_RUN")
    RETURNS = TaskOutcome("RETURNS", object())

    ASSUMABLE_OUTCOMES = {
        SUCCESS.outcome,
        FAILURE.outcome,
        NOT_RUN.outcome,
        RUNS.outcome,
        RETURNS.outcome,
    }
    EXPECTABLE_OUTCOMES = {MAY_RUN, MAY_NOT_RUN, WILL_RUN, WILL_NOT_RUN}


class TaskSelectorEnum(Enum):
    """Enumerator for ways in which to select tasks for validations."""

    ANY = 1
    ALL = 2


class TaskGroupSelector:
    """An internal class used to build sets of tasks that meet a condition.

    This class has no backards-compatibility guarantees.
    """

    def __init__(
        self,
        ids: typing.Optional[typing.Collection[str]] = None,
        operators: typing.Optional[
            typing.Collection[typing.Type[Operator]]
        ] = None,
        group_is: TaskSelectorEnum = None,
    ):
        self.ids = ids
        self.operators = operators
        self.group_is = group_is

    def generate_task_groups(
        self, dag: DAG
    ) -> typing.Iterable[typing.List[typing.Tuple[TaskIdType, Operator]]]:
        """A generator of groups of tasks matching input selection criteria."""

        def id_matches(real_id, matching_id):
            # TODO(pabloem): support wildcard matching
            return real_id == matching_id

        matching_tasks = []
        for task_id in dag.task_dict:
            operator = dag.task_dict[task_id]
            if self.operators and any(
                isinstance(operator, allowed_operator)
                for allowed_operator in self.operators
            ):
                matching_tasks.append((task_id, dag.task_dict[task_id]))
                continue
            if self.ids and any(
                id_matches(task_id, matching_id) for matching_id in self.ids
            ):
                matching_tasks.append((task_id, dag.task_dict[task_id]))
                continue

        # Verify that for exact ID matching, we have one task per id
        if self.ids and len(self.ids) != len(matching_tasks):
            found_ids = {id for id, _ in matching_tasks}
            raise ValueError(
                "Unable to match all expected IDs to tasks in the DAG. "
                "Unmatched IDs: %r" % set(self.ids).difference(found_ids)
            )

        if self.group_is == TaskSelectorEnum.ANY:
            return [[id_op] for id_op in matching_tasks]
        assert self.group_is == TaskSelectorEnum.ALL
        return [matching_tasks]


class DagSelector:
    """An internal class used to represent the selection of the whole dag run.

    This class has no backards-compatibility guarantees.
    """


class FinalTaskTestCheck:
    """An internal class that represents a fully constructed dagcheck check.

    It contains a DAG, a validation chain, and a series of assumptions in the
    TaskTestBuilder.

    This class has no backards-compatibility guarantees.
    """

    def __init__(
        self,
        dag: DAG,
        task_test_condition: "TaskTestBuilder",
        validation_chain: typing.List[
            typing.Tuple[TaskGroupSelector, TaskOutcome]
        ],
    ):
        self.dag = dag
        self.task_test_condition = task_test_condition
        self.validation_chain = validation_chain


class TaskTestCheckBuilder:
    """An internal class that represents a dagcheck check that can be extended.

    It contains a validation chain that can be used to check against a DAG or
    series of tasks.

    This class has no backards-compatibility guarantees.
    """

    def __init__(self, task_test_condition: "TaskTestBuilder"):
        self.task_test_condition = task_test_condition
        # TODO(pabloem): Support task-or-dag selector
        self.validation_chain: typing.List[
            typing.Tuple[TaskGroupSelector, TaskOutcome]
        ] = []
        self.checked = False

    def and_(self, task_selector, task_outcome):
        """Add the next assumed condition for this DAG-based test.

        This method receives a task selector, and an assumed outcome.
        """
        # TODO(pabloem): Flesh out / improve this method. It's one of the most
        #  important methods of the library.
        raise NotImplementedError("Not yet implemented sorry")

    def add_check(
        self, task_or_dag_selector, task_or_dag_outcome
    ) -> "TaskTestCheckBuilder":
        """Add a new check to the existing chain of checks."""
        self.validation_chain.append(
            (task_or_dag_selector, task_or_dag_outcome)
        )
        return self

    def __del__(self):
        if not self.checked:
            # TODO(pabloem): Figure out how to throw an error when
            #   tests are used incorrectly.
            # raise AssertionError(
            _LOGGER.error(
                "A dagcheck test has been defined, but was not tested.\n\t"
                "Please wrap your test with assert_that to make sure checks "
                "will run."
            )

    def mark_checked(self):
        """Mark this check as visited.

        If a check is created but never visited, then the test case has not
        been validated."""
        self.checked = True
        self.task_test_condition.mark_checked()

    def build(self):
        """Take validation chain and task selectors, and build final check."""
        self.mark_checked()
        return FinalTaskTestCheck(
            self.task_test_condition.dag_test.dag,
            self.task_test_condition,
            self.validation_chain,
        )


class TaskTestBuilder:
    """An internal class that represents a dagcheck check that can be extended.

    It contains a DAG, a validation chain, and a series of assumptions in the
    TaskTestBuilder.

    This class has no backards-compatibility guarantees.
    """

    def __init__(
        self,
        dag_test: "DagTest",
        task_selector: TaskGroupSelector,
        outcome: TaskOutcome,
    ):  # TODO(pabloem): How do we make an ENUM? lol
        self.dag_test = dag_test
        self.condition_chain = [(task_selector, outcome)]

        # Dagcheck should support multi-and conditions or multi-or conditions.
        # Because mixed AND / OR evaluations are not associative, supporting
        # a mix of these conditions would create ambiguity in the API.
        # TODO(pabloem): Add support to multi-OR conditions, not just multi-AND
        #  conditions. Idea: We can take advantage of a typesystem by defining
        #  'AdjunctiveTaskTestBuilder' and 'DisjunctiveTaskTestBuilder' as
        #  subclasses of 'TaskTestBuilder'. This would 'force' users to use
        #  appropriate methods from the typesystem and checked statically
        #  instead of at runtime (though runtime check should follow the
        #  static check nearly immediately).
        self.condition_chain_method = "AND"
        self.checked = False

    def mark_checked(self):
        """Mark this check as visited.

        If a check is created but never visited, then the test case has not
        been validated."""
        self.checked = True
        self.dag_test.mark_checked()

    def __del__(self):
        if not self.checked:
            # TODO(pabloem): Figure out how to throw an error when
            #   tests are used incorrectly.
            # raise AssertionError(
            _LOGGER.error(
                "A dagcheck test has been defined, but was not tested.\n\t"
                "Please wrap your test with assert_that to make sure checks "
                "will run."
            )

    def and_(self, task_selector, outcome) -> "TaskTestBuilder":
        """Add the next expected outcome for this DAG check.

        This method takes a task selector, and an expected outcome.
        """
        # TODO(pabloem): Flesh out / improve this method. It's one of the most
        #  important methods of the library.
        raise NotImplementedError(
            "Additive test conditions are not yet supported."
        )

    def then(
        self,
        task_or_dag_selector,
        task_or_dag_outcome: TaskOutcome,
    ) -> TaskTestCheckBuilder:
        """Add the first expected outcome for this DAG check.

        This method takes a task selector, and an expected outcome.
        """
        # TODO(pabloem): Flesh out / improve this method. It's one of the most
        #  important methods of the library.
        TaskOutcome.assert_expectable(task_or_dag_outcome)
        check_builder = TaskTestCheckBuilder(self)
        check_builder.add_check(task_or_dag_selector, task_or_dag_outcome)
        return check_builder


class DagTest:
    """The first class used to build a dagcheck check.

    This class contains a reference to the Airflow DAG that we're testing,
    and can received assumptions via the `when` method.

    This class has no backwards compatibility guarantees.
    """

    def __init__(self, dag: DAG):
        self.dag = dag
        self.checked = False

    def mark_checked(self):
        """Mark this check as visited.

        If a check is created but never visited, then the test case has not
        been validated."""
        self.checked = True

    def __del__(self):
        if not self.checked:
            # TODO(pabloem): Figure out how to throw an error when
            #   tests are used incorrectly.
            _LOGGER.error(
                "A dagcheck test has been defined, but was not tested.\n\t"
                "Please wrap your test with assert_that to make sure checks "
                "will run."
            )

    def when(self, task_selector, outcome) -> TaskTestBuilder:
        """Add the first assumed condition for this DAG-based test.

        This method receives a task selector, and an assumed outcome.
        """
        # TODO(pabloem): Flesh out / improve this method. It's one of the most
        #  important methods of the library.
        TaskOutcome.assert_assumable(outcome)
        return TaskTestBuilder(self, task_selector, outcome)
