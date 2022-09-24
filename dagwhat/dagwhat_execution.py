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

"""Execution classes and methods for DAG checks in dagwhat."""

import typing

from airflow import DAG, AirflowException
from airflow.executors.debug_executor import DebugExecutor
from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State
from airflow.utils.context import Context
from airflow.operators.python import PythonOperator

from dagwhat import base
from dagwhat.base import TaskOutcome, TaskOutcomes, FinalTaskTestCheck


class HypothesisExecutor(DebugExecutor):
    """An executor that can execute assumptions and verify expectations."""

    def __init__(
        self,
        assumed_tasks_and_outcomes: typing.Mapping[str, "base.TaskOutcome"],
        expected_tasks_and_outcomes: typing.Mapping[str, "base.TaskOutcome"],
        simulated_tasks_and_outcomes: typing.Mapping[str, "base.TaskOutcome"],
    ):
        super().__init__()
        self.assumed_tasks_and_outcomes = assumed_tasks_and_outcomes
        self.expected_tasks_and_outcomes = expected_tasks_and_outcomes
        self.matching_expectations = set(
            self.expected_tasks_and_outcomes.keys()
        )
        self.actual_task_results: typing.MutableMapping = {
            t: base.TaskOutcomes.NOT_RUN for t in expected_tasks_and_outcomes
        }
        self.simulated_tasks_and_outcomes = simulated_tasks_and_outcomes
        self.done = False

    def _patch_and_execute_operator(
        self, task_instance: TaskInstance, task_outcome: base.TaskOutcome
    ):
        # PythonOperators need to be patched because we often need to run
        # the execute_callable function - particularly for Branching operators.
        if isinstance(task_instance.task.roots[0], PythonOperator):
            original_lambda = getattr(
                task_instance.task.roots[0], "execute_callable"
            )

            def new_lambda(*unused_args, **unused_kwargs):
                return task_outcome.return_value

            setattr(task_instance.task.roots[0], "execute_callable", new_lambda)
            # TODO(pabloem): Fix key population for Context (if necessary).
            assert isinstance(task_instance.task.dag, DAG)
            assert isinstance(task_instance.task, BaseOperator)
            task_instance.task.roots[0].execute(
                Context(
                    {  # type: ignore
                        "dag": task_instance.task.dag,
                        "ti": task_instance,
                        "task": task_instance.task,
                        "dag_run": task_instance.dag_run,
                    }
                )
            )
            setattr(
                task_instance.task.roots[0], "execute_callable", original_lambda
            )

    def _run_task(self, ti: TaskInstance) -> bool:
        if ti.task_id in self.assumed_tasks_and_outcomes:
            # Assert that this assumed outcome is among correct assumable
            # outcomes, or is a returns('value') outcome.
            assert (
                self.assumed_tasks_and_outcomes[ti.task_id].outcome
                in base.TaskOutcomes.ASSUMABLE_OUTCOMES
            )
            self.actual_task_results[
                ti.task_id
            ] = self.assumed_tasks_and_outcomes[ti.task_id]
            if self.assumed_tasks_and_outcomes[ti.task_id].outcome in (
                base.TaskOutcomes.SUCCESS.outcome,
                base.TaskOutcomes.RETURNS.outcome,
            ):
                self._patch_and_execute_operator(
                    ti, self.assumed_tasks_and_outcomes[ti.task_id]
                )
                self.change_state(ti.key, State.SUCCESS)
                ti.set_state(State.SUCCESS)
            elif (
                self.assumed_tasks_and_outcomes[ti.task_id]
                == base.TaskOutcomes.FAILURE
            ):
                self.change_state(ti.key, State.FAILED)
                ti.set_state(State.FAILED)
            else:
                raise ValueError(
                    "The outcome %r is not acceptable (or not supported) "
                    "for task %r"
                    % (self.assumed_tasks_and_outcomes[ti.task_id], ti.task_id)
                )
        elif ti.task_id in self.simulated_tasks_and_outcomes:
            # If we don't have a pre-determined outcome for this task, we must
            # simulate a success and a failure to obtain the actual result.
            if (
                self.simulated_tasks_and_outcomes[ti.task_id]
                == base.TaskOutcomes.SUCCESS
            ):
                self.change_state(ti.key, State.SUCCESS)
                ti.set_state(State.SUCCESS)
            elif (
                self.simulated_tasks_and_outcomes[ti.task_id]
                == base.TaskOutcomes.FAILURE
            ):
                self.change_state(ti.key, State.FAILED)
                ti.set_state(State.FAILED)

            self._check_if_task_had_expectation(ti)
        else:
            # In this case, this task's result does not matter because
            # it is not upstream of any of the tasks we care about. We
            # just call it succeeded and move on.
            self.change_state(ti.key, State.SUCCESS)
            ti.set_state(State.SUCCESS)
            self._check_if_task_had_expectation(ti)

        return ti.state == State.SUCCESS

    def _check_if_task_had_expectation(self, task_instance: TaskInstance):
        if task_instance.task_id in self.expected_tasks_and_outcomes:
            assert (
                self.expected_tasks_and_outcomes[task_instance.task_id]
                in base.TaskOutcomes.EXPECTABLE_OUTCOMES
            )
            # We cannot know whether the task will fail or succeed. We can
            # only know that it has been reached, and under the current
            # conditions it will run.
            self.actual_task_results[task_instance.task_id] = TaskOutcomes.RUNS
            if (
                self.matching_expectations.intersection(
                    self.actual_task_results.keys()
                )
                == self.matching_expectations
            ):
                self.done = True
                self.end()


def _next_simulation(
    previous_simulation: typing.List[typing.Tuple[str, TaskOutcome]]
):
    result = []
    flips_done = False
    for i in reversed(range(len(previous_simulation))):
        if flips_done:
            result.append(previous_simulation[i])
        elif previous_simulation[i][1] == TaskOutcomes.FAILURE:
            result.append((previous_simulation[i][0], TaskOutcomes.SUCCESS))
            flips_done = True
        else:
            result.append((previous_simulation[i][0], TaskOutcomes.FAILURE))

    return list(reversed(result))


def _get_tasks_to_simulate(
    dag: DAG, assummed_tasks_and_outs, expected_tasks_and_outs
):
    tasks_to_simulate: typing.Set[str] = set()
    check_upstreams: typing.List[str] = list(
        expected_tasks_and_outs.keys()
    ) + list(assummed_tasks_and_outs.keys())
    while check_upstreams:
        current_task_id = check_upstreams.pop()
        current_task = dag.task_dict[current_task_id]
        tasks_to_simulate = tasks_to_simulate.union(
            tasks_to_simulate, current_task.upstream_task_ids
        )
        check_upstreams.extend(current_task.upstream_task_ids)

    return list(tasks_to_simulate)


def _evaluate_assumption_and_expectation(
    assumed_tasks_and_outs, expected_tasks_and_outs, dag: DAG
):
    tasks_to_simulate = _get_tasks_to_simulate(
        dag, assumed_tasks_and_outs, expected_tasks_and_outs
    )
    simulation_results = []
    simulations_to_run = 2 ** len(tasks_to_simulate)
    print("Running a total of %r simulations." % simulations_to_run)

    for i in range(simulations_to_run):
        if (i + 1) % 1000 == 0:
            print(f"Simulation {i+1}")
        current_simulation: typing.List[typing.Tuple[str, TaskOutcome]] = (
            [(task_id, TaskOutcomes.FAILURE) for task_id in tasks_to_simulate]
            if i == 0
            else _next_simulation(current_simulation)
        )
        current_simulation_dict = dict(current_simulation)

        hypothesis_executor = HypothesisExecutor(
            assumed_tasks_and_outs,
            expected_tasks_and_outs,
            current_simulation_dict,
        )
        dag.clear()
        try:
            dag.run(executor=hypothesis_executor, run_at_least_once=True)
            dag_fails = False
        except AirflowException:
            dag_fails = True

        if (
            set(hypothesis_executor.actual_task_results.keys()).intersection(
                assumed_tasks_and_outs.keys()
            )
            != assumed_tasks_and_outs.keys()
        ):
            # Will not consider this run because it does not meet assumptions.
            continue

        print(
            "Dag Fails: %s"
            "\n\tSimulated t&o: %r"
            "\n\tAssumed t&o: %r"
            "\n\tExpected t&o: %r"
            "\n\tActual t&o: %r"
            % (
                dag_fails,
                hypothesis_executor.simulated_tasks_and_outcomes,
                hypothesis_executor.assumed_tasks_and_outcomes,
                hypothesis_executor.expected_tasks_and_outcomes,
                hypothesis_executor.actual_task_results,
            )
        )

        simulation_results.append(hypothesis_executor.actual_task_results)

        if any(
            (
                result == TaskOutcomes.NOT_RUN
                and expected_tasks_and_outs[task_id] == TaskOutcomes.WILL_RUN
            )
            or (
                result == TaskOutcomes.RUNS
                and expected_tasks_and_outs[task_id]
                == TaskOutcomes.WILL_NOT_RUN
            )
            for task_id, result
            in hypothesis_executor.actual_task_results.items()
        ):
            return True, False, simulation_results
        if any(
            (
                result == TaskOutcomes.RUNS
                and expected_tasks_and_outs[task_id] == TaskOutcomes.MAY_RUN
            )
            or (
                result == TaskOutcomes.NOT_RUN
                and expected_tasks_and_outs[task_id] == TaskOutcomes.MAY_NOT_RUN
            )
            for task_id, result
            in hypothesis_executor.actual_task_results.items()
        ):
            return False, True, simulation_results

    return False, False, simulation_results


def run_check(check: "FinalTaskTestCheck"):
    """Entry point for execution of a DAG check.

    This method is called by the assert_what function that wraps a fully
    defined DAG check."""
    all_resulting_outcomes = []

    # TODO(pabloem): Support multiple test conditions.
    #  The code below assumes only single test conditions.
    (
        assumed_task_selector,
        assumed_outcome,
    ) = check.task_test_condition.condition_chain[0]

    # TODO(pabloem): Support multiple test conditions.
    #  The code below assumes only single test conditions.
    expected_task_selector, expected_outcome = check.validation_chain[0]

    for matching_taskgroup in assumed_task_selector.generate_task_groups(
        check.dag
    ):
        assumed_tasks_and_ops = dict(matching_taskgroup)
        assumed_tasks_and_outs = {
            t: assumed_outcome for t in assumed_tasks_and_ops
        }

        for (
            expected_matching_taskgroup
        ) in expected_task_selector.generate_task_groups(check.dag):
            expected_tasks_and_ops = dict(expected_matching_taskgroup)
            expected_tasks_and_outs = {
                t: expected_outcome for t in expected_tasks_and_ops
            }

            # The instant_failure variable becomes true if we find a simulation
            # scenario that would qualify the whole test case for failure.
            # For example when a WILL_NOT_RUN task has run in a simulation,
            # this means that the whole DAG invariant has been broken and the
            # test can be considered a failure.

            # The instant_success variable becomes true if we find a simulation
            # scenario that would qualify the whole test for success right away.
            # For example when a MAY_RUN task has run in a simulation, it means
            # there is at least one scenario where the task runs, and therefore
            # the whole test qualifies for success.
            (
                instant_failure,
                instant_success,
                actual_tasks_and_outs,
            ) = _evaluate_assumption_and_expectation(
                assumed_tasks_and_outs=assumed_tasks_and_outs,
                expected_tasks_and_outs=expected_tasks_and_outs,
                dag=check.dag,
            )
            all_resulting_outcomes.append(actual_tasks_and_outs)

            if instant_failure:
                print("RAN %s iterations" % len(all_resulting_outcomes))
                raise AssertionError(
                    "Failures - \n\tExpected: %r \n\tActuals: %r"
                    % (expected_tasks_and_outs, actual_tasks_and_outs)
                )

            if instant_success:
                print("RAN %s iterations" % len(all_resulting_outcomes))
                return

        # If we are not successful, then we return an assertion error
        if not all(
            _task_expectation_matches_outcomes(t, e, all_resulting_outcomes[0])
            for t, e in expected_tasks_and_outs.items()
        ):
            raise AssertionError(
                "Failures - \n\tExpected: %r \n\tActuals: %r"
                % (expected_tasks_and_outs, all_resulting_outcomes)
            )


def _task_expectation_matches_outcomes(task, expectation, outcomes):
    operator = (
        all
        if expectation in (TaskOutcomes.WILL_RUN, TaskOutcomes.WILL_NOT_RUN)
        else any
    )
    value = (
        TaskOutcomes.RUNS
        if expectation in (TaskOutcomes.WILL_RUN, TaskOutcomes.MAY_RUN)
        else TaskOutcomes.NOT_RUN
    )
    return operator(outcome[task] == value for outcome in outcomes)
