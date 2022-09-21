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
import typing

from airflow import DAG, AirflowException
from airflow.executors.debug_executor import DebugExecutor
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State
from airflow.operators.python import PythonOperator

import dagwhat.dagwhat
from dagwhat.dagwhat import base
from dagwhat.dagwhat.base import TaskOutcome, FinalTaskTestCheck


class HypothesisExecutor(DebugExecutor):
    def __init__(self,
                 assumed_tasks_and_outcomes: typing.Mapping[str, 'base.TaskOutcome'],
                 expected_tasks_and_outcomes: typing.Mapping[str, 'base.TaskOutcome'],
                 simulated_tasks_and_outcomes: typing.Mapping[str, 'base.TaskOutcome']):
        super(HypothesisExecutor, self).__init__()
        self.assumed_tasks_and_outcomes = assumed_tasks_and_outcomes
        self.expected_tasks_and_outcomes = expected_tasks_and_outcomes
        self.actual_task_results: typing.MutableMapping = {t: base.TaskOutcome.NOT_RUN for t in expected_tasks_and_outcomes}
        self.simulated_tasks_and_outcomes = simulated_tasks_and_outcomes

    def _patch_and_execute_operator(self, ti: TaskInstance, to: base.TaskOutcome):
        # PythonOperators need to be patched because we often need to run
        # the execute_callable function - particularly for Branching operators.
        if isinstance(ti.task.roots[0], PythonOperator):
            original_lambda = getattr(ti.task.roots[0], 'execute_callable')
            def new_lambda(*args, **kwargs):
                return to.return_value
            setattr(ti.task.roots[0], 'execute_callable', new_lambda)
            ti.task.roots[0].execute({'dag': ti.task.dag, 'ti': ti, 'task': ti.task, 'dag_run': ti.dag_run})
            setattr(ti.task.roots[0], 'execute_callable', original_lambda)

    def _run_task(self, ti: TaskInstance) -> bool:
        if ti.task_id in self.assumed_tasks_and_outcomes:
            # Assert that this assumed outcome is among correct assumable outcomes, or is a returns('value')
            # outcome.
            assert (self.assumed_tasks_and_outcomes[ti.task_id] in base.TaskOutcome.ASSUMABLE_OUTCOMES
                    or isinstance(self.assumed_tasks_and_outcomes[ti.task_id], base.TaskOutcome))
            self.actual_task_results[ti.task_id] = self.assumed_tasks_and_outcomes[ti.task_id]
            if (self.assumed_tasks_and_outcomes[ti.task_id] == base.TaskOutcome.SUCCESS
                or isinstance(self.assumed_tasks_and_outcomes[ti.task_id], base.TaskOutcome)):
                # TODO(pabloem): Patch Python Operators to execute a simple lambda
                self._patch_and_execute_operator(
                    ti, self.assumed_tasks_and_outcomes[ti.task_id])
                self.change_state(ti.key, State.SUCCESS)
                ti.set_state(State.SUCCESS)
            elif self.assumed_tasks_and_outcomes[ti.task_id] == base.TaskOutcome.FAILURE:
                self.change_state(ti.key, State.FAILED)
                ti.set_state(State.FAILED)
            else:
                raise ValueError(
                    'The outcome %r is not acceptable (or not supported) for task %r' %
                    (self.assumed_tasks_and_outcomes[ti.task_id], ti.task_id))
        elif ti.task_id in self.expected_tasks_and_outcomes:
            assert self.expected_tasks_and_outcomes[ti.task_id] in base.TaskOutcome.EXPECTABLE_OUTCOMES
            # We cannot know whether the task will fail or succeed. We can only know that it has been reached,
            # and under the current assumed conditions, the task will run.
            ti.set_state(State.SUCCESS)
            self.actual_task_results[ti.task_id] = TaskOutcome.RUNS

        if ti.task_id in self.simulated_tasks_and_outcomes:
            # If we don't have a pre-determined outcome for this task, we must
            # simulate a success and a failure to obtain the actual result.
            if self.simulated_tasks_and_outcomes[ti.task_id] == base.TaskOutcome.SUCCESS:
                self.change_state(ti.key, State.SUCCESS)
                ti.set_state(State.SUCCESS)
            elif self.simulated_tasks_and_outcomes[ti.task_id] == base.TaskOutcome.FAILURE:
                self.change_state(ti.key, State.FAILED)
                ti.set_state(State.FAILED)

        return ti.state == State.SUCCESS


def _next_simulation(previous_simulation: typing.List[typing.Tuple[str, TaskOutcome]]):
    result = []
    flips_done = False
    for i in reversed(range(len(previous_simulation))):
        if flips_done:
            result.append(previous_simulation[i])
        elif previous_simulation[i][1] == TaskOutcome.SUCCESS:
            result.append((previous_simulation[i][0], TaskOutcome.FAILURE))
            flips_done = True
        else:
            result.append((previous_simulation[i][0], TaskOutcome.SUCCESS))

    return list(reversed(result))


def _evaluate_assumption_and_expectation(assumed_tasks_and_outs,
                                         expected_tasks_and_outs,
                                         dag: DAG):
    tasks_to_simulate = dag.task_dict.keys() - assumed_tasks_and_outs.keys()
    simulation_results = []
    simulations_to_run = 2 ** len(tasks_to_simulate)
    print('Running a total of %r simulations.' % simulations_to_run)

    current_simulation: typing.Optional[typing.List[typing.Tuple[str, TaskOutcome]]] = None
    for i in range(simulations_to_run):
        current_simulation = (
            [(task_id, TaskOutcome.FAILURE) for task_id in tasks_to_simulate]
            if i == 0
            else _next_simulation(current_simulation))

        hypothesis_executor = dagwhat.dagwhat.dagwhat_execution.HypothesisExecutor(
            assumed_tasks_and_outs,
            expected_tasks_and_outs,
            dict(current_simulation)
        )
        dag.clear()
        try:
            dag.run(executor=hypothesis_executor)
            dag_fails = False
        except AirflowException:
            dag_fails = True

        if (set(hypothesis_executor.actual_task_results.keys()).intersection(assumed_tasks_and_outs.keys())
            != assumed_tasks_and_outs.keys()):
            # We must not count this run as part of the rest because it cannot meet the assumed conditions.
            continue

        print('Dag Fails: %s'
              '\n\tSimulated t&o: %r'
              '\n\tAssumed t&o: %r'
              '\n\tExpected t&o: %r'
              '\n\tActual t&o: %r'
              % (dag_fails,
                 hypothesis_executor.simulated_tasks_and_outcomes,
                 hypothesis_executor.assumed_tasks_and_outcomes,
                 hypothesis_executor.expected_tasks_and_outcomes,
                 hypothesis_executor.actual_task_results))

        # TODO(pabloem): We should create a new ENUM for test outcomes rather than
        #    reutilizing the TaskOutcome ENUM.
        simulation_results.append(hypothesis_executor.actual_task_results)

        if any(
            (result == TaskOutcome.NOT_RUN and expected_tasks_and_outs[task_id] == TaskOutcome.WILL_RUN)
            or (result == TaskOutcome.RUNS and expected_tasks_and_outs[task_id] == TaskOutcome.WILL_NOT_RUN)
            for task_id, result in hypothesis_executor.actual_task_results.items()):
            return True, False, simulation_results
        elif any(
                (result == TaskOutcome.RUNS and expected_tasks_and_outs[task_id] == TaskOutcome.MAY_RUN)
                or (result == TaskOutcome.NOT_RUN and expected_tasks_and_outs[task_id] == TaskOutcome.MAY_NOT_RUN)
                for task_id, result in hypothesis_executor.actual_task_results.items()):
                return False, True, simulation_results

    return False, False, simulation_results


def run_check(check: 'FinalTaskTestCheck'):
    dag = check.dag
    condition_tester = check.task_test_condition
    validations_to_apply = check.validation_chain

    all_resulting_outcomes = []

    # TODO(pabloem): Support multiple test conditions.
    #  The code below assumes only single test conditions.
    assumed_task_selector, assumed_outcome = condition_tester.condition_chain[0]

    # TODO(pabloem): Support multiple test conditions.
    #  The code below assumes only single test conditions.
    expected_task_selector, expected_outcome = validations_to_apply[0]

    for matching_taskgroup in assumed_task_selector.generate_task_groups(dag):
        assumed_tasks_and_ops = dict(matching_taskgroup)
        assumed_tasks_and_outs = {t: assumed_outcome for t in assumed_tasks_and_ops}

        for expected_matching_taskgroup in expected_task_selector.generate_task_groups(dag):
            expected_tasks_and_ops = dict(expected_matching_taskgroup)
            expected_tasks_and_outs = {t: expected_outcome for t in expected_tasks_and_ops}

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
            instant_failure, instant_success, actual_tasks_and_outs = _evaluate_assumption_and_expectation(
                assumed_tasks_and_outs=assumed_tasks_and_outs,
                expected_tasks_and_outs=expected_tasks_and_outs,
                dag=dag)
            all_resulting_outcomes.append(actual_tasks_and_outs)

            if instant_failure:
                print('RAN %s iterations' % len(all_resulting_outcomes))
                raise AssertionError('Failures - \n\tExpected: %r \n\tActuals: %r'
                                     % (expected_tasks_and_outs, actual_tasks_and_outs))

            if instant_success:
                print('RAN %s iterations' % len(all_resulting_outcomes))
                return

        success = all(_task_expectation_matches_outcomes(t, e, all_resulting_outcomes[0])
            for t, e in expected_tasks_and_outs.items())
        if not success:
            raise AssertionError('Failures - \n\tExpected: %r \n\tActuals: %r'
                                 % (expected_tasks_and_outs, all_resulting_outcomes))


def _task_expectation_matches_outcomes(task, expectation, outcomes):
    operator = all if expectation in (TaskOutcome.WILL_RUN, TaskOutcome.WILL_NOT_RUN) else any
    value = TaskOutcome.RUNS if expectation in (TaskOutcome.WILL_RUN, TaskOutcome.MAY_RUN) else TaskOutcome.NOT_RUN
    return operator(outcome[task] == value for outcome in outcomes)

