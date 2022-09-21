import unittest
from parameterized import parameterized

from dagwhat.dagwhat_execution import _next_simulation
from dagwhat.base import TaskOutcome


def _make_simulation_from_outcomes(outcomes):
    return [("%s" % i, outcome) for i, outcome in enumerate(outcomes)]


class ExecutionUtilsTest(unittest.TestCase):
    @parameterized.expand(
        [
            (
                [TaskOutcome.SUCCESS, TaskOutcome.SUCCESS],
                [TaskOutcome.FAILURE, TaskOutcome.FAILURE],
            ),
            (
                [TaskOutcome.FAILURE, TaskOutcome.FAILURE],
                [TaskOutcome.FAILURE, TaskOutcome.SUCCESS],
            ),
            (
                [TaskOutcome.FAILURE, TaskOutcome.SUCCESS],
                [TaskOutcome.SUCCESS, TaskOutcome.FAILURE],
            ),
            (
                [TaskOutcome.SUCCESS, TaskOutcome.FAILURE],
                [TaskOutcome.SUCCESS, TaskOutcome.SUCCESS],
            ),
            (
                [TaskOutcome.SUCCESS, TaskOutcome.FAILURE, TaskOutcome.SUCCESS],  # 101
                [TaskOutcome.SUCCESS, TaskOutcome.SUCCESS, TaskOutcome.FAILURE],  # 110
            ),
        ]
    )
    def test_evaluate_next_execution(self, in_ex, out_ex):
        self.assertEqual(
            _next_simulation(_make_simulation_from_outcomes(in_ex)),
            _make_simulation_from_outcomes(out_ex),
        )
