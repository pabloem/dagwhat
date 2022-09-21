from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def basic_dag() -> models.DAG:
    the_dag = models.DAG(
        "the_basic_dag",
        schedule_interval="@once",
        start_date=DEFAULT_DATE,
    )

    with the_dag:
        op1 = EmptyOperator(task_id="task_1")
        op2 = EmptyOperator(task_id="task_2")
        op3 = EmptyOperator(task_id="task_3")

        _ = (
            op1
            >> op2
            >> op3
            >> PythonOperator(task_id="task_4", python_callable=lambda: print("DONE"))
        )

    return the_dag


def branching_dag() -> models.DAG:
    the_dag = models.DAG(
        "the_basic_branching_dag",
        schedule_interval="@once",
        start_date=DEFAULT_DATE,
    )

    with the_dag:
        op1 = EmptyOperator(task_id="task_1")
        op2 = EmptyOperator(task_id="task_2")
        op3 = EmptyOperator(task_id="task_3")

        op1 >> op2
        op1 >> op3

    return the_dag


def dag_with_branching_operator() -> models.DAG:
    the_dag = models.DAG(
        "dag_with_a_branching_operator",
        schedule_interval="@once",
        start_date=DEFAULT_DATE,
    )
    with the_dag:
        op1 = EmptyOperator(task_id="task_1")
        branch_op = BranchPythonOperator(
            task_id="branching_boi", python_callable=lambda: "run_task_B"
        )

        task_a = EmptyOperator(task_id="task_A")
        task_b = EmptyOperator(task_id="task_B")
        op1 >> branch_op
        branch_op >> task_a
        branch_op >> task_b
    return the_dag


def dag_with_shorting_operator() -> models.DAG:
    the_dag = models.DAG(
        "dag_with_a_shorting_operator",
        schedule_interval="@once",
        start_date=DEFAULT_DATE,
    )
    with the_dag:
        op1 = EmptyOperator(task_id="task_1")
        branch_op = ShortCircuitOperator(
            task_id="shorting_boi", python_callable=lambda: True
        )

        task_a = EmptyOperator(task_id="task_A")
        op1 >> branch_op
        branch_op >> task_a
    return the_dag


def branching_either_or_dag() -> models.DAG:
    the_dag = models.DAG(
        "the_basic_branching_dag",
        schedule_interval="@once",
        start_date=DEFAULT_DATE,
    )

    with the_dag:
        op1 = EmptyOperator(task_id="task_1")
        op2 = EmptyOperator(task_id="task_2", trigger_rule="always")
        op3 = EmptyOperator(task_id="task_3", trigger_rule="one_failed")

        op1 >> op2
        op1 >> op3

    return the_dag


def failing_dag() -> models.DAG:
    the_dag = models.DAG(
        "the_bad_dag",
        schedule_interval="@once",
        start_date=DEFAULT_DATE,
    )

    def complain():
        raise RuntimeError("NO GOOD!")

    with the_dag:
        op1 = EmptyOperator(task_id="task_1")
        op2 = PythonOperator(task_id="task_2", python_callable=complain)
        op3 = EmptyOperator(task_id="task_3")

        op1 >> op2 >> op3

    return the_dag
