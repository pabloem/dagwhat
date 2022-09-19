

from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def basic_dag() -> models.DAG:
    the_dag = models.DAG(
        'the_basic_dag',
        schedule_interval='@once',
        start_date=DEFAULT_DATE,
    )

    with the_dag:
        op1 = EmptyOperator(task_id='task_1')
        op2 = EmptyOperator(task_id='task_2')
        op3 = EmptyOperator(task_id='task_3')

        op1 >> op2 >> op3 >> PythonOperator(task_id='task_4', python_callable=lambda : print('DONE'))

    return the_dag


def branching_dag() -> models.DAG:
    the_dag = models.DAG(
        'the_basic_branching_dag',
        schedule_interval='@once',
        start_date=DEFAULT_DATE,
    )

    with the_dag:
        op1 = EmptyOperator(task_id='task_1')
        op2 = EmptyOperator(task_id='task_2')
        op3 = EmptyOperator(task_id='task_3')

        op1 >> op2
        op1 >> op3

    return the_dag


def branching_either_or_dag() -> models.DAG:
    the_dag = models.DAG(
        'the_basic_branching_dag',
        schedule_interval='@once',
        start_date=DEFAULT_DATE,
    )

    with the_dag:
        op1 = EmptyOperator(task_id='task_1')
        op2 = EmptyOperator(task_id='task_2',
                            trigger_rule='always')
        op3 = EmptyOperator(task_id='task_3',
                            trigger_rule='one_failed')

        op1 >> op2
        op1 >> op3

    return the_dag


def failing_dag() -> models.DAG:
    the_dag = models.DAG(
        'the_bad_dag',
        schedule_interval='@once',
        start_date=DEFAULT_DATE,
    )

    def complain():
        raise RuntimeError('NO GOOD!')

    with the_dag:
        op1 = EmptyOperator(task_id='task_1')
        op2 = PythonOperator(task_id='task_2', python_callable=complain)
        op3 = EmptyOperator(task_id='task_3')

        op1 >> op2 >> op3

    return the_dag
