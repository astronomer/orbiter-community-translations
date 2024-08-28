from typing import Callable

from orbiter.objects.callbacks import OrbiterCallback

from orbiter_translations.dag_factory.yaml_base import (
    python_operator_rule,
    task_common_args,
)

expected_task = """def example_task_mapping():
    return [[1], [2], [3]]
request_task = PythonOperator(task_id='request', python_callable=example_task_mapping)"""

expected_task_2 = """def expand_task(x, test_id):
    print(test_id)
    print(x)
    return [x]
process_task = PythonOperator(task_id='process', python_callable=expand_task, partial={'op_kwargs': {'test_id': 'test'}}, expand={'op_args': 'request.output'})"""  # noqa: E501


def test_python_operator(change_test_dir):
    actual = python_operator_rule(
        {
            "__task_id": "request",
            "operator": "airflow.operators.python.PythonOperator",
            "python_callable_name": "example_task_mapping",
            "python_callable_file": "/usr/local/airflow/workflow/dags/expand_tasks.py",
        }
    )
    assert "<function example_task_mapping" in str(actual.python_callable)
    assert isinstance(actual.python_callable, Callable)
    assert actual == expected_task

    actual = python_operator_rule(
        {
            "__task_id": "process",
            "operator": "airflow.operators.python.PythonOperator",
            "python_callable_name": "expand_task",
            "python_callable_file": "/usr/local/airflow/workflow/dags/expand_tasks.py",
            "partial": {"op_kwargs": {"test_id": "test"}},
            "expand": {"op_args": "request.output"},
            "dependencies": ["request"],
        }
    )
    assert "<function expand_task" in str(actual.python_callable)
    assert actual == expected_task_2


def test_task_common_args_callbacks(change_test_dir):
    import sys

    sys.path += ["."]
    actual = task_common_args(
        {
            "__task_id": "request",
            "on_failure_callback_file": "/usr/local/airflow/workflow/dags/expand_tasks.py",
            "on_failure_callback_name": "example_task_mapping",
        }
    )
    assert isinstance(actual.get("on_failure_callback"), OrbiterCallback)
    assert actual.get("task_id", "") == "request"
