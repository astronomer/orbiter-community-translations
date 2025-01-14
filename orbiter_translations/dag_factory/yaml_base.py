"""Full DAG Factory YAML Translation Ruleset.

Convert DAG Factory YAML into full Airflow DAGs"""
from __future__ import annotations

import inspect
import os
import re
from copy import deepcopy
from pathlib import Path
from typing import MutableMapping, Literal

import inflection
from loguru import logger
from orbiter import import_from_qualname
from orbiter.file_types import FileTypeYAML
from orbiter.objects import OrbiterInclude
from orbiter.objects.callbacks import OrbiterCallback
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.python import OrbiterPythonOperator
from orbiter.objects.project import OrbiterProject
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator, OrbiterTask, OrbiterTaskDependency
from orbiter.objects.task_group import OrbiterTaskGroup
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_filter_rule,
    task_rule,
    task_dependency_rule,
    cannot_map_rule,
    post_processing_rule,
)
from orbiter.rules.rulesets import (
    DAGFilterRuleset,
    DAGRuleset,
    TaskFilterRuleset,
    TaskRuleset,
    TaskDependencyRuleset,
    PostProcessingRuleset,
    TranslationRuleset,
)


def file_and_name_to_import(
    file: str,
    name: str,
    return_type: Literal["str", "callable"] = "callable",
    exists: bool = False,
):
    """Turns a file path and function name into an imported python object

    **Optional Environmental Variable**: `ORBITER_DAG_FACTORY_DIR_PREFIX`: The original directory prefix to replace.
    Replaces with `./` (so files should be local to current directory), defaults to `/usr/local/airflow/`

    :param file: python file path - e.g. /usr/local/airflow/workflow/dags/expand_tasks.py
    :type file: str
    :param name: python function name - e.g. example_task_mapping
    :type name: str
    :param exists: Whether the file must exist, defaults to False (Warning: Might encounter false-negatives)
    :type exists: bool, optional
    :param return_type: The type of object to return, either 'str' or 'callable', defaults to 'callable'
    :type return_type: Literal['str', 'callable'], optional
    :return: python callable
    :rtype Callable
    """

    dir_prefix = os.getenv("ORBITER_DAG_FACTORY_DIR_PREFIX", "/usr/local/airflow/")
    file = Path(file.replace(dir_prefix, ""))
    if exists and not file.exists():
        raise FileNotFoundError(f"File {file} does not exist")
    callable_file_as_module = str(file).replace("/", ".").replace(".py", "")
    if return_type == "str":
        return f"{callable_file_as_module}.{name}"
    else:
        (_, python_callable) = import_from_qualname(f"{callable_file_as_module}.{name}")
        return python_callable


def callback_args(val: dict) -> dict:
    # Callbacks, e.g. on_failure_callback, are split as two keys: on_x_callback_file and on_x_callback_name
    callback_keys = {}
    seen_callback_types = set()
    all_keys = list(val.keys())
    while len(all_keys):
        k = all_keys.pop(0)

        # Search for
        try:
            (callback_type, name_or_file) = re.match(
                r"on_([a-z]+)_callback_(name|file)", k
            ).groups()

            if callback_type not in seen_callback_types:
                seen_callback_types.add(callback_type)
                callback_file = val.pop(f"on_{callback_type}_callback_file")
                callback_name = val.pop(f"on_{callback_type}_callback_name")
                [*module, function] = file_and_name_to_import(
                    callback_file, callback_name, return_type="str"
                ).split(".")
                src_file = Path(
                    inspect.getfile(
                        file_and_name_to_import(callback_file, callback_name)
                    )
                )
                if src_file.exists():
                    callback_includes = {
                        OrbiterInclude(
                            filepath=f"{'/'.join(module)}.py",
                            contents=(src_file.read_text()),
                        )
                    }
                else:
                    logger.warning(
                        f"Callback file {src_file} does not exist! Unable to copy contents"
                    )
                    callback_includes = {}
                callback_keys["orbiter_includes"] = (
                    callback_keys.get("orbiter_includes", set()) | callback_includes
                )
                callback_keys[f"on_{callback_type}_callback"] = OrbiterCallback(
                    imports=[
                        OrbiterRequirement(
                            package="apache-airflow",
                            module=".".join(module),
                            names=[function],
                        )
                    ],
                    function=function,
                )
        except (ValueError, AttributeError):
            pass
    return callback_keys


@dag_filter_rule
def basic_dag_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@dag_rules`"""
    val = deepcopy(val)
    dags = []
    default = val.pop("default", {})
    for k, v in val.items():
        if isinstance(v, MutableMapping):
            v["__dag_id"] = k
            v["__default"] = default
            dags.append(v)
    return dags


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Translate input into an `OrbiterDAG`

    ```pycon
    >>> input_dict = {
    ...   '__dag_id': 'test_expand',
    ...   '__default': {"doc_md": "foo"},
    ...   'default_args': {'owner': 'custom_owner'},
    ...   'description': 'test expand',
    ...   'schedule_interval': '0 3 * * *',
    ...   'default_view': 'graph',
    ...   'tasks': {'foo': {'operator': 'airflow.operators.python.PythonOperator'}}
    ... }
    >>> basic_dag_rule(input_dict)
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='test_expand', schedule='0 3 * * *', start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, default_args={'owner': 'custom_owner'}, doc_md='foo', description='test expand', default_view='graph'):

    ```
    """  # noqa: E501
    if "__dag_id" in val:
        val = deepcopy(val)
        dag_id = val.pop("__dag_id")
        _ = val.pop("tasks", [])
        _ = val.pop("task_groups", [])
        default = val.pop("__default", {})
        val = val | default
        schedule = val.pop("schedule_interval", None) or val.pop("schedule", None)
        return OrbiterDAG(
            dag_id=dag_id,
            file_path=f"{inflection.underscore(dag_id)}.py",
            schedule=schedule,
            **callback_args(val),
            **val,
        )
    return None


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@task_rules`

    ```pycon
    >>> basic_task_filter({
    ...   '__dag_id': 'test_expand',
    ...   '__default': {"doc_md": "foo"},
    ...   'default_args': {'owner': 'custom_owner', 'start_date': '2 days'},
    ...   'description': 'test expand',
    ...   'schedule_interval': '0 3 * * *',
    ...   'default_view': 'graph',
    ...   'tasks': {'foo': {'operator': 'airflow.operators.python.PythonOperator'}}
    ... })
    [{'operator': 'airflow.operators.python.PythonOperator', '__task_id': 'foo'}]

    ```
    """
    tasks = []
    if isinstance(_tasks := val.get("tasks", {}), MutableMapping):
        for k, v in _tasks.items():
            if isinstance(v, MutableMapping):
                v["__task_id"] = k
                tasks.append(v)
    return tasks or None


@task_filter_rule
def basic_task_group_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@task_rules` to produce @task_groups

    ```pycon
    >>> basic_task_group_filter({
    ...   '__dag_id': 'test_expand',
    ...   '__default': {"doc_md": "foo"},
    ...   'default_args': {'owner': 'custom_owner', 'start_date': '2 days'},
    ...   'description': 'test expand',
    ...   'schedule_interval': '0 3 * * *',
    ...   'default_view': 'graph',
    ...   'task_groups': {'task_group1': {'tooltip': 'this is a task group', 'dependencies': ['task_1']}}
    ... })
    [{'tooltip': 'this is a task group', 'dependencies': ['task_1'], '__task_group_id': 'task_group1'}]

    ```
    """
    task_groups = []
    if isinstance(_task_groups := val.get("task_groups", {}), MutableMapping):
        for k, v in _task_groups.items():
            if isinstance(v, MutableMapping):
                v["__task_group_id"] = k
                task_groups.append(v)
    return task_groups or None


def task_common_args(val: dict) -> dict:
    """Extracts common task arguments, such as callbacks and task_id"""
    _ = val.pop("dependencies", [])
    _ = val.pop("task_group_name", "")
    return {"task_id": val.pop("__task_id"), **callback_args(val)}


@task_rule(priority=10)
def task_group_rule(val: dict) -> OrbiterTaskGroup | None:
    """Translate input into an `OrbiterTaskGroup`

    ```pycon
    >>> task_group_rule({
    ...   '__task_group_id': 'task_group1',
    ...   'tooltip': 'this is a task group',
    ...   'dependencies': ['task_1']
    ... })
    with TaskGroup(group_id='task_group1') as task_group1:

    ```
    """
    val = deepcopy(val)
    if task_group_id := val.pop("__task_group_id", ""):
        _ = val.pop("dependencies", [])
        return OrbiterTaskGroup(
            tasks={},
            task_group_id=task_group_id,
            **callback_args(val),
        )
    return None


@task_rule(priority=20)
def python_operator_rule(val: dict) -> OrbiterOperator | None:
    """Translate input into an `PythonOperator`, inlining the function

    !!! note

        `python_callable_file` source file (containing `python_callable_name`) must be accessible
        relative to `$INPUT_DIR`
        (excluding a configurable `ORBITER_DAG_FACTORY_DIR_PREFIX` prefix, default `/usr/local/airflow/`)

    """
    val = deepcopy(val)
    if (
        (
            (val.get("operator", "") == "airflow.operators.python.PythonOperator")
            or (
                val.get("operator", "")
                == "airflow.operators.python_operator.PythonOperator"
            )
        )
        and (python_callable_file := val.pop("python_callable_file"))
        and (python_callable_name := val.pop("python_callable_name"))
    ):
        _ = val.pop("operator", [])
        # noinspection PyUnboundLocalVariable
        return OrbiterPythonOperator(
            **task_common_args(val),
            python_callable=file_and_name_to_import(
                python_callable_file, python_callable_name
            ),
            **val,
        )
    else:
        return None


@task_rule(priority=1)
def basic_task_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate input into an Operator (e.g. `OrbiterBashOperator`). will be applied first, with a higher priority

    ```pycon
    >>> input_dict = {
    ...   '__task_id': 'request',
    ...   'operator': 'airflow.operators.bash.BashOperator',
    ...   'bash_command': 'echo hi',
    ...   'doc_md': 'foo'
    ... }
    >>> basic_task_rule(input_dict)
    request_task = BashOperator(task_id='request', bash_command='echo hi', doc_md='foo')

    ```
    """
    if (
        "operator" in val
        and "python_callable_file" not in val
        and "python_callable_name" not in val
    ):
        val = deepcopy(val)
        operator = val.pop("operator")
        module, cls = operator.rsplit(".", 1)
        return OrbiterTask(
            **task_common_args(val),
            imports=[
                OrbiterRequirement(
                    package="apache-airflow",
                    module=module,
                    names=[cls],
                )
            ],
            **val,
        )
    else:
        return None


@task_dependency_rule
def basic_task_dependency_rule(val: OrbiterDAG) -> list | None:
    """Translate input into a list of task dependencies

    ```pycon
    >>> from orbiter.objects.operators.empty import OrbiterEmptyOperator
    >>> basic_task_dependency_rule(OrbiterDAG(dag_id='test_expand', file_path='test_expand', tasks={
    ...     'foo': OrbiterEmptyOperator(task_id='foo', orbiter_kwargs={'val': {'dependencies': ['bar']}}),
    ...     'bar': OrbiterEmptyOperator(task_id='bar', orbiter_kwargs={'val': {'dependencies': ['baz', 'bop']}}),
    ...     'baz': OrbiterEmptyOperator(task_id='baz', orbiter_kwargs={}),
    ...     'bop': OrbiterEmptyOperator(task_id='bop', orbiter_kwargs={})
    ... }))
    [bar >> foo, baz >> bar, bop >> bar]

    ```
    """
    task_dependencies = []
    for task in val.tasks.values():
        if (
            dependencies := (task.orbiter_kwargs or {})
            .get("val", {})
            .get("dependencies", [])
        ):
            for dependency in dependencies:
                # dependencies are upstream, not downstream, so need to be reversed
                task_dependencies.append(
                    OrbiterTaskDependency(task_id=dependency, downstream=task.task_id)
                )
    return task_dependencies


# noinspection t
@post_processing_rule
def move_tasks_to_task_group(val: OrbiterProject):
    task_group_names = set()
    # Move all the tasks into their task groups, now that we know about everything
    for dag in val.dags.values():
        dag: OrbiterDAG
        for task in deepcopy(list(dag.tasks.values())):
            task: OrbiterOperator | OrbiterTaskGroup
            if task_group_name := task.orbiter_kwargs.get("val", {}).get(
                "task_group_name"
            ):
                task_group_names.add(task_group_name)
                dag.tasks[task_group_name].tasks.append(task)
                del dag.tasks[task.task_id]

    # Fix task group names in dependencies
    for task_group_name in task_group_names:
        for dag in val.dags.values():
            dag: OrbiterDAG
            for task in dag.tasks.values():
                task: OrbiterOperator | OrbiterTaskGroup
                # Fix - task_a >> task_group_task ==> task_a >> task_group
                task.downstream = set(
                    (
                        downstream
                        if downstream != f"{task_group_name}_task"
                        else task_group_name
                    )
                    for downstream in task.downstream
                )


translation_ruleset = TranslationRuleset(
    file_type={FileTypeYAML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(
        ruleset=[basic_task_filter, basic_task_group_filter]
    ),
    task_ruleset=TaskRuleset(
        ruleset=[
            python_operator_rule,
            task_group_rule,
            basic_task_rule,
            cannot_map_rule,
        ]
    ),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[basic_task_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[move_tasks_to_task_group]),
)
