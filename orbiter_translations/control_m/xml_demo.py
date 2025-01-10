"""
Demo Control-M XML Translation Ruleset

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

```pycon
>>> translation_ruleset.test({
...     "DEFTABLE": {
...       '@xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
...       'SMART_FOLDER': [
...         {"@FOLDER_NAME": "demo", "JOB": [
...           {"@JOBNAME": "TaskA", "@APPL_TYPE": "OS",
...            "@CMDLINE": "echo job a", 'OUTCOND': [{'@NAME': 'A', "@SIGN": "+"}]},
...           {"@JOBNAME": "TaskB", "@APPL_TYPE": "OS",
...            "@FILE_PATH": "/x/y/z", "@FILE_NAME": "job_b.sh", 'INCOND': [{"@NAME": "A"}]}
...         ]}
...       ]
...     }
... }).dags['demo']
... # doctest: +ELLIPSIS
from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='demo', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):
    task_a_task = BashOperator(task_id='task_a', bash_command='echo job a')
    task_b_task = BashOperator(task_id='task_b', bash_command='/x/y/z/job_b.sh')
    task_a_task >> task_b_task

```
"""  # noqa: E501
from __future__ import annotations

from pathlib import Path

import inflection
from loguru import logger
from orbiter import clean_value
from orbiter.file_types import FileTypeXML
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.bash import OrbiterBashOperator
from orbiter.objects.task import OrbiterTaskDependency
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_rule,
    task_filter_rule,
    task_dependency_rule,
    cannot_map_rule,
)
from orbiter.rules.rulesets import (
    DAGFilterRuleset,
    DAGRuleset,
    TaskDependencyRuleset,
    TaskFilterRuleset,
    TaskRuleset,
    TranslationRuleset,
    PostProcessingRuleset,
)


# noinspection t
@dag_filter_rule(priority=1)
def dag_filter_rule(val) -> list[dict] | None:
    """
    Descend until we find a key titled SMART_FOLDER or FOLDER that has a list of entries

    ```pycon
    >>> dag_filter_rule(val={"DEFTABLE": [{
    ...   '@xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    ...   "SMART_FOLDER": [{'@FOLDER_NAME': 'ONE', "FOO": "BAR"}],
    ...   "FOLDER": [{'@FOLDER_NAME': 'TWO', "BAZ": "BOP"}]
    ... }]})
    [{'@FOLDER_NAME': 'ONE', 'FOO': 'BAR'}, {'@FOLDER_NAME': 'TWO', 'BAZ': 'BOP'}]

    ```
    """

    def _find(_key, _val) -> list:
        results = []
        if isinstance(_val, list):
            if _key in ["SMART_FOLDER", "FOLDER"]:
                return _val
            else:
                for __val in _val:
                    results.extend(_find(_key, __val))
        elif isinstance(_val, dict):
            for __key, __val in _val.items():
                results.extend(_find(__key, __val))
        return results

    return _find("", val) or None


@dag_rule(priority=1)
def basic_dag_rule(val) -> OrbiterDAG | None:
    if isinstance(val, dict) and "@FOLDER_NAME" in val:
        dag_id = val["@FOLDER_NAME"]
        params = {
            "dag_id": dag_id,
            "file_path": Path(f"{inflection.underscore(dag_id)}.py"),
            "doc_md": "**Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
            "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
            "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
        }
        return OrbiterDAG(**params)
    return None


def task_common_args(val: dict) -> dict:
    """
    Common mappings for all tasks
    """

    params = {"task_id": val.get("@JOBNAME", "UNKNOWN")}
    return params


@task_rule(priority=2)
def bash_script_rule(val) -> OrbiterBashOperator | None:
    """
    `.Type==Job:Script` => `BashOperator(bash_command=.FilePath/.FileName)`

    ```pycon
    >>> bash_script_rule(val={
    ...   "@APPL_TYPE": "OS", "@JOBNAME": "foo", "@FILE_PATH": "/x/y/z", "@FILE_NAME": "foo.sh"
    ... })
    foo_task = BashOperator(task_id='foo', bash_command='/x/y/z/foo.sh')

    ```
    """
    if (
        val.get("@APPL_TYPE", "") == "OS"
        and val.get("@FILE_PATH")
        and val.get("@FILE_NAME")
    ):
        return OrbiterBashOperator(
            bash_command=val["@FILE_PATH"] + "/" + val["@FILE_NAME"],
            **task_common_args(val),
        )
    return None


@task_rule(priority=2)
def bash_command_rule(val) -> OrbiterBashOperator | None:
    """
    .Type==Job:Command => BashOperator(bash_command=.Command)

    ```pycon
    >>> bash_command_rule(val={
    ...   "@APPL_TYPE": "OS", "@JOBNAME": "foo", "@CMDLINE": "echo 'hi'"
    ... })
    foo_task = BashOperator(task_id='foo', bash_command="echo 'hi'")

    ```
    """
    if val.get("@APPL_TYPE", "") == "OS" and val.get("@CMDLINE"):
        return OrbiterBashOperator(
            bash_command=val["@CMDLINE"], **task_common_args(val)
        )
    return None


@task_rule(priority=1)
def _cannot_map_rule(val):
    """Add task_ids on top of common 'cannot_map_rule'"""
    task = cannot_map_rule(val)
    task.task_id = clean_value(task_common_args(val)["task_id"])
    return task


@task_filter_rule(priority=1)
def task_filter_rule(val) -> list[dict] | None:
    """
    The key is 'JOB' and it's a list of dicts with @JOBNAME

    >>> task_filter_rule(val={"JOB": [{"@JOBNAME": "foo"}]})
    [{'@JOBNAME': 'foo'}]
    """
    if val.get("JOB", []) and isinstance(val.get("JOB"), list):
        return [v for v in val.get("JOB") if isinstance(v, dict) and v.get("@JOBNAME")]
    return None


# noinspection t
@task_dependency_rule(priority=1)
def simple_event_task_dependencies(val: OrbiterDAG) -> list | None:
    """
    Parses task-based `eventToAdd` and `eventsToWaitFor`

    ```pycon
    >>> simple_event_task_dependencies(val=OrbiterDAG(
    ...   dag_id="foo", file_path="",
    ...   tasks={
    ...     "a": OrbiterBashOperator(task_id="a", bash_command="echo 'A'", orbiter_kwargs={"val": {
    ...         "OUTCOND": [{"@SIGN": "+", "@NAME": "A"}]
    ...     }}),
    ...     "b": OrbiterBashOperator(task_id="b", bash_command="echo 'B'", orbiter_kwargs={"val": {
    ...        "INCOND": [{"@SIGN": "+", "@NAME": "A"}]
    ...     }}),
    ...   },
    ...   orbiter_kwargs={}
    ... ))
    [a >> b]

    ```
    """
    task_dependencies = []
    event_to_task_id = {}
    if isinstance(val, OrbiterDAG) and hasattr(val, "tasks"):
        # map over the found tasks once to record which task is adding which event
        for task in val.tasks.values():
            for event in (
                event
                for event in task.orbiter_kwargs["val"].get("OUTCOND", [])
                if isinstance(event, dict)
                and event.get("@SIGN", "") in ("+", "ADD")
                and event.get("@NAME")
                and ("@ODATE" not in event or event.get("@ODATE", "") == "ODAT")
            ):
                if len(event.keys()) > 3:
                    logger.warning(
                        f"-----------\n"
                        f"More than `@NAME`, `@ODATE==ODAT` `@SIGN` in `OUTCOND` not yet supported!"
                        f"\nevent={event}"
                        f"\ntask={task}"
                        f"\nval={task.orbiter_kwargs['val']}"
                        f"\n-----------"
                    )
                else:
                    event_to_task_id[event.get("@NAME")] = task.task_id

        # map over the found tasks a second time to add a dependency for each task waiting for an event
        for task in val.tasks.values():
            if any(
                True
                for event in task.orbiter_kwargs["val"].get("INCOND", [])
                if event.get("@AND_OR", "") in ("O", "OR")
            ):
                task.trigger_rule = "one_success"

            for event in (
                event
                for event in task.orbiter_kwargs["val"].get("INCOND", [])
                if isinstance(event, dict)
                and event.get("@NAME")
                and ("@ODATE" not in event or event.get("@ODATE", "") == "ODAT")
            ):
                if len(event.keys()) > 3:
                    logger.warning(
                        f"-----------\n"
                        f"More than `@NAME`, `@ODATE==ODAT`, and `@AND_OR` in `INCOND` not yet supported!"
                        f"\nevent={event}"
                        f"\ntask={task}"
                        f"\nval={task.orbiter_kwargs['val']}"
                        f"\n-----------"
                    )
                else:
                    try:
                        task_dependencies.append(
                            OrbiterTaskDependency(
                                task_id=event_to_task_id[event["@NAME"]],
                                downstream=task.task_id,
                            )
                        )
                    except KeyError:
                        logger.error(
                            f"-----------\n"
                            f"Event={event['@NAME']} not found in events added by tasks in this DAG!"
                            f"\ntask={task}"
                            f"\nval={task.orbiter_kwargs['val']}"
                            f"\n-----------"
                        )
        return task_dependencies
    return None


translation_ruleset: TranslationRuleset = TranslationRuleset(
    file_type={FileTypeXML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[dag_filter_rule]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[task_filter_rule]),
    task_ruleset=TaskRuleset(
        ruleset=[
            bash_script_rule,
            bash_command_rule,
            _cannot_map_rule,
        ]
    ),
    task_dependency_ruleset=TaskDependencyRuleset(
        ruleset=[simple_event_task_dependencies]
    ),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)

if __name__ == "__main__":
    import doctest

    doctest.testmod(
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL
    )
