"""
## Demo `translation_ruleset` Example
```pycon
>>> translation_ruleset.test({
...     "workflow-app": [{
...         '@xmlns:xsi': 'uri:oozie:workflow:1.0',
...         "@name": "demo",
...         "start": [{'@to': 'cleanup-node'}],
...         "action": [{
...             '@name': 'cleanup-node',
...             'fs': [{'delete': [{'@path': '/foo/bar.txt'}]}],
...             'ok': [{'@to': 'end'}],
...             'error': [{'@to': 'fail'}]
...         }],
...         "end": [{"@name": "end"}],
...         "kill": [{'name': 'fail', 'message': ["Demo workflow failed"]}]
...     }]
... }).dags['demo']
... # doctest: +ELLIPSIS
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='demo', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):
    start_task = EmptyOperator(task_id='start')
    cleanup_node_task = BashOperator(task_id='cleanup_node', bash_command='rm /foo/bar.txt')
    end_task = EmptyOperator(task_id='end')
    unknown_task = EmptyOperator(task_id='unknown', trigger_rule='one_failed')
    cleanup_node_task >> [end_task, fail_task]

```
"""

from __future__ import annotations

from itertools import chain
from pathlib import Path

import inflection
from orbiter import clean_value
from orbiter.file_types import FileTypeXML
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.bash import OrbiterBashOperator
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.task import OrbiterOperator, OrbiterTaskDependency
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_dependency_rule,
    task_rule,
    task_filter_rule,
    cannot_map_rule,
)
from orbiter.rules.rulesets import (
    DAGFilterRuleset,
    TranslationRuleset,
    TaskDependencyRuleset,
    DAGRuleset,
    TaskRuleset,
    TaskFilterRuleset,
    PostProcessingRuleset,
)


@dag_filter_rule
def dag_filter_rule(val: dict) -> list[dict] | None:
    """Basic filter rule to extract the `workflow-app` element
    ```pycon
    >>> dag_filter_rule(val={"workflow-app": [{
    ...   '@xmlns:xsi': 'uri:oozie:workflow:1.0',
    ...   "@name": "demo-wf",
    ...   "start": [{'@to': 'cleanup-node'}],
    ...   "action": [{
    ...       '@name': 'cleanup-node',
    ...       'fs': [{'delete': [{'@path': '/foo/bar.txt'}]}],
    ...       'ok': [{'@to': 'end'}],
    ...       'error': [{'@to': 'fail'}]
    ...   }],
    ...   "end": [{"@name": "end"}],
    ...   "kill": [{'name': 'fail', 'message': ["Demo workflow failed"]}]
    ... }]})
    ... # doctest: +ELLIPSIS
    [{'@xmlns:xsi': 'uri:oozie:workflow:1.0', '@name': 'demo-wf', ...}]

    ```
    """
    return val.get("workflow-app", []) or None


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG:
    """
    Map <workflow-app name="..." to DAG_ID and other properties

    ```pycon
    >>> basic_dag_rule(val={
    ...   '@xmlns:xsi': 'uri:oozie:workflow:1.0',
    ...   "@name": "demo-wf",
    ...   "start": [{'@to': 'cleanup-node'}],
    ...   "kill": [{'name': 'fail', 'message': ["Demo workflow failed"]}]
    ... })
    ... # doctest: +ELLIPSIS
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='demo_wf', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):


    ```
    """
    dag_id = val["@name"]
    return OrbiterDAG(
        dag_id=dag_id,
        file_path=Path(f"{inflection.underscore(dag_id)}.py"),
        doc_md="**Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
        "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
        "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
    )


@task_filter_rule()
def task_filter_rule(val: dict) -> list[dict]:
    """Extract elements of types: start, end, action.

    !!! note

        Add `@type=` to each element, to preserve the type of the element within the entry

    ```pycon
    >>> task_filter_rule(val={
    ...   '@xmlns:xsi': 'uri:oozie:workflow:1.0',
    ...   "@name": "demo-wf",
    ...   "start": [{'@to': 'cleanup-node'}],
    ...   "end": [{"@name": "end"}],
    ...   "kill": [{'name': 'fail', 'message': ["Demo workflow failed"]}]
    ... })
    [{'@to': 'cleanup-node', '@type': 'start'}, {'@name': 'end', '@type': 'end'}, {'name': 'fail', 'message': ['Demo workflow failed'], '@type': 'kill'}]

    ```
    """  # noqa: E501
    tasks = []
    for k, v in val.items():
        if not k.startswith("@") and isinstance(v, list):
            for task in v:
                task["@type"] = k
                tasks.append(task)
    return tasks


def task_common_args(val: dict) -> dict:
    """
    Common mappings for all tasks

    .NAME -> task_id
    """
    return {"task_id": val.get("@name", "UNKNOWN")}


# noinspection t
@task_rule(priority=2)
def fs_action(val: dict) -> OrbiterOperator | None:
    """
    ```pycon
    >>> fs_action(val={
    ...   '@type': 'action',
    ...   '@name': 'cleanup-node',
    ...   'fs': [{'delete': [{'@path': '/foo/bar.txt'}]}],
    ...   'error': [{'@to': 'fail'}],
    ...   'ok': [{'@to': 'end'}],
    ... })
    cleanup_node_task = BashOperator(task_id='cleanup_node', bash_command='rm /foo/bar.txt')

    ```
    """
    if val.get("@type") == "action" and "fs" in val:
        [fs_block] = val["fs"][:1] or [None]
        for action_type, actions in fs_block.items():
            # e.g. <delete>
            [action] = actions[:1] or [None]
            if action_type == "delete":
                bash_command = f"rm {action['@path']}"
            elif action_type == "move":
                bash_command = f"mv {action['@source']} {action['@target']}"
            else:
                return None
            return OrbiterBashOperator(
                bash_command=bash_command, **task_common_args(val)
            )
    return None


@task_rule(priority=2)
def start_or_end(val: dict) -> OrbiterOperator | None:
    """
    ```pycon
    >>> start_or_end(val={'@to': 'cleanup-node', '@type': 'start'})
    start_task = EmptyOperator(task_id='start')

    >>> start_or_end(val={'@name': 'end', '@type': 'end'})
    end_task = EmptyOperator(task_id='end')

    ```
    """
    if val.get("@type") in ("start", "end"):
        if val["@type"] == "start":
            val["@name"] = "start"
        return OrbiterEmptyOperator(**task_common_args(val))
    return None


@task_rule(priority=2)
def kill(val: dict) -> OrbiterOperator | None:
    """
    ```pycon
    >>> kill(val={'@name': 'fail', '@type': 'kill', 'message': ['Something failed!']})
    fail_task = EmptyOperator(task_id='fail', trigger_rule='one_failed')

    ```
    """
    if val.get("@type") == "kill":
        return OrbiterEmptyOperator(
            trigger_rule="one_failed",
            **task_common_args(val),
        )
    return None


@task_rule(priority=1)
def _cannot_map_rule(val):
    """Add task_ids on top of common 'cannot_map_rule'"""
    task = cannot_map_rule(val)
    task.task_id = clean_value(task_common_args(val)["task_id"])
    return task


@task_dependency_rule
def to_task_dependency(val: OrbiterDAG) -> list[OrbiterTaskDependency] | None:
    """
    ```pycon
    >>> to_task_dependency(val=OrbiterDAG(
    ...   dag_id="foo", file_path="",
    ...   tasks={
    ...     "start": OrbiterEmptyOperator(task_id="start", orbiter_kwargs={"val": {'ok': [{"@to": 'a'}]}}),
    ...     "a": OrbiterEmptyOperator(task_id="a", orbiter_kwargs={"val": {'ok': [{"@to": 'b'}]}}),
    ...     "b": OrbiterEmptyOperator(task_id="b", orbiter_kwargs={
    ...       "val": {"ok": [{'@to': 'end'}], 'error': [{'@to': 'fail'}]}
    ...     }),
    ...     "fail": OrbiterEmptyOperator(task_id="fail", orbiter_kwargs={"val": {}}),
    ...     "end": OrbiterEmptyOperator(task_id="end", orbiter_kwargs={"val": {}}),
    ...   },
    ...   orbiter_kwargs={}
    ... ))
    [start >> a, a >> b, b >> ['end', 'fail']]

    ```
    """
    task_dependencies = []
    for task in val.tasks.values():
        task_id = task.task_id
        downstream = [
            d
            for d in chain(
                [t["@to"] for t in task.orbiter_kwargs.get("val", {}).get("to", [])],
                [t["@to"] for t in task.orbiter_kwargs.get("val", {}).get("ok", [])],
                [t["@to"] for t in task.orbiter_kwargs.get("val", {}).get("error", [])],
            )
            if d
        ]
        if len(downstream) > 1:
            task_dependencies.append(
                OrbiterTaskDependency(task_id=task_id, downstream=downstream)
            )
        elif len(downstream) == 1:
            task_dependencies.append(
                OrbiterTaskDependency(task_id=task_id, downstream=downstream[0])
            )
    return task_dependencies


translation_ruleset: TranslationRuleset = TranslationRuleset(
    file_type={FileTypeXML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[dag_filter_rule]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[task_filter_rule]),
    task_ruleset=TaskRuleset(ruleset=[kill, fs_action, start_or_end, _cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[to_task_dependency]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)

if __name__ == "__main__":
    import doctest

    doctest.testmod(
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL
    )
