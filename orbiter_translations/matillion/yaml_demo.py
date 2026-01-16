"""Demonstration of translating Matillion YAML files into Airflow DAGs.

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

```pycon
>>> translation_ruleset.test('''
...  name: matillion_pipeline
...  type: orchestration
...  pipeline:
...    components:
...      start:
...        type: start
...        parameters:
...          componentName: Start
...        transitions:
...          unconditional: [print_hello_world]
...      print_hello_world:
...        type: python-pushdown
...        parameters:
...          componentName: Print Hello World
...          pythonScript: |
...            print('Hello, World!')
...        transitions:
...          unconditional: [print_pipeline_finished]
...      print_pipeline_finished:
...        type: end
...        parameters:
...          componentName: Print Pipeline Finished
...        transitions: {}
... ''').dags['matillion_pipeline']  # doctest: +ELLIPSIS
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
with DAG(dag_id='matillion_pipeline', ...):
    start_task = EmptyOperator(task_id='start')
<BLANKLINE>
    @task()
    def print_hello_world():
        print('Hello, World!')
    print_pipeline_finished_task = EmptyOperator(task_id='print_pipeline_finished')
    print_hello_world_task >> print_pipeline_finished_task
    start_task >> print_hello_world_task

```
"""
from __future__ import annotations

import textwrap
from pathlib import Path
import re

from orbiter.file_types import FileTypeYAML
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.operators.python import OrbiterPythonOperator, OrbiterDecoratedPythonOperator
from orbiter.objects.task import OrbiterTaskDependency
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_dependency_rule,
    task_filter_rule,
    task_rule,
    create_cannot_map_rule_with_task_id_fn
)
from orbiter.rules.rulesets import (
    TranslationRuleset,
    DAGFilterRuleset,
    DAGRuleset,
    TaskDependencyRuleset,
    TaskFilterRuleset,
    PostProcessingRuleset,
    TaskRuleset
)

### Helper
def _slugify(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", name).lower().strip("_")

### DAG Filter
@dag_filter_rule
def matillion_dag_filter(val: dict) -> list[dict] | None:
    """Extract `pipeline` value if `type` is `orchestration`.
    ```pycon
    >>> matillion_dag_filter(val={"type": "orchestration", "pipeline": {"foo": "bar"}})
    [{'foo': 'bar'}]

    ```
    """
    if val.get("type") == "orchestration" and "pipeline" in val:
        return [val["pipeline"]]
    return None


### DAG Rule
@dag_rule
def matillion_dag_rule(val: dict) -> OrbiterDAG:
    """
    ```pycon
    >>> matillion_dag_rule(val={"name": "matillion_pipeline"})  # doctest: +ELLIPSIS
    from airflow import DAG...
    with DAG(dag_id='matillion_pipeline', ...):...

    ```
    """
    # TODO - move this into orbiter.clean_value
    dag_id = _slugify(val.get("name", "matillion_pipeline"))
    return OrbiterDAG(
        dag_id=dag_id,
        file_path=Path(f"{dag_id}.py"),
        doc_md="**Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
            "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
            "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
    )

@task_filter_rule
def matillion_task_filter(val: dict) -> list[dict] | None:
    """
    ```pycon
    >>> matillion_task_filter(val={"components": {"foo": {"name": "Start"}}})
    [{'name': 'Start'}]

    ```
    """
    return list(val.get("components", {}).values())

def task_common_args(val: dict) -> dict:
    """
    Extracts common task arguments from the Matillion component parameters.
    """
    if componentName := val.get("parameters", {}).get("componentName"):
        return {
            "task_id": _slugify(componentName),
        }
    else:
        return {
            "task_id": "UNKNOWN",
        }

@task_rule(priority=10)
def start_task_rule(val: dict) -> OrbiterEmptyOperator | None:
    """
    ```pycon
    >>> start_task_rule(val={"type": "start", "parameters": {"componentName": "Start"}})
    start_task = EmptyOperator(task_id='start')

    ```
    """
    if val.get("type") == "start":
        return OrbiterEmptyOperator(**task_common_args(val))
    return None


@task_rule(priority=10)
def python_pushdown_rule(val: dict) -> OrbiterPythonOperator | None:
    """Map `python-pushdown` to `@task` via `.parameters.pythonScript`.
    ```pycon
    >>> python_pushdown_rule(val={
    ...     "type": "python-pushdown",
    ...     "parameters": {
    ...         "componentName": "Python Task",
    ...         "pythonScript": "print('Hello, World!')"
    ...     }
    ... })
    @task()
    def python_task():
        print('Hello, World!')

    ```
    """
    if val.get("type") == "python-pushdown":
        _common_args = task_common_args(val)
        task_id = _common_args['task_id']

        return OrbiterDecoratedPythonOperator(
            **_common_args,
            python_callable=textwrap.dedent(f"""
            def {task_id}():
                {val.get("parameters", {}).get("pythonScript", "# no script")}
            """),
        )
    return None


@task_rule(priority=10)
def end_task_rule(val: dict) -> OrbiterEmptyOperator | None:
    """Map `end` to `EmptyOperator`
    ```pycon
    >>> end_task_rule(val={
    ...     "type": "end",
    ...     "parameters": {
    ...         "componentName": "Print Pipeline Finished",
    ...     }
    ... })
    print_pipeline_finished_task = EmptyOperator(task_id='print_pipeline_finished')

    ```
    """
    if val.get("type") == "end":
        return OrbiterEmptyOperator(**task_common_args(val))
    return None

### Dependency Rule
@task_dependency_rule
def matillion_dependency_rule(val: OrbiterDAG) -> list[OrbiterTaskDependency]:
    """
    Reads 'unconditional' transitions and wires downstream dependencies.
    ```pycon
    >>> matillion_dependency_rule(val=OrbiterDAG(
    ...     tasks={
    ...         "start": OrbiterEmptyOperator(task_id="start"),
    ...         "print_hello_world": OrbiterEmptyOperator(task_id="print_hello_world"),
    ...         "print_pipeline_finished": OrbiterEmptyOperator(task_id="print_pipeline_finished"),
    ...     },
    ...     orbiter_kwargs={"val": {"components": {
    ...         "start": {
    ...             "parameters": {"componentName": "Start"},
    ...             "transitions": {
    ...                "unconditional": ["print_hello_world"]
    ...             }
    ...         },
    ...         "print_hello_world": {
    ...             "parameters": {"componentName": "Print Hello World"},
    ...             "transitions": {
    ...                 "unconditional": ["print_pipeline_finished"]
    ...             }
    ...         },
    ...         "print_pipeline_finished": {
    ...             "parameters": {"componentName": "Print Pipeline Finished"},
    ...             "transitions": {}
    ...         }
    ...     }}}, dag_id=".", file_path="."
    ... ))
    [start >> print_hello_world, print_hello_world >> print_pipeline_finished]

    ```
    """
    dependencies = []
    for component in val.orbiter_kwargs["val"].get("components", {}).values():
        src = _slugify(component["parameters"]["componentName"])
        for target in component.get("transitions", {}).get("unconditional", []):
            tgt = _slugify(target)
            if src in val.tasks and tgt in val.tasks:
                dependencies.append(
                    OrbiterTaskDependency(task_id=src, downstream=tgt)
                )
    return dependencies


translation_ruleset = TranslationRuleset(
    file_type={FileTypeYAML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[matillion_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[matillion_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[matillion_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[start_task_rule, end_task_rule, python_pushdown_rule, create_cannot_map_rule_with_task_id_fn(lambda val: task_common_args(val)["task_id"])]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[matillion_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
