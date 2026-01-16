"""Demo translation ruleset for Tidal XML files to Airflow DAGs

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.
"""
from __future__ import annotations

from pathlib import Path

from orbiter import clean_value
from orbiter.file_types import FileTypeXML, xmltodict_parse
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.bash import OrbiterBashOperator
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_filter_rule,
    task_rule,
    task_dependency_rule,
    create_cannot_map_rule_with_task_id_fn
)
from orbiter.rules.rulesets import (
    DAGFilterRuleset,
    DAGRuleset,
    OrbiterTaskDependency,
    TaskFilterRuleset,
    TaskRuleset,
    TaskDependencyRuleset,
    PostProcessingRuleset,
    TranslationRuleset,
)


@dag_filter_rule
def job_group_dag_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@dag_rules`"""
    return val.get("tes:jobgroup")


@dag_rule
def job_group_dag_rule(val: dict) -> OrbiterDAG | None:
    """Translate `tes:jobgroup` into an `OrbiterDAG`

    ```pycon
    >>> job_group_dag_rule({"tes:name": "foo"}) # doctest: +ELLIPSIS
    from airflow import DAG...
    with DAG(dag_id='foo'):...

    ```
    """
    dag_id = val.get("tes:name")
    return OrbiterDAG(dag_id=dag_id, file_path=f"{dag_id}.py")


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    tasks = []
    __file: Path = val.get("__file")
    __input_dir: Path = val.get("__input_dir")
    basedir: Path = (__input_dir / __file.relative_to(__file.parts[0])).parent
    for path in basedir.rglob("*"):
        if path.name == "Job.xml":
            with path.open() as f:
                task = xmltodict_parse(f.read())
                tasks.append(task["tes:job"][0])
    return tasks

def task_common_args(val: dict) -> dict:
    """Common properties for all tasks
    - `tes:name` -> `task_id`
    """
    return {"task_id": val.get("tes:name", "UNKNOWN")}



@task_rule(priority=2)
def basic_command_task(val: dict) -> OrbiterBashOperator | None:
    """Translate `tes:command` into a `BashOperator`

    ```pycon
    >>> basic_command_task(val={'tes:name': 'foo', 'tes:command': 'echo "hi"'})
    foo_task = BashOperator(task_id='foo', bash_command='echo "hi"')

    ```
    """
    if command := val.get("tes:command"):
        return OrbiterBashOperator(**task_common_args(val), bash_command=command)
    return None


@task_dependency_rule
def job_dependency_rule(val: OrbiterDAG) -> list | None:
    """Use `Job.dependencies.xml` to find dependencies"""
    dependencies = []
    __file: Path = val.orbiter_kwargs.get('val', {}).get("__file")
    __input_dir: Path = val.orbiter_kwargs.get('val', {}).get("__input_dir")
    basedir: Path = (__input_dir / __file.relative_to(__file.parts[0])).parent
    for path in basedir.rglob("*"):
        if path.name == "Job.dependencies.xml":
            with path.open() as f:
                dependency = xmltodict_parse(f.read())["Dependencies"]
                for d in dependency:
                    if "tes:jobdependency" in d:
                        dependencies.extend(d["tes:jobdependency"])

    task_dependencies = []
    for dependency in dependencies:
        task_dependencies.append(
            OrbiterTaskDependency(
                task_id=clean_value(dependency["tes:depjobname"]),
                downstream=clean_value(dependency["tes:jobname"]),
            )
        )
    return task_dependencies


translation_ruleset = TranslationRuleset(
    file_type={FileTypeXML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[job_group_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[job_group_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[basic_command_task, create_cannot_map_rule_with_task_id_fn(lambda val: task_common_args(val)["task_id"])]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[job_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
