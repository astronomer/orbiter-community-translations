from __future__ import annotations
from pathlib import Path

import inflection
import jq
from orbiter.file_types import FileTypeXML
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.project import OrbiterProject
from orbiter.objects.task import OrbiterOperator
from orbiter.objects.task_group import OrbiterTaskGroup
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_filter_rule,
    task_rule,
    task_dependency_rule,
    post_processing_rule,
    cannot_map_rule,
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


@dag_filter_rule
def basic_dag_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@dag_rules`"""
    dag = [
        {dag_struct_key: struct}
        for dag_struct_key, struct in val.get("uc-export", [{}])[0].items()
        if dag_struct_key in ["JOBP", "JOBS", "SCRI"]
    ]
    return dag or None


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Translate input into an `OrbiterDAG`"""
    if isinstance(val, dict):
        try:
            dag_id = jq.compile(""".JOBP[0] | ."@name" """).input_value(val).first()
            if not dag_id:
                raise StopIteration("DAG struct not relavant")
            return OrbiterDAG(
                dag_id=dag_id,
                file_path=Path(f"{inflection.underscore(dag_id)}.py"),
                doc_md=" **Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
                "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
                "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
            )
        except StopIteration:
            pass
    return None


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@task_rules`"""
    for k, v in val.items():
        pass
    return []


@task_rule(priority=2)
def basic_task_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate input into an Operator (e.g. `OrbiterBashOperator`). will be applied first, with a higher priority"""
    if "task_id" in val:
        return OrbiterEmptyOperator(task_id=val["task_id"])
    else:
        return None


@task_dependency_rule
def basic_task_dependency_rule(val: OrbiterDAG) -> list | None:
    """Translate input into a list of task dependencies"""
    for task_dependency in val.orbiter_kwargs["task_dependencies"]:
        pass
    return []


@post_processing_rule
def basic_post_processing_rule(val: OrbiterProject) -> None:
    """Modify the project in-place, after all other rules have applied"""
    for dag_id, dag in val.dags.items():
        for task_id, task in dag.tasks.items():
            pass


translation_ruleset = TranslationRuleset(
    file_type={FileTypeXML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[basic_task_rule, cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[basic_task_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[basic_post_processing_rule]),
)
