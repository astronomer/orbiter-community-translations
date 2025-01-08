"""
"""

from __future__ import annotations

from orbiter.objects import conn_id
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.operators.ssh import OrbiterSSHOperator
from orbiter.objects.task import OrbiterOperator, OrbiterTaskDependency
from orbiter.objects.task_group import OrbiterTaskGroup
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_filter_rule,
    task_rule,
    task_dependency_rule,
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

from orbiter_translations.esp.wld_parser import FileTypeWLD


@dag_filter_rule
def basic_dag_filter(val: dict) -> list | None:
    """Filter to only top-level dictionaries returned by the WLD Parser"""
    if val and isinstance(val, dict):
        if "__file" in val:
            val.pop("__file")
        return list(val.values())


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Use 'APPL' key for a DAG ID"""
    if "APPL" in val:
        dag_id = val["APPL"]
        return OrbiterDAG(dag_id=dag_id, file_path=f"{dag_id}.py")


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Filter input to dicts with a key containing the word 'job'"""
    return [
        v
        for v in (v for v in val.values() if isinstance(v, dict))
        if any("job" in _k.lower() for _k in v.keys())
    ] or None


def common_args(val: dict) -> dict:
    try:
        job_key = next(key for key in val.keys() if "job" in key.lower())
        task_id = val[job_key]
        return {"task_id": task_id}
    except StopIteration:
        return {}


@task_rule(priority=10)
def win_ssh_task_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate input into an SSHOperator from 'NT_JOB' key"""
    try:
        job_key = next(key for key in val.keys() if "job" in key.lower())
        if job_key == "NT_JOB":
            return OrbiterSSHOperator(
                **common_args(val),
                command=val.get("CMDNAME"),
                **conn_id(val.get("AGENT"), prefix="ssh", conn_type="ssh"),
            )
    except StopIteration:
        pass


@task_rule(priority=10)
def linux_ssh_task_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate input into an SSHOperator from 'LINUX_JOB' key"""
    try:
        job_key = next(key for key in val.keys() if "job" in key.lower())
        if job_key == "LINUX_JOB":
            return OrbiterSSHOperator(
                **common_args(val),
                command=val.get("SCRIPTNAME"),
                **conn_id(val.get("AGENT"), prefix="ssh", conn_type="ssh"),
            )
    except StopIteration:
        pass


@task_rule(priority=2)
def basic_task_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate input into an EmptyOperator if nothing else has matched, but it still looks right"""
    if val and isinstance(val, dict):
        return OrbiterEmptyOperator(**common_args(val))


@task_dependency_rule
def basic_task_dependency_rule(val: OrbiterDAG) -> list | None:
    """Translate input into a list of task dependencies"""
    task_dependencies = []
    for task in val.tasks.values():
        original_task_kwargs = task.orbiter_kwargs["val"]
        if "RELEASE" in original_task_kwargs:
            dependencies = original_task_kwargs["RELEASE"].split(",")
            dependencies = [
                dependency.replace("(", "").replace(")", "").replace(" ", "")
                for dependency in dependencies
            ]
            task_dependencies.append(
                OrbiterTaskDependency(task_id=task.task_id, downstream=dependencies)
            )
    return task_dependencies or None


translation_ruleset = TranslationRuleset(
    file_type={FileTypeWLD},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(
        ruleset=[
            linux_ssh_task_rule,
            win_ssh_task_rule,
            basic_task_rule,
            cannot_map_rule,
        ]
    ),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[basic_task_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
