from __future__ import annotations

from orbiter import clean_value
from orbiter.file_types import FileTypeXML, xmltodict_parse
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.bash import OrbiterBashOperator
from orbiter.objects.task import OrbiterOperator
from orbiter.objects.task_group import OrbiterTaskGroup
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_filter_rule,
    task_rule,
    task_dependency_rule,
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
def basic_dag_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@dag_rules`"""
    return val.get("tes:jobgroup")


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Translate input into an `OrbiterDAG`"""
    dag_id = val.get("tes:name")
    return OrbiterDAG(dag_id=dag_id, file_path=f"{dag_id}.py")


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@task_rules`"""
    tasks = []
    basedir = val.get("__file").parent.resolve()
    for path in basedir.rglob("*"):
        if path.name == "Job.xml":
            with path.open() as f:
                task = xmltodict_parse(f.read())
                tasks.append(task["tes:job"][0])
    return tasks


@task_rule(priority=2)
def basic_ssh_task(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate input into an Operator (e.g. `OrbiterBashOperator`). will be applied first, with a higher priority"""
    task_id = val.get("tes:name")
    if task_id:
        return OrbiterBashOperator(task_id=task_id, bash_command=val.get("tes:command"))


@task_dependency_rule
def basic_task_dependency_rule(val: OrbiterDAG) -> list | None:
    """Translate input into a list of task dependencies"""
    basedir = val.orbiter_kwargs["val"]["__file"].parent.resolve()
    dependencies = []
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
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[basic_ssh_task]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[basic_task_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
