from __future__ import annotations

import inflection
import jq
import json

from orbiter.file_types import FileTypeJSON
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
    OrbiterTaskDependency,
)


@dag_filter_rule
def basic_dag_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@dag_rules`"""
    val = json.loads(json.dumps(val, default=str))  # pre-serialize values, for JQ

    try:
        return jq.compile(""".data.json.pipeline""").input_value(val).all()
    except StopIteration:
        pass
    return None


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Translate input into an `OrbiterDAG`"""
    dag_id = val["__file"].stem
    return OrbiterDAG(
        dag_id=dag_id,
        file_path=f"{dag_id}.py",
        doc_md="**Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
        "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
        "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
    )


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@task_rules`"""
    return val.get("stages", [])


@task_rule(priority=2)
def basic_task_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate input into an Operator (e.g. `OrbiterBashOperator`). will be applied first, with a higher priority"""
    val = json.loads(json.dumps(val, default=str))  # pre-serialize values, for JQ
    task_id = inflection.underscore(val["name"])
    commands = []

    # Considering only the first branch for each stage in pipeline for now
    # TODO: Add support for multiple branches
    try:
        steps = jq.compile(""".branches[] | .steps""").input_value(val).first()
    except StopIteration:
        return None

    for step in steps:
        command = []
        command.append(step["name"])
        for argument in step["arguments"]:
            argument_defn = argument["value"]
            command.append(
                f"'{argument_defn["value"]}'"
                if argument_defn["isLiteral"]
                else argument_defn["value"]
            )
        commands.append(command)
    commands = [" ".join(command) for command in commands]
    bash_command = "; ".join(commands)

    return OrbiterBashOperator(
        task_id=task_id,
        bash_command=bash_command,
    )


@task_dependency_rule
def basic_task_dependency_rule(val: OrbiterDAG) -> list | None:
    """Translate input into a list of task dependencies"""
    task_dependencies = []
    tasks = list(val.tasks.values())

    for i in range(len(tasks) - 1):
        task_dependencies.append(
            OrbiterTaskDependency(
                task_id=tasks[i].task_id, downstream=tasks[i + 1].task_id
            )
        )

    return task_dependencies


translation_ruleset = TranslationRuleset(
    file_type={FileTypeJSON},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[basic_task_rule, cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[basic_task_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)

if __name__ == "__main__":
    import doctest

    doctest.testmod(
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL
    )
