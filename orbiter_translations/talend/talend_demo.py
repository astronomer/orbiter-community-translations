from __future__ import annotations
from pathlib import Path

import inflection

from orbiter.file_types import FileTypeJSON
from orbiter.objects.include import OrbiterInclude
from orbiter.objects import OrbiterRequirement
from orbiter import clean_value
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.python import OrbiterPythonOperator
from orbiter.objects.task import OrbiterTaskDependency
from orbiter.rules import (
    task_dependency_rule,
    dag_filter_rule,
    dag_rule,
    task_filter_rule,
    task_rule,
    cannot_map_rule,
)
from orbiter.rules.rulesets import (
    TranslationRuleset,
    TaskDependencyRuleset,
    DAGFilterRuleset,
    DAGRuleset,
    TaskFilterRuleset,
    TaskRuleset,
    PostProcessingRuleset,
)


# noinspection t
@dag_filter_rule
def dag_filter_rule(val) -> list[dict] | None:

    if isinstance(val, dict) and "userFlow" in val:
        return [val["userFlow"]]
    return None


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:

    if isinstance(val, dict) and "label" in val:
        dag_id = inflection.underscore(val["label"])
        return OrbiterDAG(
            dag_id=dag_id,
            file_path=Path(f"{dag_id}.py"),
            doc_md=val.get("description", "Created from Talend pipeline"),
        )
    return None


def task_common_args(val: dict) -> dict:
    """
    Common mappings for all tasks
    """
    task_id = (
        val.get("data", {})
        .get("properties", {})
        .get("$componentMetadata", {})
        .get("name", "UNKNOWN")
    )

    params = {
        "task_id": task_id.replace(" ", "_"),
    }
    return params


# noinspection t
@task_filter_rule
def task_filter_rule(val: dict) -> list[dict] | None:

    if isinstance(val, dict) and "pipelines" in val:
        components = []
        for pipeline in val["pipelines"]:
            components.extend(pipeline.get("components", []))
        return components if components else None
    return None


@task_rule(priority=2)
def python_task_rule(val: dict) -> OrbiterPythonOperator | None:

    if (
        "python3"
        in val.get("data", {})
        .get("properties", {})
        .get("$componentMetadata", {})
        .get("technicalType", "")
        .lower()
    ):
        code = (
            val.get("data", {})
            .get("properties", {})
            .get("configuration", {})
            .get("pythonCode", "")
        )
        name = (
            val.get("data", {})
            .get("properties", {})
            .get("$componentMetadata", {})
            .get("name", "")
            .lower()
            .replace(" ", "_")
        )
        if code and name:
            indented_code = "\n    ".join([""] + code.splitlines())
            function_content = f"""def {name}(**context):
            {indented_code}
            """
            return OrbiterPythonOperator(
                orbiter_includes={
                    OrbiterInclude(
                        filepath=f"include/{name}.py",
                        contents=function_content,
                    ),
                },
                imports=[
                    OrbiterRequirement(module=f"include.{name}", names=[f"{name}"]),
                    OrbiterRequirement(
                        module="airflow.operators.python", names=["PythonOperator"]
                    ),
                ],
                python_callable=f"{name}",
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
def simple_task_dependencies(
    val: OrbiterDAG,
) -> list[OrbiterTaskDependency] | None:
    """Extract task dependencies from Talend pipeline steps."""
    dependencies = []
    pipeline = val.orbiter_kwargs.get("val", {}).get("pipelines", [{}])[0]

    component_to_task_name = {}
    for component in pipeline.get("components", []):
        name = (
            component.get("data", {})
            .get("properties", {})
            .get("$componentMetadata", {})
            .get("name")
        )
        if name:
            component_to_task_name[component["id"]] = name.replace(" ", "_").lower()

    port_to_component = {}
    for port in pipeline.get("ports", []):
        port_type = (
            port.get("graphicalAttributes", {}).get("properties", {}).get("type")
        )
        port_to_component[port["id"]] = (port["nodeId"], port_type)

    for step in pipeline.get("steps", []):
        source_port = step["sourceId"]
        target_port = step["targetId"]

        if source_port in port_to_component and target_port in port_to_component:
            source_component_id, source_type = port_to_component[source_port]
            target_component_id, target_type = port_to_component[target_port]

            if source_type != "OUTGOING" or target_type != "INCOMING":
                continue

            source_task_name = component_to_task_name.get(source_component_id)
            target_task_name = component_to_task_name.get(target_component_id)

            if source_task_name and target_task_name:
                dependencies.append(
                    OrbiterTaskDependency(
                        task_id=source_task_name, downstream=target_task_name
                    )
                )

    return dependencies if dependencies else None


translation_ruleset: TranslationRuleset = TranslationRuleset(
    file_type={FileTypeJSON},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[dag_filter_rule]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[task_filter_rule]),
    task_ruleset=TaskRuleset(ruleset=[python_task_rule, _cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[simple_task_dependencies]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)

if __name__ == "__main__":
    import doctest

    doctest.testmod(
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL
    )
