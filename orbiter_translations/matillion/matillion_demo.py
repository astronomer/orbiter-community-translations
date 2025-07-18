from __future__ import annotations
from pathlib import Path
import re
import yaml

from orbiter.file_types import FileTypeYAML
from orbiter.objects import OrbiterRequirement
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.include import OrbiterInclude
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.operators.python import OrbiterPythonOperator
from orbiter.objects.task import OrbiterTaskDependency
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_dependency_rule,
    task_filter_rule,
    task_rule,
    cannot_map_rule
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
    print(1)
    if val.get("type") == "orchestration" and "pipeline" in val:
        print(2)
        return [val["pipeline"]]
    return None

### DAG Rule
@dag_rule
def matillion_dag_rule(val: dict) -> OrbiterDAG:
    dag_id = _slugify(val.get("name", "matillion_pipeline"))
    return OrbiterDAG(
        dag_id=dag_id,
        file_path=Path(f"{dag_id}.py"),
        doc_md="Generated from Matillion YAML export"
    )

### Task Filter Rule
@task_filter_rule
def matillion_task_filter(val: dict) -> list[dict] | None:
    return list(val.get("components", {}).values())

### Task Rules

@task_rule(priority=10)
def start_task_rule(val: dict) -> OrbiterEmptyOperator | None:
    if val.get("type") != "start":
        print('Pipeline started executing')
        return None
    return OrbiterEmptyOperator(
        task_id=_slugify(val["parameters"]["componentName"])
    )


@task_rule(priority=10)
def python_pushdown_rule(val: dict) -> OrbiterPythonOperator | None:
    if val.get("type") != "python-pushdown":
        return None

    component_name = val["parameters"]["componentName"]
    task_id = _slugify(component_name)
    python_code = val["parameters"].get("pythonScript", "# no script")

    # indent code properly:
    indented_code = "\n    ".join(python_code.splitlines())
    function_name = task_id

    python_fn = f"""def {function_name}(**context):\n    {indented_code}\n"""

    return OrbiterPythonOperator(
        task_id=task_id,
        orbiter_includes={
            OrbiterInclude(
                filepath=f"include/{task_id}.py",
                contents=python_fn,
            )
        },
        imports=[
            OrbiterRequirement(module=f"include.{task_id}", names=[function_name]),
            OrbiterRequirement(module="airflow.operators.python", names=["PythonOperator"]),
        ],
        python_callable=f"{function_name}"
    )

@task_rule(priority=1)
def fallback_task_rule(val: dict):
    task = cannot_map_rule(val)
    task.task_id = _slugify(val["parameters"]["componentName"])
    return task

### Dependency Rule
@task_dependency_rule
def matillion_dependency_rule(val: OrbiterDAG) -> list[OrbiterTaskDependency]:
    """
    Reads 'unconditional' transitions and wires downstream dependencies.
    """
    name_map = {t.task_id.lower(): t for t in val.tasks.values()}
    dependencies = []

    pipeline = val.orbiter_kwargs["val"]
    components = pipeline.get("components", {}).values()

    for component in components:
        src = _slugify(component["parameters"]["componentName"])
        for target in component.get("transitions", {}).get("unconditional", []):
            tgt = _slugify(target)
            if src in name_map and tgt in name_map:
                dependencies.append(
                    OrbiterTaskDependency(task_id=src, downstream=tgt)
                )
    return dependencies


### Translation Ruleset
translation_ruleset = TranslationRuleset(
    file_type={FileTypeYAML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[matillion_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[matillion_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[matillion_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[start_task_rule, python_pushdown_rule, fallback_task_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[matillion_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
