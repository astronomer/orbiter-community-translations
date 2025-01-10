"""Demo ruleset converting ESP applications to Airflow DAGs

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

```pycon
>>> translation_ruleset.test('''
... APPL PAYROLL
... JCLLIB 'CYBKH01.TEST.JCL'
...
... JOB A
...  RELEASE (B,C)
...  RUN DAILY
... ENDJOB
... JOB B
...  RELEASE (D)
...  RUN DAILY
... ENDJOB
... LINUX_JOB C
...  AGENT LNX_AGNT
...  SCRIPTNAME /export/home/khanna/deduct.sh
...  RUN DAILY
... ENDJOB
... ''').dags['payroll'] # doctest: +ELLIPSIS
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='payroll', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):
    a_task = EmptyOperator(task_id='a')
    b_task = EmptyOperator(task_id='b')
    c_task = SSHOperator(task_id='c', ssh_conn_id='LNX_AGNT', command='/export/home/khanna/deduct.sh')
    a_task >> [b_task, c_task]
    b_task >> d_task

```
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
    """Use `APPL` key for Airflow DAG ID and file path"""
    if "APPL" in val:
        dag_id = val["APPL"].lower()
        return OrbiterDAG(
            dag_id=dag_id,
            file_path=f"{dag_id}.py",
            doc_md="**Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
                   "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
                   "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
        )


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Filter input to dicts with a key containing `*job*` (e.g. `JOB` or `NT_JOB`)"""
    return [
        v
        for v in (v for v in val.values() if isinstance(v, dict))
        if any("job" in _k.lower() for _k in v.keys())
    ] or None


def common_args(val: dict) -> dict:
    """Common argument extractions for all task rules.
    Looks for a key like `*job*` (e.g. `JOB` or `NT_JOB`) and returns the value as the task_id.
    """
    try:
        job_key = next(key for key in val.keys() if "job" in key.lower())
        task_id = val[job_key]
        return {"task_id": task_id}
    except StopIteration:
        return {}


@task_rule(priority=10)
def win_ssh_task_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate input into an `SSHOperator` from `NT_JOB` key"""
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
    """Translate input into an `SSHOperator` from `LINUX_JOB` key"""
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
    """Translate input into an `EmptyOperator` if nothing else has matched, but it still looks right"""
    if val and isinstance(val, dict):
        return OrbiterEmptyOperator(**common_args(val))


@task_dependency_rule
def basic_task_dependency_rule(val: OrbiterDAG) -> list | None:
    """Translate input into a list of task dependencies via the `RELEASE` key"""
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
