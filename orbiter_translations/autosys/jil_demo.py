r"""Demonstration of translating AutoSys JIL files into Airflow DAGs.

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

```pycon
>>> res = translation_ruleset.test('''
... insert_job: foo.job
... job_type: CMD
... command: "C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" "arg3"
... machine: bar
... owner: foo@bar.com
... description: "Foo Job"
... insert_job: bar
... job_type: CMD
... command: echo "hello, world!"
... machine: bar
... '''); res.dags["foo_job"]; print('----'); res.dags["bar"]
... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id='foo_job', default_args={'owner': 'foo@bar.com'}, doc_md=...):
    foo_job_task = BashOperator(task_id='foo_job', bash_command='"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" "arg3"', doc='Foo Job')
----
from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id='bar', doc_md=...):
    bar_task = BashOperator(task_id='bar', bash_command='echo "hello, world!"')

```
"""
from __future__ import annotations

from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.bash import OrbiterBashOperator
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_filter_rule,
    task_rule,
    create_cannot_map_rule_with_task_id_fn,
)
from orbiter.rules.rulesets import (
    TranslationRuleset,
    DAGFilterRuleset,
    DAGRuleset,
    TaskFilterRuleset,
    TaskRuleset,
    PostProcessingRuleset,
    translate,
    TaskDependencyRuleset,
)
from orbiter_parsers.file_types.jil import FileTypeJIL


@dag_filter_rule
def demo_dag_filter_rule(val: dict) -> list[dict] | None:
    """Pass parsed JIL through filter."""
    return [val]

_demo_dag_common_args_params_doc = {
    "owner": "DAG.default_args.owner",
    "insert_job": "DAG.dag_id"
}
def demo_dag_common_args(val: dict) -> dict:
    """Generates common DAG arguments."""
    dag_id = val["insert_job"].replace(".", "_").lower()
    params = {"dag_id": dag_id}
    default_args = {}
    if owners := val.get("owner"):
        default_args["owner"] = owners
    if default_args:
        params["default_args"] = default_args
    return params

@dag_rule(params_doc=_demo_dag_common_args_params_doc)
def demo_dag_rule(val: dict) -> OrbiterDAG | None:
    r"""Map AutoSysWorkflow into a DAG

    ```pycon
    >>> demo_dag_rule({
    ...   '__file': 'workflow.jil',
    ...   'command': '"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" "arg3"',
    ...   'group': 'group_name',
    ...   'insert_job': 'foo.job',
    ...   'job_type': 'CMD',
    ...   'machine': 'machine_id',
    ...   'owner': 'foo@bar.net'
    ... }) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    from airflow import DAG...
    with DAG(dag_id='foo_job', default_args={'owner': 'foo@bar.net'}, doc_md=...):...

    ```
    """
    if "insert_job" in val:
        common_args = demo_dag_common_args(val)
        return OrbiterDAG(
            file_path=f"{common_args['dag_id']}.py",
            doc_md="**Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
                   "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
                   "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
            **common_args,
        )
    return None


@task_filter_rule
def demo_task_filter(val: dict) -> list[dict] | None:
    r"""Pass parsed JIL through filter."""
    return [val]


_demo_task_common_args_params_doc = {
    "insert_job": "Operator.task_id",
    "description": "Operator.doc"
}
def demo_task_common_args(val: dict) -> dict:
    """Generates common task arguments.

    ```pycon
    >>> demo_task_common_args({'insert_job': 'foo.job', 'description': 'Foo Job'})
    {'task_id': 'foo_job', 'doc': 'Foo Job'}

    ```
    """
    task_id = val.get("insert_job", "UNKNOWN").replace(".", "_").lower()
    params = {"task_id": task_id}
    if description := val.get("description"):
        params["doc"] = description
    return params

@task_rule(params_doc={"command": "BashOperator.bash_command"} | _demo_task_common_args_params_doc)
def demo_bash_command_job_rule(val) -> OrbiterBashOperator | None:
    r"""Maps a workflow of job_type 'CMD' into a BashOperator

    ```pycon
    >>> demo_bash_command_job_rule({
    ...     'insert_job': 'foo_bar',
    ...     'job_type': 'CMD',
    ...     'command': '"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" "arg3"',
    ... })
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    foo_bar_task = BashOperator(task_id='foo_bar',
        bash_command='"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" "arg3"')

    ```
    """
    if val.get("job_type") == "CMD":
        return OrbiterBashOperator(
            bash_command=val["command"],
            **demo_task_common_args(val),
        )
    return None


translation_ruleset = TranslationRuleset(
    file_type={FileTypeJIL},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[demo_dag_filter_rule]),
    dag_ruleset=DAGRuleset(ruleset=[demo_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[demo_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[demo_bash_command_job_rule, create_cannot_map_rule_with_task_id_fn(lambda val: demo_task_common_args(val)['task_id'])]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
    translate_fn=translate,
)
