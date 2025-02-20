"""Demo translation ruleset for Argo YAML files to Airflow DAGs

```pycon
>>> translation_ruleset.test('''
... apiVersion: argoproj.io/v1alpha1
... kind: Workflow
... metadata:
...   generateName: hello-world-
... spec:
...   entrypoint: whalesay
...   templates:
...   - name: whalesay
...     container:
...      image: docker/whalesay:latest
...      command: [cowsay]
...      args: ["hello world"]
... ''').dags['hello_world']
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='hello_world', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    whalesay_task = KubernetesPodOperator(task_id='whalesay', kubernetes_conn_id='KUBERNETES', image='docker/whalesay:latest', cmds=['cowsay'], arguments=['hello world'])

```
"""
from __future__ import annotations

from orbiter.file_types import FileTypeYAML
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.kubernetes_pod import OrbiterKubernetesPodOperator
from orbiter.objects.task import OrbiterOperator
from orbiter.objects.task_group import OrbiterTaskGroup
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_filter_rule,
    task_rule,
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
    """Allow input of kind `Workflow` or `CronWorkflow`"""
    if val.get('kind') in ('Workflow', 'CronWorkflow'):
        return [val]



@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Translate input into a DAG, using the `name` or `generateName` as the `dag_id`

    ```pycon
    >>> basic_dag_rule({'kind': 'Workflow', 'metadata': {'name': 'hello-world'}, 'spec': {'schedule': '0 0 * * *'}})
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='hello_world', schedule='0 0 * * *', start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):

    ```
    """
    if val.get('kind') in ('Workflow', 'CronWorkflow'):
        metadata = val.get('metadata', {})
        dag_id = metadata.get('name', metadata.get('generateName', '')[:-1])
        args = {}
        if schedule := val.get('spec', {}).get('schedule'):
            args['schedule'] = schedule
        return OrbiterDAG(dag_id=dag_id, file_path=f"{dag_id}.py", **args)


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Reshape and filter input to `template` tasks"""
    # noinspection PyUnboundLocalVariable
    if (
        val.get('kind') in ('Workflow', 'CronWorkflow')
        and (spec := val.get('spec', {}))
        and (templates := spec.get('templates'))
    ):
        return templates


@task_rule(priority=2)
def basic_task_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate a pod definition into a KubernetesPodOperator"""
    if (name := val.get('name')) and (container := val.get('container', {})):
        return OrbiterKubernetesPodOperator(
            task_id=name,
            image=container.get("image") or None,
            cmds=container.get("command") or None,
            arguments=container.get("args") or None,
        )

translation_ruleset = TranslationRuleset(
    file_type={FileTypeYAML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[basic_task_rule, cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
