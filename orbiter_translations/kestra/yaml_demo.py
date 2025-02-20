"""Demonstration of translating Kestra YAML files into Airflow DAGs.

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

```pycon
>>> translation_ruleset.test('''
...   id: getting_started
...   namespace: company.team
...   tasks:
...     - id: hello_world
...       type: io.kestra.plugin.core.log.Log
...       message: Hello World!
... ''').dags['company.team.getting_started']
from airflow import DAG
from airflow.decorators import task
from pendulum import DateTime, Timezone
with DAG(dag_id='company.team.getting_started', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
<BLANKLINE>
    @task()
    def hello_world():
        print('Hello World!')

```
"""
from __future__ import annotations

import textwrap

from orbiter.file_types import FileTypeYAML
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.operators.python import OrbiterDecoratedPythonOperator
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
)


@dag_filter_rule
def basic_dag_filter(val: dict) -> list | None:
    """Check that top-level dictionary has 'id' and 'tasks' keys

    ```pycon
    >>> basic_dag_filter({'id': 'foo', 'tasks': [{'id': 'foo'}, {'id': 'bar'}]})  # doctest: +ELLIPSIS
    [...]

    ```
    """
    if 'id' in val and 'tasks' in val:
        return [val]


def common_dag_args(val) -> dict:
    """Extract common DAG arguments from the input dictionary

    - Extract a `cron` schedule from a `io.kestra.plugin.core.trigger.Schedule` trigger  (only last, if multiple)
    """
    args = {}
    if triggers := val.get('trigger'):
        for trigger in triggers:
            if cron := trigger.get('cron') and trigger.get('type') == 'io.kestra.plugin.core.trigger.Schedule':
                args['schedule'] = cron
    if labels := val.get('labels'):
        if labels.get('owner'):
            args['owner'] = labels.pop('owner')
        args['tags'] = [f"{k}={v}" for k, v in labels.items()]
    if description := val.get('description'):
        args['doc_md'] = description
    return args


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Utilize `id` and `namespace` to create a DAG ID and file path - using namespace for directory structure

    ```pycon
    >>> basic_dag_rule({'id': 'foo', 'namespace': 'baz.bar'})
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='baz.bar.foo', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):

    ```
    """
    id = val.get('id')
    namespace = val.get('namespace')
    if id and namespace:
        dag_id = f"{namespace}.{id}"
        return OrbiterDAG(
            dag_id=dag_id,
            file_path=dag_id.replace('.', '/') + ".py",
            **common_dag_args(val)
        )


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Use the `tasks` key for filtering

    ```pycon
    >>> basic_task_filter({'id': 'foo', 'tasks': [{'id': 'foo'}, {'id': 'bar'}]})
    [{'id': 'foo'}, {'id': 'bar'}]

    ```
    """
    return val.get('tasks', []) or None


def task_common_args(val) -> dict:
    args = {}
    if task_id := val.get('id'):
        args['task_id'] = task_id
    if description := val.get('description'):
        args['doc_md'] = description
    return args


@task_rule(priority=2)
def log_task_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate a `io.kestra.plugin.core.log.Log` task into a `@task` decorated function, which prints the message

    ```pycon
    >>> log_task_rule({'id': 'foo', 'type': 'io.kestra.plugin.core.log.Log', 'message': 'Hello World!'})
    @task()
    def foo():
        print('Hello World!')

    ```
    """
    if (
        (message := val.get('message'))
        and val.get('type', '') == 'io.kestra.plugin.core.log.Log'
    ):
        common_args = task_common_args(val)
        task_id = common_args['task_id']
        return OrbiterDecoratedPythonOperator(
            python_callable=f"def {task_id}():\n    print('''{message}''')",
            **common_args
        )

@task_rule
def python_task(val: dict) -> OrbiterDecoratedPythonOperator | None:
    """
    ```pycon
    >>> python_task({'id': 'foo', 'type': 'io.kestra.plugin.core.python.Python', 'script': 'print("Hello World!")'})
    @task()
    def foo():
        print('Hello World!')

    ```
    """
    if val.get('type', '') == 'io.kestra.plugin.core.python.Python':
        common_args = task_common_args(val)
        task_id = common_args['task_id']
        return OrbiterDecoratedPythonOperator(
            python_callable=textwrap.dedent(f"""
            def {task_id}():
                {val.get('script')}
            """),
            **common_args
        )

@task_rule
def dag_as_task_rule(val: dict) -> OrbiterTaskGroup | None:
    """Translate a `io.kestra.plugin.core.flow.Flow` task into a `@task` decorated function, which prints the message

    ```pycon
    >>> dag_as_task_rule(val={'id': 'foo', 'type': 'io.kestra.plugin.core.flow.Dag', 'tasks': [
    ...   {'task': {'id': 'foo'}},
    ...   {'task': {'id': 'bar', 'dependsOn': ['foo']}}
    ... ]}) # doctest: +ELLIPSIS
    with TaskGroup(group_id='foo') as foo:
        foo_task = EmptyOperator(task_id='foo', ...)
        bar_task = EmptyOperator(task_id='bar', ...)

    ```
    """
    if val.get('type', '') == 'io.kestra.plugin.core.flow.Dag':
        common_args = task_common_args(val)
        common_args['task_group_id'] = common_args.pop('task_id')
        common_args.pop('doc_md', None)
        return OrbiterTaskGroup(
            tasks={
                _task.task_id: _task
                for task in val.get('tasks', [])
                if (_task := translation_ruleset.task_ruleset.apply(val=task.get('task', {}), take_first=True))
            },
            **common_args
        )

@task_rule(priority=1)
def _cannot_map_rule(val: dict) -> OrbiterEmptyOperator | None:
    cannot_map_task = cannot_map_rule(val)
    if task_id := task_common_args(val).get('task_id'):
        cannot_map_task.task_id = task_id
    return cannot_map_task


@task_dependency_rule
def basic_task_dependency_rule(val: OrbiterDAG) -> list | None:
    """Not Implemented for Demo"""
    return None


translation_ruleset = TranslationRuleset(
    file_type={FileTypeYAML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[log_task_rule, dag_as_task_rule, _cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[basic_task_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
