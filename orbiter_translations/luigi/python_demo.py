"""Demo Translation for Oozie XML to Airflow DAGs.

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

```pycon
>>> translation_ruleset.test('''
... import luigi
... class HelloWorld(luigi.Task):
...     def output(self):
...         return None
...
...     def run(self):
...         print('Hello World!')
... ''').dags['hello_world']
from airflow import DAG
from airflow.decorators import task
from pendulum import DateTime, Timezone
with DAG(dag_id='hello_world', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
<BLANKLINE>
    @task()
    def run():
        print('Hello World!')

```
"""
from __future__ import annotations

from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.python import OrbiterDecoratedPythonOperator
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

from orbiter.file_types import FileTypePython
from json2ast import json2ast
import ast

@dag_filter_rule
def basic_dag_filter(val: dict) -> list | None:
    """Get class definitions, from the AST

    ```pycon
    >>> basic_dag_filter(val={'_type': 'Module', 'body': [{'_type': 'ClassDef'}]})
    [{'_type': 'ClassDef'}]

    ```
    """
    if body_items := val.get('body'):
        return [item for item in body_items if item.get('_type') == 'ClassDef']


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Use the class name as both the `dag_id` and file name

    ```pycon
    >>> basic_dag_rule(val={'_type': 'ClassDef', 'name': 'HelloWorld'})
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='hello_world', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):

    ```
    """
    if name := val.get('name'):
        return OrbiterDAG(dag_id=val["name"], file_path=f"{name}.py")


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@task_rules`

    ```pycon
    >>> basic_task_filter(val={'_type': 'ClassDef', 'name': 'HelloWorld', 'body': [{'_type': 'FunctionDef', 'name': 'run'}]})
    [{'_type': 'FunctionDef', 'name': 'run'}]

    ```
    """
    if body := val.get('body'):
        return [item for item in body if item.get('_type') == 'FunctionDef' and item.get('name') == 'run']


@task_rule(priority=2)
def decorated_task_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Unwrap AST for `run` method and re-wrap it as a python callable, make an `@task` with it

    ```pycon
    >>> decorated_task_rule(val={'_type': 'FunctionDef', 'args': {'_type': 'arguments', 'args': [{'_type': 'arg','arg': 'self'}], 'defaults': [], 'kw_defaults': [], 'kwarg': None, 'kwonlyargs': [], 'posonlyargs': [], 'vararg': None}, 'body': [{'_type': 'Expr', 'value': {'_type': 'Call', 'args': [{'_type': 'Constant', 'kind': None, 'n': 'Hello World!', 's': 'Hello World!', 'value': 'Hello World!'}], 'func': {'_type': 'Name', 'ctx': {'_type': 'Load'}, 'id': 'print'}, 'keywords': []}}], 'decorator_list': [], 'lineno': 7, 'name': 'run'})
    @task()
    def run():
        print('Hello World!')

    ```
    """
    if name := val.get('name'):
        args = val.get('args', {})
        args['args'] = [arg for arg in args.get('args', []) if arg.get('arg') != 'self']

        py_fn: str = ast.unparse(json2ast(val))
        return OrbiterDecoratedPythonOperator(task_id=name, python_callable=py_fn)


translation_ruleset = TranslationRuleset(
    file_type={FileTypePython},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[decorated_task_rule, cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
