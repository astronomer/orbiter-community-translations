"""Demo Jenkins pipeline translation to Airflow DAGs

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

Jenkins provides a nifty API in the [pipeline-model-definition-plugin](https://github.com/jenkinsci/pipeline-model-definition-plugin/blob/master/EXTENDING.md#conversion-to-json-representation-from-jenkinsfile).
This API can convert a Jenkins Pipeline script (Groovy) to a JSON schema. Users can leverage this API to convert Jenkins Pipeline to a JSON structure.

Sample cURL:
```
curl --location '<jenkins-base-url>/pipeline-model-converter/toJson' \
    --header 'Content-Type: application/x-www-form-urlencoded' \
    --header 'Cookie: <authentication-cookie(s)>' \
    --data-urlencode 'jenkinsfile=<jenkinsfile>'
```

```pycon
>>> dags = translation_ruleset.test(input_value='''
... {
...     "status": "ok",
...     "data": {
...         "result": "success",
...         "json": {
...             "pipeline": {
...                 "stages": [
...                     {
...                         "name": "Build",
...                         "branches": [
...                             {
...                                 "name": "default",
...                                 "steps": [
...                                     {
...                                         "name": "echo",
...                                         "arguments": [
...                                             {
...                                                 "key": "message",
...                                                 "value": {
...                                                     "isLiteral": true,
...                                                     "value": "Building.."
...                                                 }
...                                             }
...                                         ]
...                                     },
...                                     {
...                                         "name": "echo",
...                                         "arguments": [
...                                             {
...                                                 "key": "message",
...                                                 "value": {
...                                                     "isLiteral": false,
...                                                     "value": "${PATH}"
...                                                 }
...                                             }
...                                         ]
...                                     }
...                                 ]
...                             }
...                         ]
...                     },
...                     {
...                         "name": "Test",
...                         "branches": [
...                             {
...                                 "name": "default",
...                                 "steps": [
...                                     {
...                                         "name": "echo",
...                                         "arguments": [
...                                             {
...                                                 "key": "message",
...                                                 "value": {
...                                                     "isLiteral": true,
...                                                     "value": "Testing.."
...                                                 }
...                                             }
...                                         ]
...                                     }
...                                 ]
...                             }
...                         ]
...                     },
...                     {
...                         "name": "Deploy",
...                         "branches": [
...                             {
...                                 "name": "default",
...                                 "steps": [
...                                     {
...                                         "name": "echo",
...                                         "arguments": [
...                                             {
...                                                 "key": "message",
...                                                 "value": {
...                                                     "isLiteral": true,
...                                                     "value": "Deploying...."
...                                                 }
...                                             }
...                                         ]
...                                     }
...                                 ]
...                             }
...                         ]
...                     }
...                 ],
...                 "agent": {
...                     "type": "any"
...                 }
...             }
...         }
...     }
... }
... ''').dags.values()
>>> list(dags)[0]
... # doctest: +ELLIPSIS
from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import DateTime, Timezone
with DAG(dag_id=..., schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):
    build_task = BashOperator(task_id='build', bash_command="echo 'Building..'; echo ${PATH}")
    test_task = BashOperator(task_id='test', bash_command="echo 'Testing..'")
    deploy_task = BashOperator(task_id='deploy', bash_command="echo 'Deploying....'")
    build_task >> test_task
    test_task >> deploy_task

```
"""  # noqa: E501
from __future__ import annotations

import inflection
import jq
import json

from orbiter.file_types import FileTypeJSON
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.bash import OrbiterBashOperator
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
    """
    Filter input to the top level element `pipeline` and further,
    to a list of dictionaries that can be processed by the `@dag_rules`
    ```pycon
    >>> basic_dag_filter(val={"status": "ok",
    ...     "data": {
    ...         "result": "success",
    ...         "json": {
    ...             "pipeline": {
    ...                 "stages": [
    ...                     {
    ...                         "name": "Build",
    ...                         "branches": [
    ...                             {
    ...                                 "name": "default",
    ...                                 "steps": [
    ...                                     {
    ...                                         "name": "echo",
    ...                                         "arguments": [
    ...                                             {"key": "message", "value": {"isLiteral": True, "value": "Building.."}}
    ...                                         ],
    ...                                     },
    ...                                     {
    ...                                         "name": "echo",
    ...                                         "arguments": [
    ...                                             {"key": "message", "value": {"isLiteral": False, "value": "${PATH}"}}
    ...                                         ],
    ...                                     },
    ...                                 ],
    ...                             }
    ...                         ],
    ...                     },
    ...                     {
    ...                         "name": "Test",
    ...                         "branches": [
    ...                             {
    ...                                 "name": "default",
    ...                                 "steps": [
    ...                                     {
    ...                                         "name": "echo",
    ...                                         "arguments": [
    ...                                             {"key": "message", "value": {"isLiteral": True, "value": "Testing.."}}
    ...                                         ],
    ...                                     }
    ...                                 ],
    ...                             }
    ...                         ],
    ...                     },
    ...                     {
    ...                         "name": "Deploy",
    ...                         "branches": [
    ...                             {
    ...                                 "name": "default",
    ...                                 "steps": [
    ...                                     {
    ...                                         "name": "echo",
    ...                                         "arguments": [
    ...                                             {
    ...                                                 "key": "message",
    ...                                                 "value": {"isLiteral": True, "value": "Deploying...."},
    ...                                             }
    ...                                         ],
    ...                                     }
    ...                                 ],
    ...                             }
    ...                         ],
    ...                     },
    ...                 ],
    ...                 "agent": {"type": "any"},
    ...             }
    ...         },
    ...     },
    ... })
    ... # doctest: +ELLIPSIS
    [{'stages': [{'name': 'Build', 'branches': [{'name': 'default', 'steps': [{'name': 'echo', 'arguments': [{'key': 'message', 'value': {'isLiteral': True, 'value': 'Building..'}}]}, {'name': 'echo', 'arguments': [{'key': 'message', 'value': {'isLiteral': False, 'value': '${PATH}'}}]}]}]}, {'name': 'Test', 'branches': [{'name': 'default', 'steps': [{'name': 'echo', 'arguments': [{'key': 'message', 'value': {'isLiteral': True, 'value': 'Testing..'}}]}]}]}, {'name': 'Deploy', 'branches': [{'name': 'default', 'steps': [{'name': 'echo', 'arguments': [{'key': 'message', 'value': {'isLiteral': True, 'value': 'Deploying....'}}]}]}]}], 'agent': {'type': 'any'}}]

    ```
    """  # noqa: E501
    val = json.loads(json.dumps(val, default=str))  # pre-serialize values, for JQ

    try:
        return jq.compile(""".data.json.pipeline""").input_value(val).all()
    except StopIteration:
        pass
    return None


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Translate input into an `OrbiterDAG`
    ```pycon
    >>> from pathlib import Path
    >>> basic_dag_rule(val={'pipeline': {'stages': [{'name': 'Build',
    ...  'branches': [{'name': 'default',
    ...    'steps': [{'name': 'echo',
    ...      'arguments': [{'key': 'message',
    ...        'value': {'isLiteral': True, 'value': 'Building..'}}]},
    ...     {'name': 'echo',
    ...      'arguments': [{'key': 'message',
    ...        'value': {'isLiteral': False, 'value': '${PATH}'}}]}]}]},
    ... {'name': 'Test',
    ...  'branches': [{'name': 'default',
    ...    'steps': [{'name': 'echo',
    ...      'arguments': [{'key': 'message',
    ...        'value': {'isLiteral': True, 'value': 'Testing..'}}]}]}]},
    ... {'name': 'Deploy',
    ...  'branches': [{'name': 'default',
    ...    'steps': [{'name': 'echo',
    ...      'arguments': [{'key': 'message',
    ...        'value': {'isLiteral': True, 'value': 'Deploying....'}}]}]}]}],
    ... 'agent': {'type': 'any'}},
    ... '__file': Path("demo1.json")})
    ... # doctest: +ELLIPSIS
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='demo1', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):

    ```
    """  # noqa: E501
    dag_id = inflection.underscore(val["__file"].stem)
    return OrbiterDAG(
        dag_id=dag_id,
        file_path=f"{dag_id}.py",
        doc_md="**Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
        "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
        "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
    )


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """
    The task declaration is in 'stages' inside 'pipeline'

    ```pycon
    >>> basic_task_filter(val={
    ...     "stages": [
    ...         {
    ...             "name": "Build",
    ...             "branches": [
    ...                 {
    ...                     "name": "default",
    ...                     "steps": [
    ...                         {
    ...                             "name": "echo",
    ...                             "arguments": [{"key": "message", "value": {"isLiteral": True, "value": "Building.."}}],
    ...                         },
    ...                         {
    ...                             "name": "echo",
    ...                             "arguments": [{"key": "message", "value": {"isLiteral": False, "value": "${PATH}"}}],
    ...                         },
    ...                     ],
    ...                 }
    ...             ],
    ...         },
    ...         {
    ...             "name": "Test",
    ...             "branches": [
    ...                 {
    ...                     "name": "default",
    ...                     "steps": [
    ...                         {
    ...                             "name": "echo",
    ...                             "arguments": [{"key": "message", "value": {"isLiteral": True, "value": "Testing.."}}],
    ...                         }
    ...                     ],
    ...                 }
    ...             ],
    ...         },
    ...         {
    ...             "name": "Deploy",
    ...             "branches": [
    ...                 {
    ...                     "name": "default",
    ...                     "steps": [
    ...                         {
    ...                             "name": "echo",
    ...                             "arguments": [
    ...                                 {"key": "message", "value": {"isLiteral": True, "value": "Deploying...."}}
    ...                             ],
    ...                         }
    ...                     ],
    ...                 }
    ...             ],
    ...         },
    ...     ],
    ...     "agent": {"type": "any"},
    ... })
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    [{'name': 'Build',
    'branches': [{'name': 'default',
    'steps': [{'name': 'echo',
      'arguments': [{'key': 'message',
        'value': {'isLiteral': True, 'value': 'Building..'}}]},
     {'name': 'echo',
      'arguments': [{'key': 'message',
        'value': {'isLiteral': False, 'value': '${PATH}'}}]}]}]},
    {'name': 'Test',
    'branches': [{'name': 'default',
    'steps': [{'name': 'echo',
      'arguments': [{'key': 'message',
        'value': {'isLiteral': True, 'value': 'Testing..'}}]}]}]},
    {'name': 'Deploy',
    'branches': [{'name': 'default',
    'steps': [{'name': 'echo',
      'arguments': [{'key': 'message',
        'value': {'isLiteral': True, 'value': 'Deploying....'}}]}]}]}]

    ```
    """  # noqa: E501

    return val.get("stages", [])


@task_rule(priority=2)
def basic_task_rule(val: dict) -> OrbiterBashOperator | None:
    """Translate input into a OrbiterOperator (e.g. `OrbiterBashOperator`)
    ```pycon
    >>> basic_task_rule(val={'name': 'Build',
    ... 'branches': [{'name': 'default',
    ... 'steps': [{'name': 'echo',
    ...   'arguments': [{'key': 'message',
    ...     'value': {'isLiteral': True, 'value': 'Building..'}}]},
    ...  {'name': 'echo',
    ...   'arguments': [{'key': 'message',
    ...     'value': {'isLiteral': False, 'value': '${PATH}'}}]}]}]})
    ... # doctest: +ELLIPSIS
    build_task = BashOperator(task_id='build', bash_command="echo 'Building..'; echo ${PATH}")

    ```
    """
    val = json.loads(json.dumps(val, default=str))  # pre-serialize values, for JQ
    task_id = inflection.underscore(val["name"])
    commands = []
    steps = None

    # Considering only the first branch for each stage in pipeline for now
    # TODO: Add support for multiple branches
    try:
        steps = jq.compile(""".branches[] | .steps""").input_value(val).first()
    except StopIteration:
        pass

    for step in steps:
        command = []
        command.append(step["name"])
        for argument in step["arguments"]:
            argument_defn = argument["value"]
            command.append(
                f"""'{argument_defn["value"]}'"""
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
    """
    Map all tasks to their predecessors to get the dependency graph

    ```pycon
    >>> basic_task_dependency_rule(val=OrbiterDAG(
    ...     dag_id=".", file_path=".",
    ...     tasks={
    ...            'build': OrbiterBashOperator(task_id='build', bash_command="echo 'Building..'; echo ${PATH}"),
    ...            'test': OrbiterBashOperator(task_id='test', bash_command="echo 'Testing..'"),
    ...            'deploy': OrbiterBashOperator(task_id='deploy', bash_command="echo 'Deploying....'")
    ...        },
    ...     orbiter_kwargs={'val': {'stages': [{'name': 'Build', 'branches': [{'name': 'default', 'steps': [{'name': 'echo', 'arguments': [{'key': 'message', 'value': {'isLiteral': True, 'value': 'Building..'}}]}, {'name': 'echo', 'arguments': [{'key': 'message', 'value': {'isLiteral': False, 'value': '${PATH}'}}]}]}]}, {'name': 'Test', 'branches': [{'name': 'default', 'steps': [{'name': 'echo', 'arguments': [{'key': 'message', 'value': {'isLiteral': True, 'value': 'Testing..'}}]}]}]}, {'name': 'Deploy', 'branches': [{'name': 'default', 'steps': [{'name': 'echo', 'arguments': [{'key': 'message', 'value': {'isLiteral': True, 'value': 'Deploying....'}}]}]}]}], 'agent': {'type': 'any'}}}
    ... ))
    ... # doctest: +ELLIPSIS
    [build >> test, test >> deploy]

    ```
    """  # noqa: E501
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
