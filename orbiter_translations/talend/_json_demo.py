"""
## Demo `translation_ruleset` Example

```pycon
>>> translation_ruleset.test(input_value={'userFlow': {
...     'enabled': True, 'type': 'batch', 'label': 'Pipeline 2', 'description': 'This is a newly created pipeline',
...     'pipelines': [{'components': [
...       {
...         'id': '18b971cb-5360-43dc-b036-0d24ad9d05f7',
...         'data': {
...           'properties': {
...             'configuration': {'pythonCode': 'input["Hello"] = "World"'},
...             '$componentMetadata': {'name': 'Demo Python Task', 'type': 'Python 3'}
...           }
...         }
...       },
...       {
...         'id': '1ce2ee7d-1b55-4fb9-823e-8c110d4099db',
...         'data': {'properties': {'$componentMetadata': {'name': 'Foo', 'type': 'Data generator input'}}}
...       },
...       {
...         'id': '0b03e3f7-ddbc-4f93-9993-b9a919fa80ce',
...         'data': {'properties': {'$componentMetadata': {'name': 'Bar', 'type': 'Test output'}}}
...       }
...   ], 'ports': [
...     {'id': 'bd01d665-07e4-4125-8734-4188a3046e43', 'nodeId': '1ce2ee7d-1b55-4fb9-823e-8c110d4099db'},
...     {'id': '9371efc7-3ab1-44b3-9d64-415fc087e3f4', 'nodeId': '0b03e3f7-ddbc-4f93-9993-b9a919fa80ce'},
...     {'id': 'e3735fa6-0bf9-4ffd-8ee4-aa404d34f4d3', 'nodeId': '18b971cb-5360-43dc-b036-0d24ad9d05f7'},
...     {'id': '520d299a-5fcd-4998-87a9-b3e0ecf09c88', 'nodeId': '18b971cb-5360-43dc-b036-0d24ad9d05f7'}
...   ], 'steps': [
...     {'sourceId': '520d299a-5fcd-4998-87a9-b3e0ecf09c88', 'targetId': '9371efc7-3ab1-44b3-9d64-415fc087e3f4'},
...     {'sourceId': 'bd01d665-07e4-4125-8734-4188a3046e43', 'targetId': 'e3735fa6-0bf9-4ffd-8ee4-aa404d34f4d3'}
...   ]}],
... }}).dags
... # doctest: +ELLIPSIS
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='demo.extract_sample_currency_data', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):
    extract_sample_currency_data_task = EmptyOperator(task_id='extract_sample_currency_data', doc_md='Input did not translate: `{"@id": "1", "@name": "Extract Sample Currency Data", "connections": [{"connection": [{"@id": "6", "@name": "FlatFileConnection", "@connectionManagerID": "{EA76C836-FF8B-4E34-B273-81D4F67FCB3D}"}]}], "outputs": [{"output": [{"@id": "2", "@name": "Flat File Source Output"}, {"@id": "3", "@name": "Flat File Source Error Output"}]}]}`')
    lookup_currency_key_task = SQLExecuteQueryOperator(task_id='lookup_currency_key', conn_id='mssql_default', sql="select * from (select * from [dbo].[DimCurrency]) as refTable where [refTable].[CurrencyAlternateKey] = 'ARS' OR [refTable].[CurrencyAlternateKey] = 'VEB'")
    lookup_date_key_task = SQLExecuteQueryOperator(task_id='lookup_date_key', conn_id='mssql_default', sql='select * from [dbo].[DimTime]')
    sample_ole__db_destination_task = EmptyOperator(task_id='sample_ole__db_destination', doc_md='Input did not translate: `{"@id": "100", "@name": "Sample OLE  DB Destination", "@componentClassID": "{5A0B62E8-D91D-49F5-94A5-7BE58DE508F0}", "inputs": [{"input": [{"@id": "113", "@name": "OLE DB Destination Input"}]}], "outputs": null}`')
    extract_sample_currency_data_task >> lookup_currency_key_task
    lookup_currency_key_task >> lookup_date_key_task
    lookup_date_key_task >> sample_ole__db_destination_task

```
"""
from __future__ import annotations

import inflection
from orbiter import clean_value
from orbiter.file_types import FileTypeJSON
from orbiter.objects import OrbiterRequirement
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.task import OrbiterOperator, OrbiterTask
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
    """Filter input down to a list of dictionaries that can be processed by the `@dag_rules`"""
    return [val.get("userFlow")] if "userFlow" in val else None


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Translate input into an `OrbiterDAG`"""
    if dag_id := inflection.underscore(val.get("label", "")):
        dag_kwargs = dict()
        if description := val.get("description"):
            dag_kwargs["description"] = description
        return OrbiterDAG(dag_id=dag_id, file_path=f"{dag_id}.py", **dag_kwargs)
    else:
        return None


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@task_rules`"""
    if pipelines := val.get("pipelines"):
        return [
            component
            for pipeline in pipelines
            for component in pipeline.get("components", [])
        ]
    else:
        return None


def task_common_args(val) -> dict:
    metadata = val.get("data", {}).get("properties", {}).get("$componentMetadata", {})
    return {"task_id": metadata["name"]} if metadata else {}


@task_rule(priority=2)
def python_transform_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate input into an Operator (e.g. `OrbiterBashOperator`). will be applied first, with a higher priority"""
    if (
        val.get("data", {})
        .get("properties", {})
        .get("$componentMetadata", {})
        .get("type")
        == "Python 3"
    ):
        # noinspection PyUnusedLocal
        python_code = (  # noqa: F841
            val.get("data", {})
            .get("properties", {})
            .get("configuration", {})
            .get("pythonCode")
        )
        # Needs to be a @dataframe from astro_sdk - df.apply python code
        # TODO
        return OrbiterTask(
            task_id=task_common_args(val)["task_id"],
            imports=[
                OrbiterRequirement(
                    names=["dataframe"], module="astro_sdk", package="astro-sdk-python"
                )
            ],
        )
    return None


@task_rule(priority=1)
def _cannot_map_rule(val):
    """Add task_ids on top of common 'cannot_map_rule'"""
    task = cannot_map_rule(val)
    task.task_id = clean_value(task_common_args(val)["task_id"])
    return task


@task_dependency_rule
def basic_task_dependency_rule(val: OrbiterDAG) -> list | None:
    """Translate input into a list of task dependencies"""
    for task_dependency in val.orbiter_kwargs["task_dependencies"]:
        pass
    return []


translation_ruleset = TranslationRuleset(
    file_type={FileTypeJSON},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[python_transform_rule, cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[basic_task_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
