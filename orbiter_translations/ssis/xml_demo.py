"""
Demo SSIS XML to Airflow DAG translation ruleset

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

```pycon
>>> translation_ruleset.test(input_value='''<?xml version="1.0"?>
...     <DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts" DTS:ExecutableType="SSIS.Package.2">
...     <DTS:Executable DTS:ExecutableType="SSIS.Pipeline.2">
...     <DTS:Property DTS:Name="ObjectName">Extract Sample Currency Data</DTS:Property>
...     <DTS:Property DTS:Name="Description">Data Flow Task</DTS:Property>
...     <DTS:ObjectData>
...       <pipeline>
...         <components>
...           <component id="1" name="Extract Sample Currency Data">
...             <connections><connection id="6" name="FlatFileConnection" connectionManagerID="{EA76C836-FF8B-4E34-B273-81D4F67FCB3D}" /></connections>
...             <outputs><output id="2" name="Flat File Source Output" /><output id="3" name="Flat File Source Error Output" /></outputs>
...           </component>
...           <component id="30" name="Lookup CurrencyKey" componentClassID="{27648839-180F-45E6-838D-AFF53DF682D2}">
...             <properties><property name="SqlCommand">select * from (select * from [dbo].[DimCurrency]) as refTable where [refTable].[CurrencyAlternateKey] = 'ARS' OR [refTable].[CurrencyAlternateKey] = 'VEB'</property></properties>
...             <inputs><input id="31" /></inputs>
...             <outputs><output id="32" /><output id="266" name="Lookup No Match Output" /><output id="42" name="Lookup Error Output" /></outputs>
...           </component>
...           <component id="66" name="Lookup DateKey" componentClassID="{27648839-180F-45E6-838D-AFF53DF682D2}">
...             <properties><property id="69" name="SqlCommand">select * from [dbo].[DimTime]</property></properties>
...             <inputs><input id="67" name="Lookup Input" /></inputs>
...             <outputs><output id="68" name="Lookup Match Output" /><output id="270" name="Lookup No Match Output" /><output id="78" name="Lookup Error Output"/></outputs>
...           </component>
...           <component id="100" name="Sample OLE  DB Destination" componentClassID="{5A0B62E8-D91D-49F5-94A5-7BE58DE508F0}">
...             <inputs><input id="113" name="OLE DB Destination Input"/></inputs>
...             <outputs />
...           </component>
...         </components>
...         <paths><path startId="2" endId="31" /><path startId="32" endId="67" /><path startId="68" endId="113" /></paths>
...       </pipeline>
...   </DTS:ObjectData>
...   </DTS:Executable>
...       <DTS:Property DTS:Name="ObjectName">Demo</DTS:Property>
...   </DTS:Executable>
... ''').dags['demo.extract_sample_currency_data']
... # doctest: +ELLIPSIS
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='demo.extract_sample_currency_data', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):
    extract_sample_currency_data_task = EmptyOperator(task_id='extract_sample_currency_data', doc_md=...)
    lookup_currency_key_task = SQLExecuteQueryOperator(task_id='lookup_currency_key', conn_id='mssql_default', sql="select * from (select * from [dbo].[DimCurrency]) as refTable where [refTable].[CurrencyAlternateKey] = 'ARS' OR [refTable].[CurrencyAlternateKey] = 'VEB'")
    lookup_date_key_task = SQLExecuteQueryOperator(task_id='lookup_date_key', conn_id='mssql_default', sql='select * from [dbo].[DimTime]')
    sample_ole__db_destination_task = EmptyOperator(task_id='sample_ole__db_destination', doc_md=...)
    extract_sample_currency_data_task >> lookup_currency_key_task
    lookup_currency_key_task >> lookup_date_key_task
    lookup_date_key_task >> sample_ole__db_destination_task

```
"""  # noqa: E501

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import inflection
import jq

from orbiter import clean_value
from orbiter.file_types import FileTypeXML
from orbiter.objects import conn_id
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.sql import OrbiterSQLExecuteQueryOperator
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
    """
    Filter to top-level <DTS:Executable DTS:ExecutableType="SSIS.Package*"> elements,
    then filter to <DTS:Executable DTS:ExecutableType="SSIS.Pipeline*">

    !!! note

        Input is modified. 'Package' entry (without pipelines) is attached to 'Pipeline' entry with new "@package" key

    ```pycon
    >>> dag_filter_rule(val={'DTS:Executable': [{
    ...     '@xmlns:DTS': 'www.microsoft.com/SqlServer/Dts',
    ...     '@DTS:ExecutableType': 'SSIS.Package.2',
    ...     'DTS:Property': [{
    ...         '#text': 'Demo', '@DTS:Name': 'ObjectName'
    ...     }],
    ...     'DTS:ConnectionManager': [{"__": "Omitted for brevity"}],
    ...     'DTS:Executable': [{
    ...         '@DTS:ExecutableType': 'SSIS.Pipeline.2',
    ...         'DTS:ObjectData': [{'pipeline': [{
    ...             'components': [{'component': [{"__": "Omitted for brevity"}]}],
    ...             'paths': [{'path': [{'@endId': '31', '@id': '45', '@name': 'XYZ', '@startId': '2'}]}]
    ...         }]}],
    ...         'DTS:Property': [
    ...             {'#text': 'Extract Sample Currency Data', '@DTS:Name': 'ObjectName'},
    ...             {'#text': 'Data Flow Task', '@DTS:Name': 'Description'},
    ...         ]
    ...      }]
    ... }]})
    ... # doctest: +ELLIPSIS
    [{'@DTS:ExecutableType': 'SSIS.Pipeline.2', 'DTS:ObjectData': [{'pipeline':...'@package': {'@xmlns:DTS': 'www.microsoft.com/SqlServer/Dts'...}}]

    ```
    """  # noqa: E501
    dags = []
    for package in val.get("DTS:Executable", []):  # package is top-level
        if "SSIS.Package" in package.get(
            "@DTS:ExecutableType", ""
        ):  # check for 'package' as type
            pipelines = deepcopy(package.get("DTS:Executable", []))
            if pipelines:
                del package["DTS:Executable"]
                for pipeline in pipelines:  # pipeline is inside of that
                    if "SSIS.Pipeline" in pipeline.get("@DTS:ExecutableType", ""):
                        pipeline["@package"] = package  # Attach package as '@package'
                        dags.append(pipeline)
    return dags or None


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """
     Translate input into an `OrbiterDAG`

     Pipeline name is <DTS:Property><DTS:Name="ObjectName">...</DTS:Name>
     Package name is in (modified) @package property, and is <DTS:Property><DTS:Name="ObjectName">...</DTS:Name>

    ```pycon
     >>> basic_dag_rule(val={
     ...     '@DTS:ExecutableType': 'SSIS.Pipeline.2',
     ...     'DTS:ObjectData': [{'pipeline': [{
     ...         'components': [{'component': [{"__": "Omitted for brevity, tasks would be here"}]}],
     ...         'paths': [{'path': [{"__": "Omitted for brevity, task dependencies would be here"}]}]
     ...     }]}],
     ...     'DTS:Property': [
     ...         {'#text': 'Extract Sample Currency Data', '@DTS:Name': 'ObjectName'},
     ...         {'#text': 'Data Flow Task', '@DTS:Name': 'Description'},
     ...     ],
     ...     '@package': {
     ...         '@xmlns:DTS': 'www.microsoft.com/SqlServer/Dts',
     ...         '@DTS:ExecutableType': 'SSIS.Package.2',
     ...         'DTS:Property': [
     ...             {'#text': 'Demo', '@DTS:Name': 'ObjectName'},
     ...             {'#text': '0', '@DTS:Name': 'SuppressConfigurationWarnings'},
     ...             {'#text': 'ComputerName', '@DTS:Name': 'CreatorComputerName'},
     ...             {'#text': '8/29/2005 1:15:48 PM', '@DTS:DataType': '7', '@DTS:Name': 'CreationDate'}
     ...         ],
     ...         'DTS:ConnectionManager': [{"__": "Omitted for brevity, connections would be here"}],
     ...     }
     ... })
     ... # doctest: +ELLIPSIS
     from airflow import DAG
     from pendulum import DateTime, Timezone
     with DAG(dag_id='demo.extract_sample_currency_data', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):

     ```
    """  # noqa: E501
    if isinstance(val, dict):
        val = json.loads(json.dumps(val, default=str))  # pre-serialize values, for JQ
        try:
            # Pipeline name is <DTS:Property><DTS:Name="ObjectName">...</DTS:Name>
            pipeline_name = (
                jq.compile(
                    """."DTS:Property"[]? | select(."@DTS:Name" == "ObjectName") | ."#text" """
                )
                .input_value(val)
                .first()
            )

            # Package is in modified @package property, and is <DTS:Property><DTS:Name="ObjectName">...</DTS:Name>
            package_name = (
                jq.compile(
                    """."@package"?."DTS:Property"[]? | select(."@DTS:Name" == "ObjectName") | ."#text" """
                )
                .input_value(val)
                .first()
            )
            dag_id = f"{package_name}.{pipeline_name}"
            return OrbiterDAG(
                dag_id=dag_id,
                file_path=Path(f"{inflection.underscore(dag_id)}.py"),
                doc_md="**Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
                "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
                "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
            )
        except StopIteration:
            pass
    return None


def task_common_args(val: dict) -> dict:
    """
    Common mappings for all tasks
    """
    params = {"task_id": val.get("@name", "UNKNOWN").replace(" ", "_")}
    return params


# noinspection t
@task_filter_rule
def task_filter_rule(val: dict) -> list[dict] | None:
    """
    The key is 'components' and it has details of pipeline components.

    ```pycon
    >>> task_filter_rule(val={
    ...     '@DTS:ExecutableType': 'SSIS.Pipeline.2',
    ...     'DTS:ObjectData': [{'pipeline': [{
    ...         'components': [{'component': [{
    ...             '@name': 'Lookup CurrencyKey',
    ...             'properties': [{'property': [
    ...                 {'@id': '33', '@name': 'SqlCommand', '#text': "select * from foo"}
    ...             ]}],
    ...          }]}],
    ...         'paths': [{'path': [{"__": "Omitted for brevity, task dependencies would be here"}]}]
    ...     }]}],
    ...     'DTS:Property': [],
    ...     '@package': {
    ...         '@DTS:ExecutableType': 'SSIS.Package.2',
    ...         'DTS:Property': [
    ...             {'#text': 'Demo', '@DTS:Name': 'ObjectName'},
    ...         ],
    ...         'DTS:ConnectionManager': [{"__": "Omitted for brevity, connections would be here"}],
    ...     }
    ... })
    ... # doctest: +ELLIPSIS
    [{'@name': 'Lookup CurrencyKey', 'properties': [{'property': [{'@id': '33', '@name': 'SqlCommand', '#text': 'select * from foo'}]}]}]

    ```
    """  # noqa: E501
    if isinstance(val, dict):
        val = json.loads(json.dumps(val, default=str))  # pre-serialize values, for JQ
        try:
            return (
                jq.compile(
                    """."DTS:ObjectData"[]?.pipeline[]?.components[]?.component[]"""
                )
                .input_value(val)
                .all()
            )
        except StopIteration:
            pass
    return None


@task_rule(priority=2)
def sql_command_rule(val) -> OrbiterSQLExecuteQueryOperator | None:
    """
    For SQLQueryOperator.

    ```pycon
    >>> sql_command_rule(val={
    ...     '@name': 'Lookup CurrencyKey',
    ...     'properties': [{'property': [
    ...         {'@id': '33', '@name': 'SqlCommand', '#text': "select * from (select * from [dbo].[DimCurrency]) as refTable where [refTable].[CurrencyAlternateKey] = 'ARS' OR [refTable].[CurrencyAlternateKey] = 'AUD'"}
    ...     ]}],
    ... })
    ... # doctest: +ELLIPSIS
    lookup_currency_key_task = SQLExecuteQueryOperator(task_id='lookup_currency_key', conn_id='mssql_default', sql="select * from (select * from [dbo].[DimCurrency]) as refTable where [refTable].[CurrencyAlternateKey] = 'ARS'...")

    ```
    ''
    """  # noqa: E501
    try:
        sql: str = (
            jq.compile(
                """.properties[]?.property[]? | select(."@name" == "SqlCommand") | ."#text" """
            )
            .input_value(val)
            .first()
        )
        if sql:
            return OrbiterSQLExecuteQueryOperator(
                sql=sql,
                **conn_id(conn_id="mssql_default", conn_type="mssql"),
                **task_common_args(val),
            )
    except StopIteration:
        pass
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
    """
    Map all task input elements to task_id
        <inputs><input id='...'> -> task_id

    and map all task output elements to task_id
        <outputs><output id='...'> -> task_id

    then map all path elements to task dependencies
        <paths><path startId='...' endId='...'> -> OrbiterTaskDependency(task_id=start.task_id, downstream=end.task_id)

    ```pycon
    >>> from orbiter.objects.operators.empty import OrbiterEmptyOperator
    >>> simple_task_dependencies(val=OrbiterDAG(
    ...     dag_id=".", file_path=".",
    ...     tasks={
    ...        "foo": OrbiterEmptyOperator(task_id="foo", orbiter_kwargs={"val": {"outputs": [{"output": [{"@id": "1"}]}]}}),
    ...        "bar": OrbiterEmptyOperator(task_id="bar", orbiter_kwargs={"val": {"inputs": [{"input": [{"@id": "2"}]}]}}),
    ...     },
    ...     orbiter_kwargs={"val": {
    ...         'DTS:ObjectData': [{'pipeline': [{
    ...             'paths': [{'path': [{'@startId': '1', '@endId': '2'}]}]
    ...         }]}],
    ...      }}
    ... ))
    [foo >> bar]

    ```
    """  # noqa: E501
    # Descend through all the tasks, and get their `<outputs><output id='...'>` elements
    output_to_task_id = {
        # @id -> task_id
        output_id: task.task_id
        for task in (val.tasks.values() or [])
        # e.g. {"val": {"outputs": [{"output": {"@id": "1"}}]}}
        for output in (task.orbiter_kwargs.get("val", {}).get("outputs", []) or [])
        for o in (output.get("output", []) or [])
        if (output_id := o.get("@id"))
    }
    # Descend through all the tasks, and get their `<inputs><input id='...'>` elements
    input_to_task_id = {
        # @id -> task_id
        input_id: task.task_id
        for task in (val.tasks.values() or [])
        # e.g. {"val": {"inputs": [{"input": [{"@id": "1"}]}]}}
        for _input in (task.orbiter_kwargs.get("val", {}).get("inputs", []) or [])
        for i in (_input.get("input", []) or [])
        if (input_id := i.get("@id"))
    }

    dag_original_input = json.loads(
        json.dumps(val.orbiter_kwargs.get("val", {}), default=str)
    )  # pre-serialize values, for JQ
    # noinspection PyUnboundLocalVariable
    return [
        OrbiterTaskDependency(
            task_id=output_to_task_id[start],
            downstream=input_to_task_id[end],
        )
        # <DTS:ObjectData><pipeline><paths><path startId='...' endId='...'>
        for path in jq.compile("""."DTS:ObjectData"[]?.pipeline[]?.paths[]?.path[]""")
        .input_value(dag_original_input)
        .all()
        if (start := path.get("@startId"))
        and (end := path.get("@endId"))
        and start in output_to_task_id
        and end in input_to_task_id
    ] or None


translation_ruleset: TranslationRuleset = TranslationRuleset(
    file_type={FileTypeXML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[dag_filter_rule]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[task_filter_rule]),
    task_ruleset=TaskRuleset(ruleset=[sql_command_rule, _cannot_map_rule]),
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
