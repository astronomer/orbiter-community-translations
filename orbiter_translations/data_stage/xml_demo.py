"""Demo translation ruleset for DataStage XML files to Airflow DAGs

Contact Astronomer @ https://astronomer.io/contact for access to our full translation

```pycon
>>> translation_ruleset.test(input_value='''<?xml version="1.0" encoding="UTF-8"?>
... <DSExport>
...    <Job Identifier="DataStage_Job" DateModified="2020-11-27" TimeModified="05.07.33">
...       <Record Identifier="V253S0" Type="CustomStage" Readonly="0">
...          <Property Name="Name">SELECT_TABLE</Property>
...          <Collection Name="Properties" Type="CustomProperty">
...             <SubRecord>
...                <Property Name="Name">XMLProperties</Property>
...                <Property Name="Value" PreFormatted="1">&lt;?xml version=&apos;1.0&apos;
...                   encoding=&apos;UTF-16&apos;?&gt;&lt;Properties
...                   version=&apos;1.1&apos;&gt;&lt;Common&gt;&lt;Context
...                   type=&apos;int&apos;&gt;1&lt;/Context&gt;&lt;Variant
...                   type=&apos;string&apos;&gt;1.0&lt;/Variant&gt;&lt;DescriptorVersion
...                   type=&apos;string&apos;&gt;1.0&lt;/DescriptorVersion&gt;&lt;PartitionType
...                   type=&apos;int&apos;&gt;-1&lt;/PartitionType&gt;&lt;RCP
...                   type=&apos;int&apos;&gt;0&lt;/RCP&gt;&lt;/Common&gt;&lt;Connection&gt;&lt;URL
...                   modified=&apos;1&apos;
...                   type=&apos;string&apos;&gt;&lt;![CDATA[jdbc:snowflake://xyz.us-east-1.snowflakecomputing.com/?&amp;warehouse=#XYZ_DB.$snowflake_wh#&amp;db=#DB.$schema#]]&gt;&lt;/URL&gt;&lt;Username
...                   modified=&apos;1&apos;
...                   type=&apos;string&apos;&gt;&lt;![CDATA[#DB.$snowflake_userid#]]&gt;&lt;/Username&gt;&lt;Password
...                   modified=&apos;1&apos;
...                   type=&apos;string&apos;&gt;&lt;![CDATA[#DB.$snowflake_passwd#]]&gt;&lt;/Password&gt;&lt;Attributes
...                  modified=&apos;1&apos;
...                  type=&apos;string&apos;&gt;&lt;![CDATA[]]&gt;&lt;/Attributes&gt;&lt;/Connection&gt;&lt;Usage&gt;&lt;ReadMode
...                  type=&apos;int&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/ReadMode&gt;&lt;GenerateSQL
...                  modified=&apos;1&apos;
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/GenerateSQL&gt;&lt;EnableQuotedIDs
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/EnableQuotedIDs&gt;&lt;SQL&gt;&lt;SelectStatement
...                  collapsed=&apos;1&apos; modified=&apos;1&apos;
...                  type=&apos;string&apos;&gt;&lt;![CDATA[
...                  Select 1 as Dummy from db.schema.table Limit 1;]]&gt;&lt;ReadFromFileSelect
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/ReadFromFileSelect&gt;&lt;/SelectStatement&gt;&lt;EnablePartitionedReads
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/EnablePartitionedReads&gt;&lt;/SQL&gt;&lt;Transaction&gt;&lt;RecordCount
...                  type=&apos;int&apos;&gt;&lt;![CDATA[2000]]&gt;&lt;/RecordCount&gt;&lt;IsolationLevel
...                  type=&apos;int&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/IsolationLevel&gt;&lt;AutocommitMode
...                  modified=&apos;1&apos;
...                  type=&apos;int&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/AutocommitMode&gt;&lt;EndOfWave
...                  type=&apos;int&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/EndOfWave&gt;&lt;BeginEnd
...                  collapsed=&apos;1&apos;
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/BeginEnd&gt;&lt;/Transaction&gt;&lt;Session&gt;&lt;ArraySize
...                  type=&apos;int&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/ArraySize&gt;&lt;FetchSize
...                  type=&apos;int&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/FetchSize&gt;&lt;ReportSchemaMismatch
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/ReportSchemaMismatch&gt;&lt;DefaultLengthForColumns
...                  type=&apos;int&apos;&gt;&lt;![CDATA[200]]&gt;&lt;/DefaultLengthForColumns&gt;&lt;DefaultLengthForLongColumns
...                  type=&apos;int&apos;&gt;&lt;![CDATA[20000]]&gt;&lt;/DefaultLengthForLongColumns&gt;&lt;CharacterSetForNonUnicodeColumns
...                  collapsed=&apos;1&apos;
...                  type=&apos;int&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/CharacterSetForNonUnicodeColumns&gt;&lt;KeepConductorConnectionAlive
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/KeepConductorConnectionAlive&gt;&lt;/Session&gt;&lt;BeforeAfter
...                  modified=&apos;1&apos; type=&apos;bool&apos;&gt;&lt;![CDATA[1]]&gt;&lt;BeforeSQL
...                  collapsed=&apos;1&apos; modified=&apos;1&apos;
...                  type=&apos;string&apos;&gt;&lt;![CDATA[]]&gt;&lt;ReadFromFileBeforeSQL
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/ReadFromFileBeforeSQL&gt;&lt;FailOnError
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/FailOnError&gt;&lt;/BeforeSQL&gt;&lt;AfterSQL
...                  collapsed=&apos;1&apos; modified=&apos;1&apos;
...                  type=&apos;string&apos;&gt;&lt;![CDATA[
...                  SELECT * FROM db.schema.table;
...                  ]]&gt;&lt;ReadFromFileAfterSQL
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/ReadFromFileAfterSQL&gt;&lt;FailOnError
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/FailOnError&gt;&lt;/AfterSQL&gt;&lt;BeforeSQLNode
...                  type=&apos;string&apos;&gt;&lt;![CDATA[]]&gt;&lt;ReadFromFileBeforeSQLNode
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/ReadFromFileBeforeSQLNode&gt;&lt;FailOnError
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/FailOnError&gt;&lt;/BeforeSQLNode&gt;&lt;AfterSQLNode
...                  type=&apos;string&apos;&gt;&lt;![CDATA[]]&gt;&lt;ReadFromFileAfterSQLNode
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/ReadFromFileAfterSQLNode&gt;&lt;FailOnError
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[1]]&gt;&lt;/FailOnError&gt;&lt;/AfterSQLNode&gt;&lt;/BeforeAfter&gt;&lt;Java&gt;&lt;ConnectorClasspath
...                  type=&apos;string&apos;&gt;&lt;![CDATA[$(DSHOME)/../DSComponents/bin/ccjdbc.jar;$(DSHOME)]]&gt;&lt;/ConnectorClasspath&gt;&lt;HeapSize
...                  modified=&apos;1&apos;
...                  type=&apos;int&apos;&gt;&lt;![CDATA[1024]]&gt;&lt;/HeapSize&gt;&lt;ConnectorOtherOptions
...                  type=&apos;string&apos;&gt;&lt;![CDATA[-Dcom.ibm.is.cc.options=noisfjars]]&gt;&lt;/ConnectorOtherOptions&gt;&lt;/Java&gt;&lt;LimitRows
...                  collapsed=&apos;1&apos;
...                  type=&apos;bool&apos;&gt;&lt;![CDATA[0]]&gt;&lt;/LimitRows&gt;&lt;/Usage&gt;&lt;/Properties
...                  &gt;</Property>
...            </SubRecord>
...         </Collection>
...      </Record>
...   </Job>
... </DSExport>''').dags['data_stage_job'] # doctest: +ELLIPSIS
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='data_stage_job', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):
    select_table_task = SQLExecuteQueryOperator(task_id='select_table', conn_id='DB', sql=['Select 1 as Dummy from db.schema.table Limit 1;', 'SELECT * FROM db.schema.table;'])

"""
from __future__ import annotations

import json
from itertools import pairwise

from loguru import logger

import jq
from orbiter.file_types import FileTypeXML
from orbiter.objects import conn_id
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.operators.sql import OrbiterSQLExecuteQueryOperator
from orbiter.objects.task import OrbiterOperator
from orbiter.objects.task import OrbiterTaskDependency
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
def basic_dag_filter(val: dict) -> list[dict] | None:
    """Get `Job` objects from within a parent `DSExport` object

    ```pycon
    >>> basic_dag_filter({"DSExport": [{"Job": [{'@Identifier': 'foo'}, {'@Identifier': 'bar'}]}]})
    [{'@Identifier': 'foo'}, {'@Identifier': 'bar'}]

    ```
    """
    if ds_export := val.get("DSExport"):
        return [job for export in ds_export for job in export.get("Job") if export.get("Job")]


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Translate input into an `OrbiterDAG`, using the `Identifier` as the DAG ID

    ```pycon
    >>> basic_dag_rule({"@Identifier": "demo.extract_sample_currency_data"}) # doctest: +ELLIPSIS
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='demo.extract_sample_currency_data', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):

    ```
    """
    if dag_id := val.get("@Identifier"):
        return OrbiterDAG(
            dag_id=dag_id,
            file_path=f"{dag_id}.py",
            doc_md="**Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
                   "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
                   "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
        )


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries with `@Type=CustomStage`

    ```pycon
    >>> basic_task_filter({"Record": [{"@Type": "CustomStage"}, {"@Type": "SomethingElse"}]})
    [{'@Type': 'CustomStage'}]

    ```
    """
    if isinstance(val, dict):
        val = json.loads(json.dumps(val, default=str))  # pre-serialize values, for JQ
        try:
            return (
                jq.compile(""".Record[] | select(.["@Type"] == "CustomStage")""")
                .input_value(val)
                .all()
            )
        except StopIteration:
            pass
    return None


def task_common_args(val: dict) -> dict:
    """
    Common mappings for all tasks
    """
    try:
        task_id: str = (
            jq.compile(""".Property[] | select(.["@Name"] == "Name") | .["#text"]""")
            .input_value(val)
            .first()
        )
    except ValueError:
        task_id = "UNKNOWN"
    params = {"task_id": task_id}
    return params


@task_rule(priority=2)
def _cannot_map_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate input into an Operator (e.g. `OrbiterBashOperator`). will be applied first, with a higher priority"""
    return OrbiterEmptyOperator(**task_common_args(val))


def extract_sql_statements(root: dict) -> list[str]:
    """Find SQL Statements deeply nested

    Looks for text of `SelectStatement`, `BeforeSQL`, and `AfterSQL` tags

    ```pycon
    >>> extract_sql_statements({
    ...     "BeforeSQL": [{"#text": ""}],
    ...     "SelectStatement": [{"#text": "SELECT 1 as Dummy from db.schema.table Limit 1;"}],
    ...     "AfterSQL": [{"#text": "SELECT * FROM db.schema.table;"}]
    ... })
    ['SELECT 1 as Dummy from db.schema.table Limit 1;', 'SELECT * FROM db.schema.table;']
    >>> extract_sql_statements({"a": {"b": {"c": {"d": {"SelectStatement": [{"#text": "SELECT 1;"}]}}}}})
    ['SELECT 1;']

    ```
    """
    if root:
        return [
            sql.strip()
            for tag in [
                "BeforeSQL",
                "SelectStatement",
                "AfterSQL"
            ]
            for sql in jq.all(f"""recurse | select(.{tag}?) | .{tag}[]["#text"]""", root)
            if sql and sql.strip()
        ] or None
    raise ValueError("No SQL Statements found")

@task_rule(priority=2)
def sql_command_rule(val) -> OrbiterSQLExecuteQueryOperator | None:
    """
    Create a SQL Operator with one or more SQL Statements


    ```pycon
    >>> sql_command_rule({
    ...   'Property': [{'@Name': 'Name', '#text': 'SELECT_TABLE'}],
    ...   '@Identifier': 'V253S0',
    ...   "Collection": [{"SubRecord": [{"Property": [{
    ...     "@PreFormatted": "1",
    ...     "#text": {"Properties": [{
    ...         "Usage": [{"SQL": [{"SelectStatement": [{"#text": "SELECT 1;"}]}]}],
    ...     }]}
    ...   }]}]}]
    ... }) # doctest: +ELLIPSIS
    select_table_task = SQLExecuteQueryOperator(task_id='select_table', conn_id='DB', sql='SELECT 1;')

    ```
    """  # noqa: E501
    try:
        if sql := jq.first(
            """.Collection[] | .SubRecord[] | .Property[] | select(.["@PreFormatted"] == "1") | .["#text"]""",
            val
        ):
            return OrbiterSQLExecuteQueryOperator(
                sql=stmt[0] if len(stmt := extract_sql_statements(sql)) == 1 else stmt,
                **conn_id(conn_id="DB"),
                **task_common_args(val),
            )
    except (StopIteration, ValueError) as e:
        logger.debug(f"[WARNING] No SQL found in {val}, {e}")
    return None


@task_dependency_rule
def basic_task_dependency_rule(val: OrbiterDAG) -> list | None:
    """Translate input into a list of task dependencies"""
    task_dependencies = []
    if len(val.tasks.values()) > 1:
        for pre, post in pairwise(val.tasks.values()):
            task_dependencies.append(
                OrbiterTaskDependency(
                    task_id=pre.task_id,
                    downstream=post.task_id,
                )
            )
        return task_dependencies
    return []


translation_ruleset = TranslationRuleset(
    file_type={FileTypeXML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[sql_command_rule, _cannot_map_rule, cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[basic_task_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
