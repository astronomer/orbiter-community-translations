from __future__ import annotations
from itertools import pairwise
from defusedxml import ElementTree
import inflection
import json
import jq
from orbiter.file_types import FileTypeXML
from orbiter.objects import conn_id
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.operators.sql import OrbiterSQLExecuteQueryOperator
from orbiter.objects.project import OrbiterProject
from orbiter.objects.task import OrbiterOperator
from orbiter.objects.task_group import OrbiterTaskGroup
from orbiter.objects.task import OrbiterTaskDependency
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_filter_rule,
    task_rule,
    task_dependency_rule,
    post_processing_rule,
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
    return val["DSExport"][0]["Job"]


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Translate input into an `OrbiterDAG`"""
    try:
        dag_id = val["@Identifier"]
        dag_id = inflection.underscore(dag_id)
        return OrbiterDAG(dag_id=dag_id, file_path=f"{dag_id}.py")
    except Exception:
        return None


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Filter input down to a list of dictionaries that can be processed by the `@task_rules`"""
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


@task_rule(priority=2)
def basic_task_rule(val: dict) -> OrbiterOperator | OrbiterTaskGroup | None:
    """Translate input into an Operator (e.g. `OrbiterBashOperator`). will be applied first, with a higher priority"""
    if "task_id" in val:
        return OrbiterEmptyOperator(task_id=val["task_id"])
    else:
        return None


def task_common_args(val: dict) -> dict:
    """
    Common mappings for all tasks
    """
    task_id: str = (
        jq.compile(""".Property[] | select(.["@Name"] == "Name") | .["#text"]""")
        .input_value(val)
        .first()
    )
    task_id = inflection.underscore(task_id)
    params = {"task_id": task_id}
    return params


def extract_sql_statements(root):
    sql_statements = {}
    sql_tags = ["SelectStatement", "BeforeSQL", "AfterSQL"]

    for tag in sql_tags:
        elements = root.findall(f".//{tag}")
        for elem in elements:
            if elem.text:
                sql_text = elem.text.strip()
                sql_statements[tag] = sql_text
    return sql_statements


@task_rule(priority=2)
def sql_command_rule(val) -> OrbiterSQLExecuteQueryOperator | None:
    """
    For SQLQueryOperator.

    """  # noqa: E501
    try:
        sql: str = (
            jq.compile(
                """.Collection[] | .SubRecord[] | .Property[] | select(.["@PreFormatted"] == "1") | .["#text"] """
            )
            .input_value(val)
            .first()
        )
        root = ElementTree.fromstring(sql.encode("utf-16"))
        sql_statements = extract_sql_statements(root)
        sql = " ".join(sql_statements.values())
        if sql:
            return OrbiterSQLExecuteQueryOperator(
                sql=sql,
                **conn_id(conn_id="snowflake_default", conn_type="snowflake"),
                **task_common_args(val),
            )
    except StopIteration:
        pass
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
    task_ruleset=TaskRuleset(
        ruleset=[sql_command_rule, basic_task_rule, cannot_map_rule]
    ),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[basic_task_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[basic_post_processing_rule]),
)
