"""
Demo Automic XML Translation to Airflow DAGs

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

```pycon
>>> translation_ruleset.test(input_value='''<?xml version="1.0" encoding="ISO-8859-15"?>
... <uc-export clientvers="21.0.9+hf.1.build.xxxxxxxxxxxxx">
...    <JOBP AllowExternal="1" name="JOBP.DUMMY.WORKFLOW">
...        <JobpStruct mode="design">
...            <task Alias="" BranchType="0" Col="1" Lnr="1" OH_TITLE="start" OType="&lt;START&gt;" Object="START" Row="1" Text1="Start of the workflow.">
...                <predecessors/>
...            </task>
...            <task Alias="" BranchType="0" Col="2" Lnr="2" OH_TITLE="generate_file" OType="SCRI" Object="SCRI.GENERATE.FILE" Row="1" State="" Text1="Runs a Script which generates a file for processing.">
...                <predecessors>
...                    <pre BranchType="0" Lnr="1" PreLnr="1" When="" type="container"/>
...                </predecessors>
...            </task>
...            <task Alias="" BranchType="0" Col="3" Lnr="3" OH_TITLE="process_and_upload_file" OType="SCRI" Object="SCRI.PROCESS.AND.UPLOAD.FILE" Row="1" State="" Text1="Processes the file and uploads it.">
...                <predecessors>
...                    <pre BranchType="0" Lnr="1" PreLnr="2" When="" type="container"/>
...                </predecessors>
...            </task>
...            <task Alias="" BranchType="0" Col="4" Lnr="4" OH_TITLE="insert_into_database" OType="JOBS" Object="JOBS.INSERT.INTO.DATABASE" Row="1" State="" Text1="Inserts the processed records into database.">
...                <predecessors>
...                    <pre BranchType="0" Lnr="1" PreLnr="3" When="" type="container"/>
...                </predecessors>
...            </task>
...            <task Alias="" BranchType="0" Col="5" Lnr="5" OH_TITLE="end" OType="&lt;END&gt;" Object="END" Row="1" State="" Text1="End of the workflow.">
...                <predecessors>
...                    <pre BranchType="0" Lnr="1" PreLnr="4" When="" type="container"/>
...                </predecessors>
...            </task>
...        </JobpStruct>
...    </JOBP>
...    <SCRI name="SCRI.GENERATE.FILE">
...        <SCRIPT mode="1" state="1">echo "I am a Script"</SCRIPT>
...    </SCRI>
...    <JOBS name="JOBS.INSERT.INTO.DATABASE">
...        <SCRIPT><![CDATA[! A SQL command :SQL INSERT INTO users VALUES ("name", "age", "profession");]]></SCRIPT>
...    </JOBS>
...    <SCRI name="SCRI.PROCESS.AND.UPLOAD.FILE">
...        <SCRIPT mode="1" state="1">echo "Runs a Python Script"</SCRIPT>
...    </SCRI>
... </uc-export>
... ''').dags['jobp.dummy.workflow']
... # doctest: +ELLIPSIS
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='jobp.dummy.workflow', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...):
    start_task = EmptyOperator(task_id='start')
    generate_file_task = BashOperator(task_id='generate_file', bash_command='echo "I am a Script"')
    process_and_upload_file_task = BashOperator(task_id='process_and_upload_file', bash_command='echo "Runs a Python Script"')
    insert_into_database_task = SQLExecuteQueryOperator(task_id='insert_into_database', conn_id='mssql_default', sql='INSERT INTO users VALUES ("name", "age", "profession");')
    end_task = EmptyOperator(task_id='end')
    generate_file_task >> process_and_upload_file_task
    insert_into_database_task >> end_task
    process_and_upload_file_task >> insert_into_database_task
    start_task >> generate_file_task

```
"""  # noqa: E501

from __future__ import annotations
import json
from pathlib import Path

import inflection
import jq
from orbiter.file_types import FileTypeXML
from orbiter.objects import conn_id
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.operators.bash import OrbiterBashOperator
from orbiter.objects.operators.sql import OrbiterSQLExecuteQueryOperator
from orbiter.objects.task import OrbiterTaskDependency
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
def dag_filter_rule(val: dict) -> list | None:
    """
    Filter input to the top level element <uc_export> and further,
    to a list of dictionaries that can be processed by the `@dag_rules`
    ```pycon
    >>> dag_filter_rule(val={'uc-export': [
    ...    {'@clientvers': '', 'JOBP': [
    ...            {'@AllowExternal': '1', '@name': 'JOBP.DUMMY.WORKFLOW', 'JobpStruct': [
    ...                    {'@mode': 'design', 'task': [
    ...                            {'@Alias': '', '@BranchType': '0', '@Col': '2', '@Lnr': '2', '@OH_TITLE': '', '@OType': 'SCRI', '@Object': 'SCRI.SCRIPT', '@Row': '1', '@State': '', '@Text1': '', 'predecessors': [
    ...                                    {'pre': [
    ...                                            {'@BranchType': '0', '@Lnr': '1', '@PreLnr': '1', '@When': '', '@type': 'container'
    ...                                            }
    ...                                        ]
    ...                                    }
    ...                                ]
    ...                            }
    ...                        ]
    ...                    }
    ...                ]
    ...            }
    ...        ], 'SCRI': [
    ...            {'@name': 'SCRI.SCRIPT', 'SCRIPT': [
    ...                    {'@mode': '1', '@state': '1', '#text': 'echo ""'
    ...                    }
    ...                ]
    ...            }
    ...        ]
    ...    }
    ... ]
    ... })
    ... # doctest: +ELLIPSIS
    [{'@clientvers': '', 'JOBP': [{'@AllowExternal': '1', '@name': 'JOBP.DUMMY.WORKFLOW', 'JobpStruct': [{'@mode': 'design', 'task': [{'@Alias': '', '@BranchType': '0', '@Col': '2', '@Lnr': '2', '@OH_TITLE': '', '@OType': 'SCRI', '@Object': 'SCRI.SCRIPT', '@Row': '1', '@State': '', '@Text1': '', 'predecessors': [{'pre': [{'@BranchType': '0', '@Lnr': '1', '@PreLnr': '1', '@When': '', '@type': 'container'}]}]}]}]}], 'SCRI': [{'@name': 'SCRI.SCRIPT', 'SCRIPT': [{'@mode': '1', '@state': '1', '#text': 'echo ""'}]}]}]

    ```
    """  # noqa: E501
    return val.get("uc-export", {}) or None


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """
     Translate input into an `OrbiterDAG`

    ```pycon
    >>> basic_dag_rule(val={'@clientvers': '21.0.9+hf.1.build.xxxxxxxxxxxxx', 'JOBP': [
    ...    {'@AllowExternal': '1', '@name': 'JOBP.DUMMY.WORKFLOW', 'JobpStruct': [
    ...            {'@mode': 'design', 'task': [
    ...                    {'@Alias': '', '@BranchType': '0', '@Col': '2', '@Lnr': '2', '@OH_TITLE': '', '@OType': 'SCRI', '@Object': 'SCRI.SCRIPT', '@Row': '1', '@State': '', '@Text1': '', 'predecessors': [
    ...                            {'pre': [
    ...                                    {'@BranchType': '0', '@Lnr': '1', '@PreLnr': '1', '@When': '', '@type': 'container'
    ...                                    }
    ...                               ]
    ...                            }
    ...                        ]
    ...                    }
    ...                ]
    ...            }
    ...        ]
    ...    }
    ... ]
    ... })
    ... # doctest: +ELLIPSIS
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='jobp.dummy.workflow', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md=...:

    ```
    """  # noqa: E501
    val = json.loads(json.dumps(val, default=str))  # pre-serialize values, for JQ
    if isinstance(val, dict):
        try:
            dag_id = jq.compile(""".JOBP[0] | ."@name" """).input_value(val).first()
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


@task_filter_rule
def task_filter_rule(val: dict) -> list | None:
    """
    The task declaration is in 'task' inside 'JOBP.JobpStruct' and the definitions are in SCRI or JOBS.

    ```pycon
    >>> task_filter_rule(val={'@clientvers': '21.0.9+hf.1.build.xxxxxxxxxxxxx', 'JOBP': [
    ...    {'@AllowExternal': '1', '@name': 'JOBP.DUMMY.WORKFLOW', 'JobpStruct': [
    ...            {'@mode': 'design', 'task': [
    ...                    {'@Alias': '', '@BranchType': '0', '@Col': '2', '@Lnr': '2', '@OH_TITLE': '', '@OType': 'SCRI', '@Object': 'SCRI.SCRIPT', '@Row': '1', '@State': '', '@Text1': '', 'predecessors': [
    ...                            {'pre': [
    ...                                    {'@BranchType': '0', '@Lnr': '1', '@PreLnr': '1', '@When': '', '@type': 'container'
    ...                                    }
    ...                                ]
    ...                            }
    ...                        ]
    ...                    }
    ...               ]
    ...            }
    ...        ]
    ...    }
    ... ], 'SCRI': [
    ...    {'@name': 'SCRI.SCRIPT', 'SCRIPT': [
    ...            {'@mode': '1', '@state': '1', '#text': 'echo ""'
    ...            }
    ...        ]
    ...    }
    ... ]
    ... })
    ... # doctest: +ELLIPSIS
    [{'@Alias': '', '@BranchType': '0', '@Col': '2', '@Lnr': '2', '@OH_TITLE': '', '@OType': 'SCRI', '@Object': 'SCRI.SCRIPT', '@Row': '1', '@State': '', '@Text1': '', 'predecessors': [{'pre': [{'@BranchType': '0', '@Lnr': '1', '@PreLnr': '1', '@When': '', '@type': 'container'}]}], 'script': {'@name': 'SCRI.SCRIPT', 'SCRIPT': [{'@mode': '1', '@state': '1', '#text': 'echo ""'}]}}]

    ```
    """  # noqa: E501
    task_definitions = {}
    tasks = []
    for dag_struct_key, struct in val.items():
        if dag_struct_key in ["SCRI", "JOBS"]:
            for task_details in struct:
                task_definitions[task_details["@name"]] = task_details

    for dag_struct_key, struct in val.items():
        if "JOBP" in dag_struct_key:
            for task in struct[0]["JobpStruct"][0]["task"]:
                if task["@OType"] in ["SCRI", "JOBS"]:
                    task["script"] = task_definitions[task["@Object"]]
                tasks.append(task)
    return tasks


@task_rule(priority=2)
def sql_command_rule(
    val: dict,
) -> OrbiterSQLExecuteQueryOperator | None:
    """
    Translate input into an OrbiterSQLExecuteQueryOperator""

    ```pycon
    >>> sql_command_rule(val={'@Alias': '', '@BranchType': '0', '@Col': '2', '@Lnr': '2', '@OH_TITLE': 'insert_into_database', '@OType': 'JOBS', '@Object': 'JOBS.SQL', '@Row': '1', '@State': '', '@Text1': '', 'predecessors': [
    ...    {'pre': [
    ...            {'@BranchType': '0', '@Lnr': '1', '@PreLnr': '1', '@When': '', '@type': 'container'
    ...            }
    ...        ]
    ...    }
    ... ], 'script': {'@name': 'JOBS.SQL', 'SCRIPT': 'A SQL command :SQL INSERT INTO users VALUES ("name", "age", "profession");'
    ... }
    ... })
    ... # doctest: +ELLIPSIS
    insert_into_database_task = SQLExecuteQueryOperator(task_id='insert_into_database', conn_id='mssql_default', sql='INSERT INTO users VALUES ("name", "age", "profession");')

    ```
    """  # noqa: E501
    try:
        if val["@OType"] == "JOBS":
            if "SQL" in val["script"]["SCRIPT"]:
                return OrbiterSQLExecuteQueryOperator(
                    task_id=val["@OH_TITLE"],
                    sql=val["script"]["SCRIPT"].split(":SQL")[-1].strip(),
                    **conn_id(conn_id="mssql_default", conn_type="mssql"),
                )
    except StopIteration:
        pass
    return None


@task_rule(priority=2)
def bash_command_rule(
    val: dict,
) -> OrbiterBashOperator | None:
    """
    Translate input into an OrbiterBashOperator

    ```pycon
    >>> bash_command_rule(val={'@Alias': '', '@BranchType': '0', '@Col': '2', '@Lnr': '2', '@OH_TITLE': 'generate_file', '@OType': 'SCRI', '@Object': 'SCRI.SCRIPT', '@Row': '1', '@State': '', '@Text1': '', 'predecessors': [
    ...    {'pre': [
    ...            {'@BranchType': '0', '@Lnr': '1', '@PreLnr': '1', '@When': '', '@type': 'container'
    ...            }
    ...        ]
    ...    }
    ... ], 'script': {'@name': 'SCRI.SCRIPT', 'SCRIPT': [
    ...        {'@mode': '1', '@state': '1', '#text': 'echo "I am a Script"'
    ...        }
    ...    ]
    ... }
    ... })
    ... # doctest: +ELLIPSIS
    generate_file_task = BashOperator(task_id='generate_file', bash_command='echo "I am a Script"')

    ```
    """  # noqa: E501
    try:
        if val["@OType"] == "SCRI":
            return OrbiterBashOperator(
                task_id=val["@OH_TITLE"],
                bash_command=val["script"]["SCRIPT"][0]["#text"],
            )
    except StopIteration:
        pass
    return None


@task_rule(priority=2)
def start_or_end_rule(
    val: dict,
) -> OrbiterEmptyOperator | None:
    """
    Translate input into an EmptyOperator for start and end tasks

    ```pycon
    >>> start_or_end_rule(val={'@Alias': '', '@BranchType': '0', '@Col': '2', '@Lnr': '2', '@OH_TITLE': 'start', '@OType': '', '@Object': 'START', '@Row': '1', '@State': '', '@Text1': '', 'predecessors': None,
    ... })
    ... # doctest: +ELLIPSIS
    start_task = EmptyOperator(task_id='start')

    ```
    """  # noqa: E501
    task_id = val["@OH_TITLE"]
    if val["@Object"].lower() in ["start", "end"]:
        return OrbiterEmptyOperator(task_id=task_id)


@task_dependency_rule
def simple_task_dependencies(val: OrbiterDAG) -> list[OrbiterTaskDependency] | None:
    """
    Map all tasks to their predecessors to get the dependency graph

    ```pycon
    >>> simple_task_dependencies(val=OrbiterDAG(
    ...     dag_id=".", file_path=".",
    ...     tasks={
    ...            'start': OrbiterEmptyOperator(task_id='start', orbiter_kwargs={'val':{'@Alias': '', '@BranchType': '0', '@Col': '1', '@Lnr': '1', '@OH_TITLE': 'start', '@OType': '<START>', '@Object': 'START', '@Row': '1', '@Text1': 'Start of the workflow.', 'predecessors': None
    ...                        }}),
    ...            'end': OrbiterEmptyOperator(task_id='end', orbiter_kwargs={'val': {'@Alias': '', '@BranchType': '0', '@Col': '2', '@Lnr': '2', '@OH_TITLE': 'end', '@OType': '<END>', '@Object': 'END', '@Row': '1', '@State': '', '@Text1': 'End of the workflow.', 'predecessors': [{'pre': [{'@BranchType': '0', '@Lnr': '1', '@PreLnr': '1', '@When': '', '@type': 'container'}]}]}})
    ...        },
    ...     orbiter_kwargs={'val': {'@clientvers': '21.0.9+hf.1.build.xxxxxxxxxxxxx', 'JOBP': [
    ...        {'@AllowExternal': '1', '@name': 'JOBP.DUMMY.WORKFLOW', 'JobpStruct': [
    ...                {'@mode': 'design', 'task': [
    ...                        {'@Alias': '', '@BranchType': '0', '@Col': '1', '@Lnr': '1', '@OH_TITLE': 'start', '@OType': '<START>', '@Object': 'START', '@Row': '1', '@Text1': 'Start of the workflow.', 'predecessors': None
    ...                        },
    ...                        {'@Alias': '', '@BranchType': '0', '@Col': '2', '@Lnr': '2', '@OH_TITLE': 'end', '@OType': '<END>', '@Object': 'END', '@Row': '1', '@State': '', '@Text1': 'End of the workflow.', 'predecessors': [
    ...                                {'pre': [
    ...                                        {'@BranchType': '0', '@Lnr': '1', '@PreLnr': '1', '@When': '', '@type': 'container'
    ...                                        }
    ...                                    ]
    ...                                }
    ...                            ]
    ...                        }
    ...                    ]
    ...                }
    ...            ]
    ...       }
    ...    ]
    ... },
    ... }
    ... ))
    ... # doctest: +ELLIPSIS
    [start >> end]

    ```
    """  # noqa: E501
    all_tasks = {}
    for task_id, task_attr in val.tasks.items():
        task = task_attr.orbiter_kwargs.get("val", {})
        task_no = task["@Lnr"]
        if task.get("predecessors", None):
            predecessor = (
                task.get("predecessors", [{}])[0].get("pre", [{}])[0].get("@PreLnr", [])
            )
        else:
            predecessor = None
        all_tasks[task_no] = {
            "task_id": task_id,
            "predecessor": predecessor,
        }
    task_dependencies = []
    for task_no, task_metadata in all_tasks.items():
        if task_metadata["predecessor"]:
            for predecessor_task_no in task_metadata["predecessor"]:
                task_dependencies.append(
                    OrbiterTaskDependency(
                        task_id=all_tasks[predecessor_task_no]["task_id"],
                        downstream=task_metadata["task_id"],
                    )
                )
    return task_dependencies


translation_ruleset: TranslationRuleset = TranslationRuleset(
    file_type={FileTypeXML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[dag_filter_rule]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[task_filter_rule]),
    task_ruleset=TaskRuleset(
        ruleset=[
            sql_command_rule,
            bash_command_rule,
            start_or_end_rule,
            cannot_map_rule,
        ]
    ),
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
