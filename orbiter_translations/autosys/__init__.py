r"""Demonstration of converting an Autosys JIL workflow to an Airflow DAG

```pycon
>>> translation_ruleset.test('''
... insert_job: foo_job
... job_type: CMD
... command: "C:\\foo\\bar\\baz.cmd" "arg1" "arg2" "arg3"
... machine: agent01
... owner: foo@bar.com
... permission:
... date_conditions: 1
... run_calendar: Workdays_Weekdays
... start_mins: 00,20,40
... run_window: "08:00 - 17:00"
... description: "Foo Job"
... std_out_file: "C:\\apps\\autosys\\log\\%AUTO_JOB_NAME%.$$sysdate.out"
... std_err_file: "C:\\apps\\autosys\\log\\%AUTO_JOB_NAME%.$$sysdate.err"
... alarm_if_fail: 1
... group: TEST
... send_notification: F
... notification_msg: "foo_bar"
... notification_emailaddress: foo@bar.com
... notification_emailaddress: baz@bar.com
... notification_emailaddress: qux@bar.com
... ''').dags['foo_job']
... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='foo_job', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, tags=['TEST'], default_args={'owner': 'foo@bar.com'}):
    foo_job_task = BashOperator(task_id='foo_job', bash_command='"C:\\foo\\bar\\baz.cmd" "arg1" "arg2" "arg3"')

```
"""

from __future__ import annotations

from jilutil import AutoSysJob
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.bash import OrbiterBashOperator
from orbiter.rules import (
    task_filter_rule,
    task_rule,
    dag_filter_rule,
    dag_rule,
    cannot_map_rule,
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

from orbiter.file_types import FileTypeJIL


@dag_filter_rule
def basic_filter_rule(val: dict) -> list[dict] | None:
    r"""Basic filter rule to extract the `DAG` from an AutoSys dump.
    ```pycon
    >>> basic_filter_rule({
    ...     'jobs': [
    ...         {
    ...             'insert_job': 'foo_job',
    ...             'job_type': 'CMD',
    ...             'machine': 'machine_id',
    ...             'command': '"C:\\foo\\bar\\baz.cmd" "arg1" "arg2" "arg3"',
    ...         }
    ...     ]
    ... })
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    [{'insert_job': 'foo_job', 'job_type': 'CMD', 'machine': 'machine_id', 'command': '"C:\\foo\\bar\\baz.cmd" "arg1" "arg2" "arg3"'}]

    ```
    """
    return [job.data if isinstance(job, AutoSysJob) else job for job in val["jobs"]]


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Map AutoSysWorkflow into a DAG
    ```pycon
    >>> basic_dag_rule({
    ...     'insert_job': 'foo_job',
    ...     'job_type': 'CMD',
    ...     'command': '"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" "arg3"',
    ...     'machine': 'machine_id',
    ...     'owner': 'foo@bar.net',
    ...     'group': 'group_name'
    ... })
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='foo_job', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, tags=['group_name'], default_args={'owner': 'foo@bar.net'}):

    ```
    """
    if dag_id := val.get("insert_job"):
        return OrbiterDAG(
            dag_id=dag_id,
            file_path=f"{dag_id}.py",
            **(
                dict(default_args={"owner": val.get("owner")})
                if val.get("owner")
                else {}
            ),
            **(dict(tags=[val.get("group")]) if val.get("group") else {}),
        )


@task_filter_rule
def basic_task_filter(val: dict) -> list[dict] | None:
    r"""Returns the list of dictionaries that can be processed by the `@task_rules`
    ```pycon
    >>> basic_task_filter({
    ...     'insert_job': 'task_id',
    ...     'job_type': 'CMD',
    ...     'command': '"C:\\foo\\bar\\baz.cmd" "arg1" "arg2" "arg3"',
    ...     'machine': 'machine_id',
    ...     'owner': 'foo@bar.net'
    ... })
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    [{'insert_job': 'task_id',
    'job_type': 'CMD',
    'command': '"C:\\foo\\bar\\baz.cmd" "arg1" "arg2" "arg3"',
    'machine': 'machine_id',
    'owner': 'foo@bar.net'}]

    ```
    """
    return [val]


@task_rule(priority=1)
def command_rule(val) -> OrbiterBashOperator | None:
    r"""Maps a workflow of job_type 'CMD' into a SSHOperator
    ```pycon
    >>> command_rule({
    ...     'insert_job': 'foo_bar',
    ...     'job_type': 'CMD',
    ...     'command': '"C:\\foo\\bar\\baz.cmd" "arg1" "arg2" "arg3"',
    ...     'machine': 'machine_id',
    ...     'owner': 'foo@bar.net',
    ...     'alarm_if_fail': '1',
    ...     'group': 'group_name',
    ...     'send_notification': 'F',
    ...     'notification_emailaddress': []
    ... })
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    foo_bar_task = BashOperator(task_id='foo_bar', bash_command='"C:\\foo\\bar\\baz.cmd" "arg1" "arg2" "arg3"')

    ```
    """
    if val.get("job_type").lower() == "cmd":
        return OrbiterBashOperator(
            task_id=val.get("insert_job", "UNKNOWN"), bash_command=val["command"]
        )


translation_ruleset = TranslationRuleset(
    file_type={FileTypeJIL},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_filter_rule]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[command_rule, cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
    translate_fn=translate,
)
