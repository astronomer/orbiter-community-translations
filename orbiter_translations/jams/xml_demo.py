r"""Demo ruleset converting JAMS Jobs to Airflow DAGs

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

```pycon
>>> list(translation_ruleset.test(r'''<?xml version="1.0" encoding="utf-8"?>
... <Jobs xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://jams.mvpsi.com/v1">
...   <Job name="Foo">
...     <Description>Foo Job</Description>
...     <HomeDirectory>C:\FooUser</HomeDirectory>
...     <AgentNode>foo.agent.com</AgentNode>
...     <UserName>FooUser</UserName>
...     <ScheduledDate>Weekdays</ScheduledDate>
...     <AutoSubmit>true</AutoSubmit>
...     <ScheduledTime>21:00:00</ScheduledTime>
...     <ResubmitDelay>00:15</ResubmitDelay>
...     <RetryInterval>00:05</RetryInterval>
...     <RetryCount>99</RetryCount>
...     <Source>"C:\FooUser\foo_job.exe" abc</Source>
...   </Job>
...   <Job name="Bar">
...     <Description>Bar Job</Description>
...     <AgentNode>foo.agent.com</AgentNode>
...     <UserName>BarUser</UserName>
...     <ScheduledDateStart>2022-01-04T00:00:00</ScheduledDateStart>
...     <ScheduledDate>Weekdays</ScheduledDate>
...     <ExceptForDate>Holidays</ExceptForDate>
...     <AutoSubmit>true</AutoSubmit>
...     <ScheduledTime>13:30:00</ScheduledTime>
...     <Source>"C:\User\baz_job.exe" bop</Source>
...   </Job>
...   <Job name="Email Notification">
...     <Description>Send an email</Description>
...     <AgentNode>foo.agent.com</AgentNode>
...     <UserName>FooUser</UserName>
...     <ScheduledDate>Weekdays</ScheduledDate>
...     <ScheduledDateStart>2020-10-07T00:00:00</ScheduledDateStart>
...     <AutoSubmit>true</AutoSubmit>
...     <ScheduledTime>18:00:00</ScheduledTime>
...     <SubmitMethodName>Command</SubmitMethodName>
...     <Dependencies>
...         <Dependency xsi:type="DependencyJob" job="\Batch_Jobs\Foo">
...             <WithinTime>01:00</WithinTime>
...             <SinceType>Job</SinceType>
...             <SinceSeverity>Success</SinceSeverity>
...             <CompletionSeverity>Success</CompletionSeverity>
...             <WaitIfQueued>true</WaitIfQueued>
...             <SinceJobName>\Batch_Jobs\Foo</SinceJobName>
...         </Dependency>
...         <Dependency xsi:type="DependencyJob" job="\Batch_Jobs\Bar">
...             <WithinTime>01:00</WithinTime>
...             <SinceType>Job</SinceType>
...             <SinceSeverity>Success</SinceSeverity>
...             <CompletionSeverity>Success</CompletionSeverity>
...             <WaitIfQueued>true</WaitIfQueued>
...             <SinceJobName>\Batch_Jobs\Bar</SinceJobName>
...         </Dependency>
...     </Dependencies>
...     <Source>"C:\FooUser\email.exe" "hello, world"</Source>
...   </Job>
...  </Jobs>
... ''').dags.values())[0] # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='...', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    foo_task = SSHOperator(task_id='foo', ssh_conn_id='foo.agent.com_FooUser', command='"C:\\FooUser\\foo_job.exe" abc', doc_md='Foo Job')
    bar_task = SSHOperator(task_id='bar', ssh_conn_id='foo.agent.com_BarUser', command='"C:\\User\\baz_job.exe" bop', doc_md='Bar Job')
    email_notification_task = SSHOperator(task_id='email_notification', ssh_conn_id='foo.agent.com_FooUser', command='"C:\\FooUser\\email.exe" "hello, world"', doc_md='Send an email')

>>> list(translation_ruleset.test(r'''<?xml version="1.0" encoding="utf-8"?>
...  <JAMSObjects xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://jams.mvpsi.com/v1">
...   <anyType xsi:type="Job" name="Baz">
...     <Description>Baz Job</Description>
...     <AgentNode>foo.agent.com</AgentNode>
...     <UserName>FooUser</UserName>
...     <ScheduledDate>Weekdays</ScheduledDate>
...     <ExceptForDate>Holidays</ExceptForDate>
...     <BadPattern>ERROR</BadPattern>
...     <AutoSubmit>true</AutoSubmit>
...     <ScheduledTime>13:25:00</ScheduledTime>
...     <SubmitMethodName>Command</SubmitMethodName>
...     <RetryCount>5</RetryCount>
...     <RetryInterval>00:30</RetryInterval>
...     <Source>"C:\FooUser\baz.bat"</Source>
...   </anyType>
...  </JAMSObjects>
... ''').dags.values())[0]  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='...', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    baz_task = SSHOperator(task_id='baz', ssh_conn_id='foo.agent.com_FooUser', command='"C:\\FooUser\\baz.bat"', doc_md='Baz Job')

```
"""
from __future__ import annotations

from orbiter.file_types import FileTypeXML
from orbiter.objects import conn_id
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.ssh import OrbiterSSHOperator
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
    """Filter to top-level "Jobs" and "JAMSObjects" dictionaries

    ```pycon
    >>> basic_dag_filter({'Jobs': [{'Job': [{"@name": 'foo'}]}]})
    [{'Job': [{'@name': 'foo'}]}]
    >>> basic_dag_filter({'JAMSObjects': [{'Job': [{"@name": 'foo'}]}]})
    [{'Job': [{'@name': 'foo'}]}]

    ```
    """
    jobs = val.get('Jobs', [])
    jams_objects = val.get('JAMSObjects', [])
    return (jobs + jams_objects) or None


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    """Infer DAG ID from the file name, or use 'UNKNOWN' if not available

    ```pycon
    >>> from pathlib import Path; basic_dag_rule({'__file': Path('foo.xml')})
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='foo', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):

    ```
    """
    dag_id = file.stem if (file := val.get('__file')) else 'UNKNOWN'
    return OrbiterDAG(dag_id=dag_id, file_path=dag_id+'.py')


@task_filter_rule
def basic_task_filter(val: dict) -> list | None:
    """Filter to `Job` and `anyType` inputs

    ```pycon
    >>> basic_task_filter({'Job': [{"@name": 'foo'}]})
    [{'@name': 'foo'}]
    >>> basic_task_filter({'anyType': [{"@xsi:type": 'Job', "@name": 'foo'}]})
    [{'@xsi:type': 'Job', '@name': 'foo'}]

    ```
    """
    jobs = val.get('Job', [])
    jams_objects = [job for job in val.get('anyType', []) if job.get('@xsi:type') == 'Job']
    return (jobs + jams_objects) or None

def task_common_args(val) -> dict:
    """Common properties for all tasks

    - `Description` -> `description`
    """
    common_args = {}
    if description := val.get('Description'):
        common_args['doc_md'] = description
    return common_args

@task_rule(priority=2)
def ssh_rule(val: dict) -> OrbiterSSHOperator | None:
    """Translate input into an SSH Operator

    ```pycon
    >>> ssh_rule({'@name': 'foo', 'Source': 'echo "Hello, World!"', 'AgentNode': 'foo.agent.com'})
    foo_task = SSHOperator(task_id='foo', ssh_conn_id='foo.agent.com', command='echo "Hello, World!"')

    ```
    """
    if (
        (task_id := val.get("@name"))
        and (command := val.get("Source"))
        and (agent := val.get('AgentNode'))
    ):
        user = val.get('UserName')
        conn = f"{agent}_{user}" if user else agent
        return OrbiterSSHOperator(
            task_id=task_id,
            command=command,
            **conn_id(conn_id=conn, prefix='ssh', conn_type='ssh'),
            **task_common_args(val)
        )
    else:
        return None


@task_dependency_rule
def basic_task_dependency_rule(val: OrbiterDAG) -> list | None:
    """Translate input into a list of task dependencies"""
    return None

translation_ruleset = TranslationRuleset(
    file_type={FileTypeXML},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[ssh_rule, cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[basic_task_dependency_rule]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
