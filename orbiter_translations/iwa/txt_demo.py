"""Demo ruleset converting ESP applications to Airflow DAGs

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

```pycon
>>> translation_ruleset.test('''
... SCHEDULE M235062_99#SCHED_FIRST1 VALIDFROM 06/30/2005 ON FR AT 0800
... :
... M235062_99#JOBMDM
...  TASKTYPE UNIX
...  SCRIPTNAME "/usr/acct/scripts/gl1"
...  STREAMLOGON acct
...  DESCRIPTION "general ledger job1"
... B236153_00#JOB_FTA
...  TASKTYPE WINDOWS
...  SCRIPTNAME "/usr/mis/scripts/bkup"
...  STREAMLOGON "^mis^"
...  FOLLOWS JOBMDM
... FOO#BAR
...  FOLLOWS JOBMDM
...  DESCRIPTION "Finished!"
... END
... ''').dags['sched_first1'] # doctest: +ELLIPSIS
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
with DAG(dag_id='sched_first1', ...):
    jobmdm_task = SSHOperator(task_id='jobmdm', doc_md='"general ledger job1"', ssh_conn_id='M235062_99', command='"/usr/acct/scripts/gl1"')
    job_fta_task = WinRMOperator(task_id='job_fta', ssh_conn_id='B236153_00', command='"/usr/mis/scripts/bkup"')
    bar_task = EmptyOperator(task_id='bar', doc_md='"Finished!"')
    jobmdm_task >> [bar_task, job_fta_task]

```
"""
from __future__ import annotations

import pendulum
from orbiter import clean_value
from orbiter.objects import conn_id
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.operators.ssh import OrbiterSSHOperator
from orbiter.objects.operators.win_rm import OrbiterWinRMOperator
from orbiter.objects.task import OrbiterTaskDependency
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_filter_rule,
    task_rule,
    task_dependency_rule,
    create_cannot_map_rule_with_task_id_fn,
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

from orbiter_translations.iwa.file_type import FileTypeIWA

@dag_filter_rule
def demo_dag_filter(val: dict) -> list | None:
    """Look for `schedule` in the input dictionary."""
    if 'schedule' in val:
        return [val]
    return None

_demo_dag_common_args_params_doc = {
    "schedule.2": "DAG.dag_id",
    "limit": "DAG.concurrency",
    "validfrom": "DAG.start_date",
    "validto": "DAG.end_date"
}
def dag_common_args(val: dict) -> dict:
    """Common arguments for all DAGs

    ```pycon
    >>> from pendulum import DateTime
    >>> dag_common_args({
    ...     'schedule': 'M235062_99#SCHED_FIRST1',
    ...     'limit': '5',
    ...     'validfrom': '06/30/2005',
    ...     'validto': '06/30/2005'
    ... }) # doctest: +NORMALIZE_WHITESPACE
    {'dag_id': 'sched_first1',
    'concurrency': 5,
    'start_date': DateTime(2005, 6, 30, 0, 0, 0, tzinfo=Timezone('UTC')),
    'end_date': DateTime(2005, 6, 30, 0, 0, 0, tzinfo=Timezone('UTC'))}

    ```
    """
    if ' ' in (schedule := val.get('schedule')):
        # TODO - VALIDFROM or etc might be on `schedule` line
        [dag_id, _] = schedule.split(" ", maxsplit=1)
    else:
        dag_id = schedule

    # remove the workstation from the jobstream id
    if '#' in dag_id:
        [_, dag_id] = dag_id.split("#", maxsplit=1)
    dag_id = clean_value(dag_id)

    common_args = {
        "dag_id": dag_id
    }

    if limit := val.get('limit'):
        common_args["concurrency"] = int(limit)
    if validfrom := val.get('validfrom'):
        common_args["start_date"] = pendulum.parse(validfrom, strict=False)
    if validto := val.get("validto"):
        common_args["end_date"] = pendulum.parse(validto, strict=False)
    return common_args


@dag_rule(params_doc=_demo_dag_common_args_params_doc)
def demo_dag_rule(val: dict) -> OrbiterDAG | None:
    """Translate input into an `OrbiterDAG`

    ```pycon
    >>> demo_dag_rule({'schedule': 'M235062_99#SCHED_FIRST1'}) # doctest: +ELLIPSIS
    from airflow import DAG...
    with DAG(dag_id='sched_first1', ...):...

    ```
    """
    if 'schedule' in val:
        common_args = dag_common_args(val)
        return OrbiterDAG(
            file_path=f"{common_args['dag_id']}.py",
            **common_args,
            doc_md="**Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
            "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
            "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
        )
    else:
        return None


@task_filter_rule
def demo_task_filter(val: dict) -> list | None:
    """Filter out extracted `jobs` - attaching jobstream params as an inner-key for future reference.

    !!! Note

        Attaches a copy of the jobstream, minus jobs, under a `jobstream` key for future reference.
    ```pycon
    >>> demo_task_filter({'schedule': 'foo', 'jobs': [{'id': 'foo'}, {'id': 'bar'}]}) # doctest: +NORMALIZE_WHITESPACE
    [{'id': 'foo', 'jobstream': {'schedule': 'foo'}},
     {'id': 'bar', 'jobstream': {'schedule': 'foo'}}]

    ```
    """
    if 'jobs' in val:
        jobs: list[dict] = val.pop('jobs')
        return [
            j | {"jobstream": val}
            for j in jobs
        ]
    return None


_demo_task_common_args_params_doc = {
    "job_id.2": "Operator.task_id",
    "description": "Operator.doc_md",
}
def demo_task_common_args(val: dict) -> dict:
    """Common properties for all tasks

    ```pycon
    >>> demo_task_common_args({'id': 'foo#bar', 'streamlogon': 'foo', 'description': 'bar'}) # doctest: +NORMALIZE_WHITESPACE
    {'task_id': 'bar',
    'doc_md': 'bar'}

    ```
    """
    if "#" in (_id := val["id"]):
        [_, task_id] = _id.split("#", maxsplit=1)
        task_id = clean_value(task_id)
    else:
        task_id = _id
    common_args = {
        "task_id": task_id,
    }
    if description := val.get('description'):
        common_args |= {"doc_md": description}

    return common_args

_demo_connection_common_args_params_doc = {
    "job_id.1": "Operator.*_conn_id",
    "jobstream.schedule.1": "Operator.*_conn_id",
    "streamlogon": "Connection.user",
}
def demo_connection_common_args(val: dict) -> dict:
    """Common connection properties for all tasks

    ```pycon
    >>> demo_connection_common_args({'id': 'foo#bar', 'streamlogon': 'foo', 'description': 'bar'}) # doctest: +NORMALIZE_WHITESPACE
    {'ssh_conn_id': 'foo',
    'orbiter_conns': {OrbiterConnection(conn_id=foo, conn_type=ssh, user=foo)}}

    ```
    """
    common_args = {}

    # Pop workstation off task_id
    if "#" in (_id := val["id"]):
        [_conn_id, _] = _id.split("#", maxsplit=1)
    elif "#" in (schedule := val.get("jobstream", {}).get("schedule", "")):
        [_conn_id, _] = schedule.split("#", maxsplit=1)
    else:
        _conn_id = None
    if _conn_id:
        common_args |= conn_id(conn_id=_conn_id, prefix="ssh", conn_type="ssh")
        if user := val.get('streamlogon'):
            conns = list(common_args['orbiter_conns'])
            conn = conns[-1]
            conn = conn.model_copy(update={"user": user})
            common_args['orbiter_conns'] = set(conns[:-1] + [conn])
    return common_args


@task_rule(priority=2, params_doc=_demo_task_common_args_params_doc | _demo_connection_common_args_params_doc | {"scriptname": "command"})
def demo_unix_script_rule(val: dict) -> OrbiterSSHOperator | None:
    """Run a script on a unix host via ssh

    ```pycon
    >>> demo_unix_script_rule(val={"id": "M235062_99#JOBMDM", "tasktype": "unix", "scriptname": "/usr/acct/scripts/gl1"})
    jobmdm_task = SSHOperator(task_id='jobmdm', ssh_conn_id='M235062_99', command='/usr/acct/scripts/gl1')

    ```
    """
    if "id" in val and val.get('tasktype', '').lower() == 'unix' and (command := val.get('scriptname')):
        return OrbiterSSHOperator(**demo_task_common_args(val), **demo_connection_common_args(val), command=command)
    return None

@task_rule(priority=2, params_doc=_demo_task_common_args_params_doc | _demo_connection_common_args_params_doc | {"docommand": "command"})
def demo_unix_command_rule(val: dict) -> OrbiterSSHOperator | None:
    """Run a command on a unix host via ssh

    ```pycon
    >>> demo_unix_command_rule(val={"id": "M235062_99#JOBMDM", "tasktype": "unix", "docommand": "echo 'hi'"})
    jobmdm_task = SSHOperator(task_id='jobmdm', ssh_conn_id='M235062_99', command="echo 'hi'")

    ```
    """
    if "id" in val and val.get('tasktype', '').lower() == 'unix' and (command := val.get('docommand')):
        return OrbiterSSHOperator(**demo_task_common_args(val), **demo_connection_common_args(val), command=command)
    return None

@task_rule(priority=2, params_doc=_demo_task_common_args_params_doc | _demo_connection_common_args_params_doc | {"scriptname": "command"})
def demo_win_script_rule(val: dict) -> OrbiterWinRMOperator | None:
    """Run a script on a windows host via winrm

    ```pycon
    >>> demo_win_script_rule(val={"id": "M235062_99#JOBMDM", "tasktype": "WINDOWS", "scriptname": "/usr/acct/scripts/gl1"})
    jobmdm_task = WinRMOperator(task_id='jobmdm', ssh_conn_id='M235062_99', command='/usr/acct/scripts/gl1')

    ```
    """
    if "id" in val and val.get('tasktype', '').lower() == 'windows' and (command := val.get('scriptname')):
        return OrbiterWinRMOperator(**demo_task_common_args(val), **demo_connection_common_args(val), command=command)
    return None

@task_rule(priority=2, params_doc=_demo_task_common_args_params_doc | _demo_connection_common_args_params_doc | {"docommand": "command"})
def demo_win_command_rule(val: dict) -> OrbiterWinRMOperator | None:
    """Run a command on a windows host via winrm

    ```pycon
    >>> demo_win_command_rule(val={"id": "M235062_99#JOBMDM", "tasktype": "windows", "docommand": "dir"})
    jobmdm_task = WinRMOperator(task_id='jobmdm', ssh_conn_id='M235062_99', command='dir')

    ```
    """
    if "id" in val and val.get('tasktype', '').lower() == 'windows' and (command := val.get('docommand')):
        return OrbiterWinRMOperator(**demo_task_common_args(val), **demo_connection_common_args(val), command=command)
    return None

@task_rule(priority=2, params_doc=_demo_task_common_args_params_doc)
def empty_task_rule(val: dict) -> OrbiterEmptyOperator | None:
    """An empty task, if there's no `task` or `tasktype` defined.

    ```pycon
    >>> empty_task_rule(val={"id": "M235062_99#JOBMDM"})
    jobmdm_task = EmptyOperator(task_id='jobmdm')

    ```
    """
    if 'id' in val and 'tasktype' not in val and 'task' not in val:
        return OrbiterEmptyOperator(**demo_task_common_args(val))
    return None


@task_dependency_rule(params_doc={"follows": "Operator.downstream"})
def demo_task_dependency_rule(val: OrbiterDAG) -> list | None:
    """Use `follows` to infer task dependencies.

    ```pycon
    >>> demo_task_dependency_rule(val=OrbiterDAG(file_path='foo.py', dag_id='foo').add_tasks({
    ...   OrbiterEmptyOperator(task_id='foo'),
    ...   OrbiterEmptyOperator(task_id='bar', orbiter_kwargs={"val": {"follows": "foo"}}),
    ... }))
    [foo >> bar]

    ```
    """
    task_dependencies = []
    for task in val.tasks.values():
        if follows := (getattr(task, 'orbiter_kwargs', {}) or {}).get("val", {}).get('follows'):
            task_dependencies.append(OrbiterTaskDependency(task_id=follows, downstream=task.task_id))
    return task_dependencies


translation_ruleset = TranslationRuleset(
    file_type={FileTypeIWA},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[demo_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[demo_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[demo_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[
        demo_unix_script_rule,
        demo_unix_command_rule,
        demo_win_script_rule,
        demo_win_command_rule,
        empty_task_rule,
        create_cannot_map_rule_with_task_id_fn(lambda v: demo_task_common_args(v)["task_id"])
    ]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[
        demo_task_dependency_rule,
    ]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
