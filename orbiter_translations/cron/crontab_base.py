"""Ruleset to migrate CRON entries to DAGs containing an BashOperator.

Export CRON definitions via `crontab -l`, or by exporting from `/etc/cron.d` and save the contents in a directory as `<filename>.cron`

```pycon
>>> _ = translation_ruleset.test('''MAILTO=root@foo.com
... * * * * * user_a touch /tmp/file
... 0-30/5 30 9 ? MON-FRI root curl https://google.com/-/_/d) 'e f g'
... '''); _.dags['cron_0']; print('---'); _.dags['cron_1']
from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='cron_0', schedule='* * * * *', start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    cron_task = BashOperator(task_id='cron_task', bash_command='touch /tmp/file', email='root@foo.com', email_on_failure=True)
---
from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='cron_1', schedule='0-30/5 30 9 ? MON-FRI', start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    cron_task = BashOperator(task_id='cron_task', bash_command="curl https://google.com/-/_/d) 'e f g'", email='root@foo.com', email_on_failure=True)

```
"""
from __future__ import annotations

from copy import copy

from orbiter.objects import OrbiterConnection, OrbiterEnvVar
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.bash import OrbiterBashOperator
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

from orbiter_translations.cron import FileTypeCRON


@dag_filter_rule
def cron_dag_filter(val: dict) -> list | None:
    """Reshape input - combining any env vars with the cron line

    ```pycon
    >>> cron_dag_filter({"env": {"FOO": "bar"}, "cron": [{"schedule": "* * * * *", "command": "/bin/bash"}]})
    [{'i': 0, 'schedule': '* * * * *', 'command': '/bin/bash', 'FOO': 'bar'}]

    ```
    """
    return [{'i': i} | c | val.get('env', {}) for i, c in enumerate(val.get("cron", []))]


@dag_rule
def cron_dag_rule(val: dict) -> OrbiterDAG | None:
    """Transform CRON entries into a DAG

    ```pycon
    >>> cron_dag_rule(val={"i": 0, "schedule": "* * * * *", "command": "/bin/bash"})
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='cron_0', schedule='* * * * *', start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):

    ```
    """
    if (index := val.get('i', None)) is not None and (schedule := val.get('schedule')):
        dag_id=f"cron_{index}"
        # noinspection PyUnboundLocalVariable
        return OrbiterDAG(dag_id=dag_id, file_path=f"{dag_id}.py", schedule=schedule)


@task_filter_rule
def cron_task_filter(val: dict) -> list | None:
    """Tasks will just be the command to execute, so pass it through"""
    return [val]

def task_common_args(val: dict) -> dict:
    """Common arguments for all tasks

    - `MAILTO` adds an email user, and sends an email on failure
    - `env` is a dictionary of environment variables to set
    """
    args = {}
    if mailto := val.pop('MAILTO', None):
        args['email'] = mailto
        args['email_on_failure'] = True
        args['orbiter_env_vars'] = {OrbiterEnvVar(key='AIRFLOW__EMAIL__EMAIL_CONN_ID', value="smtp_default")}
        args['orbiter_conns'] = {OrbiterConnection(conn_id="smtp_default")}

    if env := val.get('env', {}):
        args['env'] = env

    return args


@task_rule(priority=2)
def cron_task_rule(val: dict) -> OrbiterBashOperator | None:
    """Turn a cron command into a BashOperator

    ```pycon
    >>> cron_task_rule(val={"i": 0, "schedule": "* * * * *", "command": "/bin/bash", "FOO": "bar", "MAILTO": "user@foo.com"})
    cron_task = BashOperator(task_id='cron_task', bash_command='/bin/bash', email='user@foo.com', email_on_failure=True)
    >>> cron_task_rule(val={"i": 0, "schedule": "* * * * *", "command": "/bin/bash"})
    cron_task = BashOperator(task_id='cron_task', bash_command='/bin/bash')

    ```
    """
    val = copy(val)
    if command := val.pop('command', None):
        val.pop('i', None)
        val.pop('schedule', None)
        val.pop('user', None)

        # noinspection PyArgumentList,PyTypeChecker
        return OrbiterBashOperator(
            task_id="cron_task",
            bash_command=command,
            **task_common_args(val)
        )


translation_ruleset = TranslationRuleset(
    file_type={FileTypeCRON},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[cron_dag_filter]),
    dag_ruleset=DAGRuleset(ruleset=[cron_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[cron_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[cron_task_rule, cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
)
