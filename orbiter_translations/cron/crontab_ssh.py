"""Ruleset to migrate CRON entries to DAGs containing an SSH Operator, to be executed against a remote agent.

```pycon
>>> _ = translation_ruleset.test('''MAILTO=root@foo.com
... * * * * * user_a touch /tmp/file
... 0-30/5 30 9 ? MON-FRI root curl https://google.com/-/_/d) 'e f g'
... '''); _.dags['cron_0']; print('---'); _.dags['cron_1']
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='cron_0', schedule='* * * * *', start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    cron_task = SSHOperator(task_id='cron_task', ssh_conn_id='user_a', command='touch /tmp/file', email='root@foo.com', email_on_failure=True)
---
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='cron_1', schedule='0-30/5 30 9 ? MON-FRI', start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    cron_task = SSHOperator(task_id='cron_task', ssh_conn_id='root', command="curl https://google.com/-/_/d) 'e f g'", email='root@foo.com', email_on_failure=True)

```
"""
from __future__ import annotations

from copy import copy, deepcopy

from orbiter.objects import conn_id
from orbiter.objects.operators.ssh import OrbiterSSHOperator
from orbiter.rules import task_rule

from orbiter_translations.cron.crontab_base import translation_ruleset, task_common_args

translation_ruleset = deepcopy(translation_ruleset)

@task_rule(priority=99)
def ssh_agent_rule(val: dict) -> OrbiterSSHOperator | None:
    """Turn a cron command into a SSHOperator - executing on a remote agent

    ```pycon
    >>> ssh_agent_rule(val={"i": 0, "schedule": "* * * * *", "user": "foo", "command": "/bin/bash", "FOO": "bar", "MAILTO": "user@foo.com"})
    cron_task = SSHOperator(task_id='cron_task', ssh_conn_id='foo', command='/bin/bash', email='user@foo.com', email_on_failure=True)
    >>> ssh_agent_rule(val={"i": 0, "schedule": "* * * * *", "user": "foo", "command": "/bin/bash"})
    cron_task = SSHOperator(task_id='cron_task', ssh_conn_id='foo', command='/bin/bash')

    ```
    """
    val = copy(val)
    if (command := val.pop('command', None)) and (user := val.pop('user', '')):
        val.pop('i', None)
        val.pop('schedule', None)

        common_args = task_common_args(val)
        conn_args = conn_id(user, "ssh", "ssh")
        if 'orbiter_conns' in common_args:
            common_args['orbiter_conns'] |= conn_args.pop('orbiter_conns')
        common_args |= conn_args

        # noinspection PyTypeChecker
        return OrbiterSSHOperator(
            task_id="cron_task",
            command=command,
            **common_args
        )

translation_ruleset.task_ruleset.ruleset.extend([ssh_agent_rule])
