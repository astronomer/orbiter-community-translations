r"""Demonstration of translating AutoSys JIL files into Airflow DAGs.

Contact Astronomer @ https://astronomer.io/contact for access to our full translation.

```pycon
>>> translation_ruleset.test('''
... insert_job: foo.job
... job_type: CMD
... command: "C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" "arg3"
... machine: bar
... owner: foo@bar.com
... description: "Foo Job"
... ''').dags["foo_job"]
... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='foo_job', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, default_args={'owner': 'foo@bar.com'}, doc_md=...):
    foo_job_task = SSHOperator(task_id='foo_job', ssh_conn_id='bar', command='"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" "arg3"', doc='Foo Job')

```
"""

from __future__ import annotations

from orbiter.file_types import FileTypeJIL
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.ssh import OrbiterSSHOperator
from orbiter.objects import conn_id
from orbiter.rules import (
    dag_filter_rule,
    dag_rule,
    task_filter_rule,
    task_rule,
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


def clean_to_airflow_valid_id(s: str) -> str:
    """Replace invalid characters in a string to make it a valid Airflow ID."""
    return s.replace(".", "_").lower()


def task_common_args(val: dict) -> dict:
    """
    Generates common task arguments including task ID, documentation, and callbacks.

    - `insert_job` -> `task_id`
    - `description` -> `doc`

    ```pycon
    >>> task_common_args({'insert_job': 'foo.job', 'description': 'Foo Job'})
    {'task_id': 'foo_job', 'doc': 'Foo Job'}

    ```
    :param val: A dictionary containing task-related information
    :type val: dict
    :return: Common task parameters
    :rtype: dict
    """
    task_id = clean_to_airflow_valid_id(val.get("insert_job", "UNKNOWN"))
    params = {"task_id": task_id}
    if description := val.get("description"):
        params["doc"] = description
    return params


@dag_filter_rule
def basic_filter_rule(val: dict) -> list[dict] | None:
    r"""Basic filter rule to extract the `DAG` from an AutoSys dump.
    ```pycon
    >>> basic_filter_rule({
    ...     'jobs': [
    ...         {
    ...             'insert_job': 'foo.job',
    ...             'job_type': 'CMD',
    ...             'machine': 'machine_id',
    ...             'command': '"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd"'
    ...         }
    ...     ]
    ... })
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    [{'DAG':
        [{'insert_job': 'foo.job',
        'job_type': 'CMD',
        'machine': 'machine_id',
        'command': '"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd"'}]}]

    ```
    """
    return [{"DAG": val["jobs"]}]


@dag_rule
def basic_dag_rule(val: dict) -> OrbiterDAG | None:
    r"""Map AutoSysWorkflow into a DAG

    - task `owner` -> DAG `owner`
    - task `insert_job` ID -> DAG ID

    ```pycon
    >>> basic_dag_rule({
    ...   '__file': 'workflow.jil',
    ...   'DAG': [{
    ...     'command': '"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" '
    ...     '"arg3"',
    ...     'group': 'group_name',
    ...     'insert_job': 'foo.job',
    ...     'job_type': 'CMD',
    ...     'machine': 'machine_id',
    ...     'owner': 'foo@bar.net'
    ...   }]
    ... })
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    from airflow import DAG
    from pendulum import DateTime, Timezone
    with DAG(dag_id='foo_job', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, default_args={'owner': 'foo@bar.net'}, doc_md=...):

    ```
    """
    if dag := val.get("DAG"):
        default_args = {}
        owners = ",".join({owner for task in dag if (owner := task.get("owner"))})
        if owners:
            default_args["owner"] = owners
        if default_args:
            default_args = {"default_args": default_args}

        dag_id = clean_to_airflow_valid_id(val["DAG"][0]["insert_job"])
        return OrbiterDAG(
            dag_id=dag_id,
            file_path=dag_id + ".py",
            doc_md="**Created via [Orbiter](https://astronomer.github.io/orbiter) w/ Demo Translation Ruleset**.\n"
                   "Contact Astronomer @ [humans@astronomer.io](mailto:humans@astronomer.io) "
                   "or at [astronomer.io/contact](https://www.astronomer.io/contact/) for more!",
            **default_args,
        )


@task_rule(priority=1)
def ssh_command_rule(val) -> OrbiterSSHOperator | None:
    r"""Maps a workflow of job_type 'CMD' into a SSHOperator

    - `command` becomes the command to run
    - `machine` becomes the ssh_conn_id

    ```pycon
    >>> ssh_command_rule({
    ...     'insert_job': 'foo_bar',
    ...     'job_type': 'CMD',
    ...     'command': '"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" "arg3"',
    ...     'machine': 'machine_id',
    ...     'owner': 'foo@bar.net',
    ...     'group': 'group_name',
    ... })
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    foo_bar_task = SSHOperator(task_id='foo_bar',
    ssh_conn_id='machine_id',
    command='"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" "arg3"')

    ```
    """
    if val.get("job_type") == "CMD":
        return OrbiterSSHOperator(
            command=val["command"],
            **conn_id(conn_id=val["machine"], prefix="ssh", conn_type="ssh"),
            **task_common_args(val),
        )


@task_filter_rule
def basic_task_filter(val: dict) -> list[dict] | None:
    r"""Returns the list of dictionaries that can be processed by the `@task_rules`
    ```pycon
    >>> basic_task_filter(
    ... {
    ...  'DAG': [{
    ...  'command': '"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" '
    ...  '"arg3"',
    ...  'group': 'group_name',
    ...  'insert_job': 'foo.job',
    ...  'job_type': 'CMD',
    ...  'machine': 'machine_id',
    ... 'owner': 'foo@bar.net'}]})
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    [{'command': '"C:\\ldhe\\cxl\\TidalDB\\startApp.cmd" "arg1" "arg2" "arg3"',
    'group': 'group_name',
    'insert_job': 'foo.job',
    'job_type': 'CMD',
    'machine': 'machine_id',
    'owner': 'foo@bar.net'}]

    ```
    """
    return val["DAG"]


translation_ruleset = TranslationRuleset(
    file_type={FileTypeJIL},
    dag_filter_ruleset=DAGFilterRuleset(ruleset=[basic_filter_rule]),
    dag_ruleset=DAGRuleset(ruleset=[basic_dag_rule]),
    task_filter_ruleset=TaskFilterRuleset(ruleset=[basic_task_filter]),
    task_ruleset=TaskRuleset(ruleset=[ssh_command_rule, cannot_map_rule]),
    task_dependency_ruleset=TaskDependencyRuleset(ruleset=[]),
    post_processing_ruleset=PostProcessingRuleset(ruleset=[]),
    translate_fn=translate,
)


if __name__ == "__main__":
    import doctest

    doctest.testmod(optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
