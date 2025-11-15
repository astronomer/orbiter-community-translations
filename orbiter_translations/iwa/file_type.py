from __future__ import annotations

from typing import ClassVar, Set, Callable

from loguru import logger
from orbiter.file_types import xmltodict_parse, FileType
from orbiter_parsers.file_types import unimplemented_dump

JOB_STREAM_ATTRS = [
    "at",
    "carryforward",
    'comment',
    "confirmed",
    "critical",
    'deadline',
    'description',
    'draft',
    'end',
    'every',
    'except',
    'fdignore',
    'follows',
    'freedays',
    'interval',
    'job',
    'join',
    'jsuntil',
    'keyjob',
    'keysched',
    'limit',
    'matching',
    'maxdur',
    'mindur',
    'needs',
    'nop',
    'on',
    'onlate',
    'onoverlap',
    'opens',
    'onuntil',
    'outcond',
    'priority',
    'prompt',
    'runcycle',
    'schedule',
    'schedtime',
    'statisticstype',
    'startcond',
    'timezone',
    'until',
    'validfrom',
    'validto',
    'vartable'
]
JOB_ATTRS = [
    'at',
    'critical',
    'deadline',
    'description',
    'docommand',
    'every',
    'follows',
    'interactive',
    'join',
    'keyjob',
    'maxdur',
    'mindur',
    'needs',
    'nop',
    'opens',
    'outputcond',
    'priority',
    'prompt',
    'recovery',
    'scriptname',
    'statisticstype',
    'streamlogon',
    'succoutputcond',
    'task',
    'tasktype',
    'until',
]


def iwa_loads(s: str) -> list[dict]:
    """Parses IWA jobstreams into a dictionary.

    ```pycon
    >>> iwa_loads('''
    ... SCHEDULE M235062_99#SCHED_FIRST1 VALIDFROM 06/30/2005
    ... ON RUNCYCLE SCHED1_PREDSIMPLE VALIDFROM 07/18/2005 "FREQ=DAILY;INTERVAL=1"
    ...    ( AT 1010 )
    ... ON RUNCYCLE SCHED1_PRED_SIMPLE VALIDFROM 07/18/2005 "FREQ=DAILY;INTERVAL=1"
    ... CARRYFORWARD
    ... :
    ... M235062_99#JOBMDM
    ...  SCRIPTNAME "/usr/acct/scripts/gl1"
    ...  STREAMLOGON acct
    ...  DESCRIPTION "general ledger job1"
    ... B236153_00#JOB_FTA
    ...  SCRIPTNAME "/usr/mis/scripts/bkup"
    ...  STREAMLOGON "^mis^"
    ...  FOLLOWS JOBMDM
    ...  RECOVERY CONTINUE AFTER recjob1
    ...  PRIORITY 30
    ...  NEEDS 16 M235062_99#JOBSLOTS
    ...  PROMPT PRMT3
    ... END''') # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    [{'schedule': 'M235062_99#SCHED_FIRST1 VALIDFROM 06/30/2005',
    'on': 'RUNCYCLE SCHED1_PRED_SIMPLE VALIDFROM 07/18/2005 "FREQ=DAILY;INTERVAL=1"',
    'carryforward': None,
    'jobs': [{'id': 'M235062_99#JOBMDM',
    'scriptname': '"/usr/acct/scripts/gl1"',
    'streamlogon': 'acct',
    'description': '"general ledger job1"'},
    {'id': 'B236153_00#JOB_FTA',
    'scriptname': '"/usr/mis/scripts/bkup"',
    'streamlogon': '"^mis^"',
    'follows': 'JOBMDM',
    'recovery': 'CONTINUE AFTER recjob1',
    'priority': '30',
    'needs': '16 M235062_99#JOBSLOTS',
    'prompt': 'PRMT3'}]}]
    >>> iwa_loads('''
    ... SCHEDULE MACHINE#/FOO_BAR/BAZ_FW_CHECK1
    ... DESCRIPTION "XXXXXXXXXX yyyy."
    ... ON REQUEST
    ... MATCHING PREVIOUS
    ... :
    ... MACHINE#/FOO_BAR/BAZ_FW_CHECK_TEST
    ...  DOCOMMAND "SLEEP 100"
    ...  STREAMLOGON XXXXX
    ...  DESCRIPTION "File Watcher for Check file"
    ...  TASKTYPE UNIX
    ...  RECOVERY STOP
    ...  AT 0030 TIMEZONE US/Pacific UNTIL 2359 TIMEZONE US/Pacific
    ...  EVERY 0005
    ... OPENS MACHINE#"/data/SFTP/XXXXXXXXX/FOO001_2_7/OUT/*.txt" (f %p)
    ...
    ... END
    ... ''') # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    [{'schedule': 'MACHINE#/FOO_BAR/BAZ_FW_CHECK1',
    'description': '"XXXXXXXXXX yyyy."',
    'on': 'REQUEST',
    'matching': 'PREVIOUS',
    'jobs': [{'id': 'MACHINE#/FOO_BAR/BAZ_FW_CHECK_TEST',
        'docommand': '"SLEEP 100"',
        'streamlogon': 'XXXXX',
        'description': '"File Watcher for Check file"',
        'tasktype': 'UNIX',
        'recovery': 'STOP',
        'at': '0030 TIMEZONE US/Pacific UNTIL 2359 TIMEZONE US/Pacific',
        'every': '0005',
        'opens': 'MACHINE#"/data/SFTP/XXXXXXXXX/FOO001_2_7/OUT/*.txt" (f %p)'}]}]

    ```
    """
    jobs: list[dict] = []

    # in_jobstream: bool = False
    in_job: bool = False

    current_jobstream: dict | None = None
    current_jobstream_attr: str | None = None

    next_line_jobdef: bool = False
    current_job: dict | None = None
    # current_job_attr: str | None = None

    current_task_definition: str | None = None

    for line in [line for line in s.splitlines() if line.strip() and not line.lstrip()[0] == '#']:
        line = line.strip()

        ####### SPECIAL CASES #######
        # We have seen "task", consume xml until </jsdl:jobDefinition>
        if current_task_definition is not None:
            # consume
            current_task_definition += f"\n{line}"
            if line.rstrip().endswith('</jsdl:jobDefinition>'):
                # set
                current_job['task'] = xmltodict_parse(current_task_definition.strip())

                # reset
                current_task_definition = None
            continue

        # skip empty lines and comments
        if not line or line.startswith('#'):
            continue

        # : separator - job def is next
        if line == ':':
            # setup
            next_line_jobdef = True
            if 'jobs' not in current_jobstream:
                current_jobstream['jobs'] = []
            continue

        # job definition line (we saw : on the last line)
        if next_line_jobdef:
            # setup
            in_job = True
            current_jobstream["jobs"].append({'id': line})
            current_job = current_jobstream["jobs"][-1]

            # reset
            next_line_jobdef = False
            continue

        # end of a jobstream definition
        if line.lower() == 'end':
            # reset
            # in_jobstream = False
            in_job = False

            current_jobstream_attr = None
            current_job = None
            # current_job_attr = None
            continue

        ####### NORMAL CASES #######
        try:
            [key, value] = line.split(" ", maxsplit=1)
            key = key.lower()
        except ValueError as e:
            # there wasn't at least one space

            # but it is in the attrs list
            if line.lower() in JOB_STREAM_ATTRS + JOB_ATTRS:
                key = line.lower()
                value = None

            # it must be a job name
            elif in_job:
                # set
                in_job = True
                current_jobstream_attr = None

                current_jobstream["jobs"].append({"id": line})
                current_job = current_jobstream["jobs"][-1]
                continue

            # else? shrug
            else:
                logger.error(f"Unable to parse line: {line}")
                logger.exception(e)
                continue

        ####### JOB CASES #######
        # check for a job attr
        if key in JOB_ATTRS and current_job:
            # set
            if key == "task":
                current_task_definition = ""
            else:
                current_job[key] = value
            continue

        ####### JOBSTREAM CASES #######
        # found 'schedule' - start of a new jobstream
        elif key == 'schedule':
            # in_jobstream = True
            # add a new job, set the pointer
            jobs.append({key: value})
            current_jobstream = jobs[-1]

            # reset
            current_jobstream_attr = None
            current_job = None
            # current_job_attr = None
            continue

        # check for jobstream attr
        elif key in JOB_STREAM_ATTRS:
            # set
            # in_jobstream = True
            current_jobstream_attr = key
            current_jobstream[current_jobstream_attr] = value
            continue

        else: # continuation, such as ON RUNCYCLE ... \n ( AT 1010 )
            in_job = False
            current_jobstream[current_jobstream_attr] += f" {line}"
            continue

    return jobs


class FileTypeIWA(FileType):
    """IWA File Type

    :param extension: TXT, IWA
    :type extension: Set[str]
    :param load_fn: custom IWA loading function
    :type load_fn: Callable[[str], dict]
    :param dump_fn: IWA dumping function not yet implemented, raises an error
    :type dump_fn: Callable[[dict], str]
    """

    extension: ClassVar[Set[str]] = {"TXT", "IWA"}
    load_fn: ClassVar[Callable[[str], dict | list[dict]]] = iwa_loads
    dump_fn: ClassVar[Callable[[dict], str]] = unimplemented_dump
