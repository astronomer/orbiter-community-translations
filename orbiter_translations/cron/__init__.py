from __future__ import annotations

import re
from typing import ClassVar, Set, Callable

from orbiter.file_types import FileType

env_pattern = re.compile(r"^(?P<key>[A-Z_]+)=(?P<value>.*)$")
cron_pattern = re.compile(
    r"(?P<schedule>("
        r"@(annually|yearly|monthly|weekly|daily|hourly|reboot)"  # predefined schedules
    r")|("
        r"@every (\d+(ns|us|µs|ms|s|m|h))+"  # 'every 4h', etc
    r")|("
        r"(((\d+,)+\d+|(\d+(\/|-|#)\d+)|\d+L?|\*(\/\d+)?|L(-\d+)?|\?|[A-Z]{3}(-[A-Z]{3})?) ?){5,7}"  # normal cronstrings
    r"))\s+"
    r"(?P<user>\w+)?\s*"
    r"(?P<command>.*)"
)

def maybe_extract_env_pattern(s: str) -> dict | None:
    """Extract environment variables from a string.

    ```pycon
    >>> maybe_extract_env_pattern("FOO=bar")
    {'FOO': 'bar'}
    >>> maybe_extract_env_pattern("# SOME=comment")

    >>> maybe_extract_env_pattern("COMPLEX='string example 123 _-@#$%^&*()'")
    {'COMPLEX': "'string example 123 _-@#$%^&*()'"}

    ```
    """
    return dict(env_pattern.findall(s)) or None

def maybe_extract_cron_pattern(s: str) -> dict | None:
    """Extract CRON patterns from a string.

    Ref: https://stackoverflow.com/questions/14203122/create-a-regular-expression-for-cron-statement

    ```pycon
    >>> maybe_extract_cron_pattern("1 2 3 4 5 /bin/bash")
    {'schedule': '1 2 3 4 5', 'user': None, 'command': '/bin/bash'}
    >>> maybe_extract_cron_pattern("*/5 * * * * root /bin/bash a b c d 'e f g'")
    {'schedule': '*/5 * * * *', 'user': 'root', 'command': "/bin/bash a b c d 'e f g'"}
    >>> maybe_extract_cron_pattern("0-30/5 30 9 ? MON-FRI root /bin/bash a b c '(https://google.com/-/_/d)' 'e f g'")
    {'schedule': '0-30/5 30 9 ? MON-FRI', 'user': 'root', 'command': "/bin/bash a b c '(https://google.com/-/_/d)' 'e f g'"}
    >>> maybe_extract_cron_pattern("# SOME=comment")
    >>> maybe_extract_cron_pattern("FOO=bar")

    ```
    """
    return (match.groupdict() if (match := cron_pattern.match(s)) else None) or None


def load_cron(s: str) -> dict:
    """Load CRON files

    ```pycon
    >>> load_cron("FOO=bar\\n1 2 3 4 5 root /bin/bash")
    {'env': {'FOO': 'bar'}, 'cron': [{'schedule': '1 2 3 4 5', 'user': 'root', 'command': '/bin/bash'}]}
    >>> load_cron("FOO=bar\\n1 2 3 4 5 root /bin/bash\\n# SOME=comment")
    {'env': {'FOO': 'bar'}, 'cron': [{'schedule': '1 2 3 4 5', 'user': 'root', 'command': '/bin/bash'}]}
    >>> load_cron("FOO=bar\\n1 2 3 4 5 root /bin/bash\\n# SOME=comment\\nCOMPLEX='string example 123 _-@#$%^&*()'")
    {'env': {'FOO': 'bar', 'COMPLEX': "'string example 123 _-@#$%^&*()'"}, 'cron': [{'schedule': '1 2 3 4 5', 'user': 'root', 'command': '/bin/bash'}]}

    ```
    """
    r = {}
    for line in s.splitlines():
        if env := maybe_extract_env_pattern(line):
            if 'env' not in r:
                r['env'] = {}
            r['env'] |= env
        if cron := maybe_extract_cron_pattern(line):
            if 'cron' not in r:
                r['cron'] = []
            r['cron'].append(cron)
    return r

def dump_cron(d: dict) -> str:
    raise NotImplementedError()


class FileTypeCRON(FileType):
    """A FileType for CRON files."""

    extension: ClassVar[Set[str]] = {"CRON"}
    load_fn: ClassVar[Callable[[str], dict]] = load_cron
    dump_fn: ClassVar[Callable[[dict], str]] = dump_cron
