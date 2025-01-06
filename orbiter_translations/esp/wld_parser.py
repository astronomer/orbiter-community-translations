from __future__ import annotations

import re

from orbiter.orbiter.file_types import FileType


def pattern_for_attrs(attrs: list[str]) -> re.Pattern:
    """Compiles to a pattern that will match any of the attrs passed in, as a space separated key-value pair

    Allows any number of spaces between key and value, and trims single-quotes.
    Allows word characters (letters, numbers, underscore) and periods.

    e.g. if attrs=["APPL", "JCLID"]
    - "APPL a" -> {"APPL": "a"}
    - "JCLID 'FOO.JCL'" -> {"JCLID": "FOO.JCL"}
    """
    return re.compile(
        # named capture group "k"
        r"((?P<k>"
        # match any of the attrs passed in, like ((FOO)|(BAR))
        + "|".join([f"({attr})" for attr in attrs])
        # 1 or more number of spaces, named capture group "v"
        + ") +'?(?P<v>[.\w]+)'?)",
        re.IGNORECASE,
    )


APPL_ATTRS = ["APPL", "JCLID"]
APPL_ATTR_PATTERN = pattern_for_attrs(APPL_ATTRS)

JOB_START_ATTRS = ["JOB", "NT_JOB", "LINUX_JOB"]
JOB_START_PATTERN = pattern_for_attrs(JOB_START_ATTRS)

JOB_ATTRS = ["AGENT", "CMDNAME", "RUN", "RELEASE", "SCRIPTNAME"]
JOB_ATTR_PATTERN = pattern_for_attrs(JOB_ATTRS)

JOB_END = "ENDJOB"


def from_pattern(pattern: re.Pattern, s: str) -> dict:
    r"""Extracts key-value pairs from a string using a pattern
    ```pycon
    >>> from_pattern(APPL_ATTR_PATTERN, "")

    >>> from_pattern(APPL_ATTR_PATTERN, "foo")

    >>> from_pattern(APPL_ATTR_PATTERN, "APPL ")

    >>> from_pattern(APPL_ATTR_PATTERN, "APPL a")
    {'APPL': 'a'}
    >>> from_pattern(APPL_ATTR_PATTERN, "APPL a\nJCLID 'FOO.JCL'")
    {'APPL': 'a', 'JCLID': 'FOO.JCL'}

    ```
    """
    r = {}
    if matches := pattern.finditer(s):
        for match in matches:
            x = list(match.groupdict().values())
            r |= dict(zip(x[::2], x[1::2]))
    return r or None


def load_wld(s: str) -> dict:
    r"""Load WLD file contents into a dictionary

    ```pycon
    >>> load_wld("")
    {}
    >>> load_wld("foo")
    {}
    >>> load_wld("APPL a")
    {'APPL': 'a'}
    >>> load_wld("APPL a\n        JCLID     'FOO.JCL'\n")
    {'APPL': 'a', 'JCLID': 'FOO.JCL'}
    >>> load_wld("APPL a\nJCLID b\nJOB c\nENDJOB\nNT_JOB d\n\nAGENT 'foo'\nENDJOB")
    {'APPL': 'a', 'JCLID': 'b', 'JOBS': {'c': {'JOB': 'c'}, 'd': {'NT_JOB': 'd', 'AGENT': 'foo'}}}

    ```
    """
    contents = {}
    current_job: str | None = None
    for line in s.splitlines():
        line = line.strip()

        if not line:
            continue

        if not current_job:
            if match := from_pattern(APPL_ATTR_PATTERN, line):
                contents |= match
            elif match := from_pattern(JOB_START_PATTERN, line):
                job = match[next(iter(match))]
                if "JOBS" not in contents:
                    contents["JOBS"] = {}
                contents["JOBS"][job] = match
                current_job = job

        if current_job:
            if match := from_pattern(JOB_ATTR_PATTERN, line):
                contents["JOBS"][current_job] |= match

            if JOB_END in line:
                current_job = None
                continue

    return contents


def dump_wld(obj: dict) -> str:
    """Dump a WLD file"""
    raise NotImplementedError("WLD dumping is not implemented yet.")


class FileTypeWLD(FileType):
    extension = {"WLD", None}
    load_fn = load_wld
    dump_fn = dump_wld
