from __future__ import annotations

import re

from orbiter.file_types import FileType


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
        + r") +'?(?P<v>[.:,/\w()\\]+)'?)",
        re.IGNORECASE,
    )


APPL_ATTRS = ["JCLID"]
APPL_ATTR_PATTERN = pattern_for_attrs(APPL_ATTRS)

APPL_START_ATTRS = ["APPL"]
APPL_START_ATTRS_PATTERN = pattern_for_attrs(APPL_START_ATTRS)

JOB_START_ATTRS = ["JOB", "NT_JOB", "LINUX_JOB"]
JOB_START_PATTERN = pattern_for_attrs(JOB_START_ATTRS)

JOB_ATTRS = ["AGENT", "CMDNAME", "RUN", "RELEASE", "SCRIPTNAME"]
JOB_ATTR_PATTERN = pattern_for_attrs(JOB_ATTRS)

JOB_END = "ENDJOB"


def from_pattern(pattern: re.Pattern, s: str) -> dict | None:
    r"""Extracts key-value pairs from a string using a pattern
    ```pycon
    >>> from_pattern(APPL_ATTR_PATTERN, "")  # needs to match pattern

    >>> from_pattern(APPL_ATTR_PATTERN, "foo")  # needs to match pattern

    >>> from_pattern(APPL_START_ATTRS_PATTERN, "APPL ")  # needs key and value

    >>> from_pattern(APPL_START_ATTRS_PATTERN, "APPL a")  # a match makes a dict
    {'APPL': 'a'}
    >>> from_pattern(JOB_ATTR_PATTERN, "AGENT a\nRELEASE b\n")  # multiple matches make multiple entries
    {'AGENT': 'a', 'RELEASE': 'b'}

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
    >>> load_wld("")  # needs to match something
    {}
    >>> load_wld("foo")  # needs to match something
    {}
    >>> load_wld("APPL a")  # an application creates an entry
    {'a': {'APPL': 'a'}}
    >>> load_wld("APPL a\n        JCLID     'FOO.JCL'\n")  # inner-line whitespace doesn't matter
    {'a': {'APPL': 'a', 'JCLID': 'FOO.JCL'}}
    >>> load_wld("APPL a\nJCLID b\nJOB c\nENDJOB\nNT_JOB d\n\nAGENT 'foo'\nENDJOB")  # jobs get nested
    {'a': {'APPL': 'a', 'JCLID': 'b', 'c': {'JOB': 'c'}, 'd': {'NT_JOB': 'd', 'AGENT': 'foo'}}}
    >>> load_wld("APPL a\nJCLID 'FOO.JCL'\nAPPL b\nJCLID 'FOO.JCL'")  # multiple applications get multiple entries
    {'a': {'APPL': 'a', 'JCLID': 'FOO.JCL'}, 'b': {'APPL': 'b', 'JCLID': 'FOO.JCL'}}

    ```
    """
    contents = {}
    current_appl: str | None = None
    current_job: str | None = None
    for line in s.splitlines():
        line = line.strip()

        # if the line isn't empty
        if line:
            # look for an application start, and set the pointer if we find one
            if match := from_pattern(APPL_START_ATTRS_PATTERN, line):
                current_appl = match[next(iter(match))]
                if current_appl not in contents:
                    contents[current_appl] = {}
                contents[current_appl] |= match
                # initialize it, if we found it
                # e.g. APPL a ->  {"a": {"APPL": "a", ...}}

            # look for application properties, if we know what application already
            elif current_appl and (match := from_pattern(APPL_ATTR_PATTERN, line)):
                contents[current_appl] |= match

            # look for a job start, if we aren't already in one, and set the pointer if we find one
            elif (
                current_appl
                and not current_job
                and (match := from_pattern(JOB_START_PATTERN, line))
            ):
                current_job = match[next(iter(match))]
                if current_job not in contents[current_appl]:
                    contents[current_appl][current_job] = {}
                contents[current_appl][current_job] = match

            # look for job attrs if we are in one
            elif (
                current_appl
                and current_job
                and (match := from_pattern(JOB_ATTR_PATTERN, line))
            ):
                contents[current_appl][current_job] |= match

            # look for a job end
            elif JOB_END in line:
                current_job = None
    return contents


def dump_wld(obj: dict) -> str:
    """Dump a WLD file"""
    raise NotImplementedError("WLD dumping is not implemented yet.")


class FileTypeWLD(FileType):
    extension = {"TXT"}
    load_fn = load_wld
    dump_fn = dump_wld
