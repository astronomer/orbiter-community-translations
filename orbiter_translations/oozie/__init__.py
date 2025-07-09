from __future__ import annotations

import configparser
import logging
import re
from pathlib import Path
from typing import Any


def translate_el(val):
    """Utilizes https://github.com/GoogleCloudPlatform/oozie-to-airflow/ libraries

    ```pycon
    >>> translate_el("${foo}") # doctest: +SKIP
    '{{foo}}'
    >>> translate_el("--conf x=${bar}") # doctest: +SKIP
    '--conf x={{bar}}'
    >>> translate_el("foo ${scriptPath}#${scriptName}") # doctest: +SKIP
    'foo {{scriptPath}}#{{scriptName}}'

    ```
    """
    from o2a_lib.el_parser import translate

    if any(c in val for c in ("${", "#{")):
        for match in re.finditer(r"(?P<el>\$\{[^}]+})", val):
            el = match.groupdict().get("el")
            val = val.replace(el, translate(el))
    return val


jinja_var = re.compile(r".*(?P<with_brackets>\{\{(?P<var>[a-zA-Z0-9-_]+)}}).*")


def _simplify_value_recursively(value: str, contents_dict: dict):
    r"""After it's been Expression Language-translated,
    simplify by turning {{var}} into the looked-up value of that var (recursively).
    We do this because Airflow doesn't recursively resolve Jinja variables.

    ```pycon
    >>> _simplify_value_recursively("{{key}}\n{{key2}}", {"key": "foo", "key2": "bar"})
    'foo\nbar'
    >>> _simplify_value_recursively("{{key}}", {"key": "value"})
    'value'
    >>> _simplify_value_recursively("{{key}}", {"key": "{{key2}}", "key2": "value"})
    'value'
    >>> _simplify_value_recursively("/foo/{{key}}/baz", {"key": "{{key2}}", "key2": "bar"})
    '/foo/bar/baz'

    ```
    """
    for match in (m for m in jinja_var.finditer(value) if m):
        if var_key := (match.groupdict().get("var") if match else False):
            with_brackets = match.groupdict().get("with_brackets")
            try:
                replaced = value.replace(with_brackets, contents_dict[var_key])
                return _simplify_value_recursively(replaced, contents_dict)
            except KeyError:
                if var_key not in ["MIN", "HOUR", "DAY", "MONTH", "YEAR"]:
                    logging.error(
                        f"Missing property substituting key={var_key}, not found in properties sources"
                    )
                logging.debug(f"Contents dict: {contents_dict}")
    return value


def _parse_template_file_contents(config_text: str) -> dict:
    r"""
    >>> _parse_template_file_contents("job_name=foo")
    {'job_name': 'foo'}
    >>> _parse_template_file_contents('''job_name=foo
    ... job_name=bar''')
    {'job_name': 'bar'}
    """
    # <class 'configparser.DuplicateOptionError'> - While reading from '<string>' [line 24]: option 'job_name' in section 'default' already exists
    config = configparser.ConfigParser(strict=False)
    contents = f"[default]\n{config_text}"
    config.read_string(contents)
    return dict(config["default"])


def parse_template_file(file: Path) -> dict:
    """Parse an oozie .properties file with Python configparser"""
    # This will work in PY3.13
    # ```
    # import configparser
    # configparser.ConfigParser(allow_unnamed_sections=True).read(Path(...))
    # ```
    # https://docs.python.org/3.13/whatsnew/3.13.html#configparser
    # but that's too new, so using something else for now https://stackoverflow.com/a/26859985
    config_text = file.read_text()
    contents_dict = _parse_template_file_contents(config_text)
    for k, v in contents_dict.items():
        contents_dict[k] = translate_el(v)
    for k, v in contents_dict.items():
        contents_dict[k] = _simplify_value_recursively(v, contents_dict)
    return contents_dict


def get_coordinator_configuration_blocks(val: dict) -> list:
    return (
        val.get("coordinator-app", [{}])[0]
        .get("action", [{}])[0]
        .get("workflow", [{}])[0]
        .get("configuration", [])
    )


def load_properties(
        coordinator_file: Path | None = None, configuration_blocks: list = None
) -> dict:
    """
    Search for a .properties file in the same directory as the workflow file,
    and combine it with any other properties, then return it
    """
    if configuration_blocks is None:
        configuration_blocks = []

    env_properties = {}

    if coordinator_file and coordinator_file.exists():
        files = [
            f for f in coordinator_file.parent.iterdir() if ".properties" in f.name
        ]
        if not files:
            logging.debug(f"No .properties files found in {coordinator_file.parent}")
        for file in files:
            env_properties |= parse_template_file(file)
    else:
        logging.debug(
            "__file property not set, and required for `load_properties` - skipping"
        )

    # Extract properties from the <action><workflow><configuration><property>... blocks
    for configuration in configuration_blocks:
        for prop in configuration.get("property", []):
            env_properties[prop["name"]] = substitute_properties_recursively(
                prop["value"], env_properties
            )

    return env_properties


def substitute_properties_recursively(val: dict | list | Any, properties: dict):
    """Iterate through lists or dicts and substitute any Jinja variables with their values

    >>> substitute_properties_recursively({'foo': '{{bar}}'}, {'bar': 'baz'})
    {'foo': 'baz'}
    >>> substitute_properties_recursively({'foo': {'bar': {'baz': '{{bop}}'}}}, {'bop': 'qux'})
    {'foo': {'bar': {'baz': 'qux'}}}
    >>> substitute_properties_recursively({'foo': ['{{bar}}']}, {'bar': 'baz'})
    {'foo': ['baz']}
    >>> substitute_properties_recursively(['{{bar}}'], {'bar': 'baz'})
    ['baz']
    >>> substitute_properties_recursively('foo', {'bar': 'baz'})
    'foo'
    """
    if isinstance(val, list):
        # Iterate over a list
        return [substitute_properties_recursively(v, properties) for v in val]
    elif isinstance(val, dict):
        # Iterate through a dict
        for k, v in val.items():
            val[k] = substitute_properties_recursively(v, properties)
        return val
    elif isinstance(val, str) and any(c in val for c in ("${", "#{")):
        # Translate Oozie/Java EL, then substitute properties
        return _simplify_value_recursively(translate_el(val), properties)
    elif isinstance(val, str) and "{{" in val:
        # Substitute Jinja properties
        return _simplify_value_recursively(val, properties)
    else:
        return val
