"""Tools for extracting metadata from strings"""
import re
from typing.re import Pattern

from .. import util

def extract_metadata_attributes(name, template, context):
    """Extracts metadata from name using the given template.

    Stores any extracted metadata in context.

    Params:
        name (str): The file or folder name (without extension)
        template (str|Pattern): The extraction pattern.
        context (dict): The context to update

    Returns:
        bool: True if the pattern matched, otherwise False
    """
    groups = {}

    if isinstance(template, Pattern):
        match = template.match(name)
        if not match:
            return False
        groups = match.groupdict()
    else:
        groups[template] = name

    for key, value in groups.items():
        if value:
            key = util.python_id_to_str(key)

            if key in util.METADATA_ALIASES:
                key = util.METADATA_ALIASES[key]

            util.set_nested_attr(context, key, value)

    return True

IS_PROPERTY_RE = re.compile(r'^[a-z]([-_a-zA-Z0-9\.]+)([a-zA-Z0-9])$')

def compile_regex(value):
    """Compile a regular expression from a template string

    Args:
        value (str): The value to compile

    Returns:
        Pattern: The compiled regular expression
    """
    regex = ''
    escape = False
    repl = ''
    in_repl = False
    for c in value:
        if escape:
            regex = regex + '\\' + c
            escape = False
        else:
            if c == '\\':
                escape = True
            elif c == '{':
                in_repl = True
            elif c == '}':
                in_repl = False
                if IS_PROPERTY_RE.match(repl):
                    # Replace value
                    regex = regex + '(?P<{}>{})'.format(repl, util.regex_for_property(repl))
                else:
                    regex = regex + '{' + repl + '}'
                repl = ''
            elif in_repl:
                repl = repl + c
            else:
                regex = regex + c

    # Finally, replace group ids with valid strings
    regex = re.sub(r'(?<!\\)\(\?P<([^>]+)>', _group_str_to_id, regex)
    return re.compile(regex)

def _group_str_to_id(m):
    return '(?P<{}>'.format(util.str_to_python_id(m.group(1)))
