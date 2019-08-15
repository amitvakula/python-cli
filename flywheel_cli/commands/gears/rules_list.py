"""Provides the 'gear-rules list' command"""
import sys

from ...sdk_impl import create_flywheel_client
from ...exchange import gear_short_str
from ...util import Bunch


def add_command(subparsers):
    parser = subparsers.add_parser('list', help='List rules, with a few options to filter')

    parser.add_argument('-p', '--project', action='store_true', help='Show project rules as well')
    parser.add_argument('--pinned', action='store_true', help='Show only pinned rules')

    parser.add_argument('--json', action='store_true', help='Print output in JSON')

    parser.set_defaults(func=list_rules)
    parser.set_defaults(parser=parser)

    return parser


def list_rules(args):
    """Print a list of rules that match a given critera"""
    fw = create_flywheel_client()

    # First retrieve the list of gears for cross-reference
    gears = {}
    for gear in fw.get_all_gears(all_versions=True, include_invalid=True):
        gears[gear.id] = gear

    # Iterate over the rules, either printing them or add them to
    # a dictionary for json export
    rules = {}
    projects = {}
    for project, rule in _iter_rules(fw, args):
        projects[project.id] = project
        if args.json:
            rules[rule.id] = rule
        else:
            print(_rule_short_str(project, gears, rule))

    if args.json:
        # Serialize to JSON, if specified
        import json
        export_json = fw.api_client.sanitize_for_serialization({
            'gears': gears,
            'projects': projects,
            'rules': rules
        })
        print(json.dumps(export_json, indent=2))


def _iter_rules(fw, args):
    """Generator that yields each rule that passes the filters specified in args"""
    # Create an attribute bunch so that site acts like a project
    site = Bunch(id='site', label='Site')

    # Iterate over site rules
    for rule in fw.get_site_rules():
        if _filter_rule(args, rule):
            yield (site, rule)

    # Optionally iterate over project rules
    if args.project:
        for project in fw.projects.iter_find(exhaustive=True):
            for rule in fw.get_project_rules(project.id):
                if _filter_rule(args, rule):
                    yield (project, rule)


def _filter_rule(args, rule):
    """Check if the given rule passes the filter specified in args.

    Args:
        args (Namespace): The arguments namespace
        rule (Rule): The rule to check

    Return:
        bool: True if the rule passes the filter (and should be included in output)
    """
    if args.pinned:
        return rule.get('auto_update') != True
    return True


def _rule_short_str(project, gears, rule):
    """Return a short descriptive string about a rule"""
    gear = gears[rule.gear_id]
    gear_str = gear_short_str(gear)

    return f'{project.label}: {rule.name} (auto_update={rule.auto_update})\n    {gear_str}'
