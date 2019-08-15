import fs
import re
import json
import os
import sys

from ...exchange import GearExchangeDB, GearVersionKey, prepare_manifest_for_upload
from ...sdk_impl import create_flywheel_client, get_site_name
from ... import util

def add_command(subparsers):
    parser = subparsers.add_parser('upgrade-rules', help='Upgrade flywheel gear rules')
    parser.add_argument('name', help='The name of the gear to upgrade')
    parser.add_argument('--dry-run', action='store_true', help='Print actions to take without executing them')
    parser.add_argument('--enable-auto-update', action='store_true', help='Ensure that auto-update is enabled')

    parser.set_defaults(func=upgrade_gear_rules)
    parser.set_defaults(parser=parser)

    return parser

def upgrade_gear_rules(args):
    fw = create_flywheel_client()

    # Get the list of gears installed on the site
    response = fw.resolve(f'gears/{args.name}')
    current_gear = response.path[0]
    current_gear_id = current_gear.id

    eligible_gears = set()
    for gear in response.children:
        eligible_gears.add(gear.id)

    original_rules = []

    try:
        for rule in fw.get_site_rules():
            if rule['gear_id'] in eligible_gears:
                original_rules.append(rule.to_dict())
                if args.dry_run:
                    print(f'Site rule: "{rule.name}" would be updated')
                    continue
                print(f'Upgrading Site Rule: "{rule.name}" (id={rule.id})')
                _update_rule(args, fw, rule, current_gear_id)

        for project in fw.get_all_projects(exhaustive='True'):
            print(f'Checking {project.group}/{project.label}...')

            for rule in fw.get_project_rules(project.id):
                if rule['gear_id'] in eligible_gears:
                    original_rules.append(rule.to_dict())
                    if args.dry_run:
                        print(f'  Project rule: "{rule.name}" (id={rule.id}) would be updated')
                        continue

                    print(f'  Upgrading Project Rule: "{rule.name}" (id={rule.id})')
                    _update_rule(args, fw, rule, current_gear_id, project_id=project.id)

        if args.dry_run:
            print(f'{len(original_rules)} total rules would be updated')

    finally:
        print('Original rules are stored in original-rules.json')
        with open('original-rules.json', 'a') as f:
            f.write(json.dumps(original_rules))
            f.write('\n')

def _update_rule(args, fw, rule, gear_id, project_id=None):
    update_doc = {
        'gear_id': gear_id
    }

    if args.enable_auto_update:
        update_doc['auto_update'] = True

    if project_id is not None:
        fw.modify_project_rule(project_id, rule.id, update_doc)
    else:
        fw.modify_site_rule(rule.id, update_doc)
