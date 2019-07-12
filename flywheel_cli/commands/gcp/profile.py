import argparse
import collections
import json
import os
import sys

from ...errors import CliError
from ...sdk_impl import create_flywheel_session
from ...util import set_subparser_print_help

PROPERTIES = collections.OrderedDict({
    'project': 'Google Cloud Platform project',
    'location': 'API location/region (Healthcare, AutoML, etc.)',
    'hc_dataset': 'Healthcare API dataset',
    'hc_dicomstore': 'Healthcare API dicom-store',
    'hc_fhirstore': 'Healthcare API FHIR-store',
    'hc_hl7store': 'Healthcare API HL7-store',
})


PROFILE_PROPERTIES = collections.OrderedDict({
    'auth_token': 'GCP auth token',
    'dicomStore': 'Healthcare API dicom-store',
    'fhirStore': 'Healthcare API FHIR-store',
    'hl7Store': 'Healthcare API HL7-store',
})


PARSER_DESC = """
Create, update and delete GCP user profiles used throughout `fw gcp` commands.

examples:
  fw gcp profile list
  fw gcp profile create my-profile project=my-project location=us-central1 ...
  fw gcp profile update my-profile hc_dataset=my-dataset ...
  fw gcp profile delete my-profile

available profile properties:
""" + '\n'.join('  {:16.16} {}'.format(key, desc) for key, desc in PROPERTIES.items())


def add_command(subparsers):
    parser = subparsers.add_parser('profile',
        help='Manage GCP profiles',
        description=PARSER_DESC,
        formatter_class=argparse.RawTextHelpFormatter)
    profile_subparsers = parser.add_subparsers(
        title='available gcp profile commands', metavar='')

    list_parser = profile_subparsers.add_parser('list',
        help='List GCP profiles',
        description='List available GCP user profiles.')
    list_parser.set_defaults(func=profile_list)
    list_parser.set_defaults(parser=list_parser)

    create_parser = profile_subparsers.add_parser('create',
        help='Create a new GCP profile',
        description='Create a new GCP user profile.')
    create_parser.add_argument('name', metavar='NAME',
        help='Profile name to create')
    create_parser.add_argument('properties', nargs='+', metavar='KEY=VALUE',
        help='Property key and value')
    create_parser.set_defaults(func=profile_create)
    create_parser.set_defaults(parser=create_parser)

    update_parser = profile_subparsers.add_parser('update',
        help='Update a GCP profile',
        description='Update an existing GCP user profile.')
    update_parser.add_argument('name', metavar='NAME',
        help='Profile name to update')
    update_parser.add_argument('properties', nargs='+', metavar='KEY=VALUE',
        help='Property key and value')
    update_parser.set_defaults(func=profile_update)
    update_parser.set_defaults(parser=update_parser)

    delete_parser = profile_subparsers.add_parser('delete',
        help='Delete a GCP profile',
        description='Delete an existing GCP user profile.')
    delete_parser.add_argument('name', metavar='NAME',
        help='Profile name to delete')
    delete_parser.set_defaults(func=profile_delete)
    delete_parser.set_defaults(parser=delete_parser)

    set_subparser_print_help(parser, profile_subparsers)
    return parser


def create_store_specific_path_part(profile_data):
    stores = {
        'dicomStore': 'dicomStores/{}'.format(profile_data.get('hc_dicomstore')),
        'fhirStore': 'fhirStores/{}'.format(profile_data.get('hc_fhirstore')),
        'hl7V2Store': 'hl7V2Stores/{}'.format(profile_data.get('hc_hl7store'))
        }
    return stores


def create_store_path(key, profile, base_path):
    if create_store_specific_path_part(profile)[key].split('/')[1] == 'None':
        return None
    else:
        return base_path + create_store_specific_path_part(profile)[key]


def get_profiles():
    api = create_flywheel_session()
    try:
        return api.get('/users/self/info')
    except CliError:
        return {}


def get_profile():
    profiles = get_profiles()
    if profiles and profiles.get('ghc_import').get('profiles'):
        for profile, value in profiles['ghc_import']['profiles'].items():
            if profile == profiles['ghc_import']['selected_profile']:
                return value
    else:
        return {}


def args_to_profile(args):
    profile = {'name': args.name}
    for prop in args.properties:
        if '=' not in prop:
            print('Invalid argument: {} (key=value format expected)'.format(prop), file=sys.stderr)
            sys.exit(1)
        key, value = prop.split('=', 1)
        if key not in PROPERTIES:
            print('Invalid property: {}'.format(key), file=sys.stderr)
            sys.exit(1)
        profile[key] = value
    return profile


def create_profile_object(store, profile=None, args=None):
    lowercase_store = store.lower()
    if profile:
        if profile.get(store):
            profile_elements = profile[store].split('/')[1::2]
            return {
                'project': profile_elements[0],
                'location': profile_elements[1],
                'dataset': profile_elements[2],
                lowercase_store: profile_elements[3]
            }
        else:
            print("Kindly provide {} in your profile!".format(lowercase_store))
            sys.exit(1)
    for param in ['project', 'location', 'dataset', lowercase_store]:
        if not getattr(args, param, None):
            raise CliError(param + ' required')
    return {
        'project': args.project,
        'location': args.location,
        'dataset': args.dataset,
        lowercase_store: getattr(args, lowercase_store)
    }


def profile_list(args):
    profiles = get_profiles()
    if profiles and profiles.get('ghc_import').get('profiles'):
        active_profile = profiles['ghc_import']['selected_profile']
        profiles = list(profiles['ghc_import']['profiles'].values())
        for index, profile in enumerate(profiles):
            print('Profile: {}'.format(profile['name']) + (' (active)' if profile['name'] == active_profile else ''))
            for key in PROFILE_PROPERTIES:
                print('  {} = {}'.format(key, profile.get(key, '')))
    else:
        print('No GCP user profiles found.')


def profile_create(args):

    def create_profile(token, additional_profile, base_path):
        return {
                    additional_profile['name']: {
                        "auth_token": token,
                        "name": additional_profile['name'],
                        "fhirStore": create_store_path("fhirStore", additional_profile, base_path),
                        "hl7Store": create_store_path("hl7V2Store", additional_profile, base_path),
                        "dicomStore": create_store_path("dicomStore", additional_profile, base_path)
                    }
                }

    def check_3rdparty_connected_account():
        try:
            token = api.get('/users/self/tokens')
            if token:
                return token[0]['_id']
        except CliError:
            return None

    api = create_flywheel_session()
    token = check_3rdparty_connected_account()
    if not token:
        raise CliError('Please add your account to "Connected 3rd-party accounts"!')

    if not [elem for elem in args.properties if elem.startswith('hc_dicomstore')]:
        raise CliError('hc_dicomstore required')
        sys.exit(1)

    existing_profiles = get_profiles()
    additional_profile = args_to_profile(args)
    base_path = 'projects/{project}/locations/{location}/datasets/{hc_dataset}/'.format(**additional_profile)

    if not existing_profiles:
        brand_new_profile = {
                    "selected_profile": additional_profile['name'],
                    "profiles": create_profile(token, additional_profile, base_path)
                    }
        api.post('/users/self/info', json={'set': {'ghc_import': brand_new_profile}})
        print('Successfully created GCP profile ' + args.name)
    elif args.name in existing_profiles['ghc_import']['profiles']:
        raise CliError('GCP profile already exists: ' + args.name)
    else:
        existing_profiles['ghc_import']['profiles'].update(create_profile(token, additional_profile, base_path))
        existing_profiles['ghc_import']['selected_profile'] = additional_profile['name']
        api.post('/users/self/info', json={'set': existing_profiles})
        print('Successfully added GCP profile ' + args.name)


def profile_update(args):

    def update_profile(profile, profile_update_data, base_path):
        return {
                "fhirStore": create_store_path("fhirStore", profile_update_data, base_path) or profile['fhirStore'],
                "hl7Store": create_store_path("hl7V2Store", profile_update_data, base_path) or profile['hl7Store'],
                "dicomStore": create_store_path("dicomStore", profile_update_data, base_path) or profile['dicomStore']

                }

    profiles = get_profiles()
    profiles_list = list(profiles['ghc_import']['profiles'].values())

    for profile in profiles_list:
        if profile['name'] == args.name:
            break
    else:
        raise CliError('GCP profile not found: ' + args.name)
    base_path_elements = profile['dicomStore'].split('/')[1::2]
    base_path = 'projects/{}/locations/{}/datasets/{}/'.format(*base_path_elements)
    profile_update_data = args_to_profile(args)
    updated_profile = update_profile(profile, profile_update_data, base_path)

    profile.update(updated_profile)
    api = create_flywheel_session()
    api.post('/users/self/info', json={'set': profiles})
    print('Successfully updated GCP profile ' + args.name)


def profile_delete(args):
    profiles = get_profiles()
    profiles_list = list(profiles['ghc_import']['profiles'].values())

    for profile in profiles_list:
        if profile['name'] == args.name:
            break
    else:
        raise CliError('GCP profile not found: ' + args.name)

    def remove_profile(profiles, profile):
        reduced_profiles = profiles['ghc_import']['profiles']
        del reduced_profiles[profile['name']]
        return reduced_profiles

    reduced = remove_profile(profiles, profile)
    api = create_flywheel_session()
    api.post('/users/self/info', json={'set': profiles})
    print('Profile successfully deleted')
