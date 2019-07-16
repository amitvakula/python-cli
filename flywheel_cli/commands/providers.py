import logging
import flywheel
import os
import tempfile
import yaml
import json
import sys
import argparse

from ..sdk_impl import create_flywheel_client
from ..util import edit_file

log = logging.getLogger(__name__)

"""
  Adds the 'add' tree of provider commands to the current command lineage

"""
def add_add_command(subparsers, parents):
    parser = subparsers.add_parser('add', parents=parents, help='Add a new provider into Flywheel')
    # We have to replicate these arguments to all the subparses to keep the command positional order sane
    #parser.add_argument('--label', required=False, help='Name of the provider')
    #parser.add_argument('--skip-edit', required=False, help='Skip the interactive editor', action='store_true')
 
    def print_help(args):
        parser.print_help()

    parser.set_defaults(func=print_help)
    subparsers = parser.add_subparsers(dest='class_')

    add_class_commands(subparsers, parents)

    return parser

"""
  Adds the 'mod' tree of provider commands to the current command lineage.

"""
def add_modify_command(subparsers, parents):
    parser = subparsers.add_parser('modify', parents=parents, help='Modify the configuration of a provider in Flywheel')

    parser.add_argument('id', help='A string with the provider id')
    parser.set_defaults(func=mod_provider)

    # The args are parseed for all command so to allow unkwown commands would required enabling that on the entire tree
    # Instead just add all the possible args and suppress the notice so they are hidden but validated if supplied
    parser.add_argument('--skip-edit', required=False, action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--label', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--aws-secret-access-key', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--aws-access-key-id', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--gs-json-key-file', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--region', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--path', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--bucket', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--queue-threshold', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--max-compute', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--machine-type', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--disk-size', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--swap-size', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--preemptible', required=False, help=argparse.SUPPRESS)
    parser.add_argument('--zone', required=False, help=argparse.SUPPRESS)

    return parser


"""
    Adds class command as a subparser to the chain
"""
def add_class_commands(subparsers, parents):

    parser_compute = subparsers.add_parser('compute', help='Compute provider type', parents=parents)
    def print_help(args):
        parser_compute.print_help()
    parser_compute.set_defaults(func=print_help)
    
    compute_subs = parser_compute.add_subparsers(dest='type', metavar='{type}')
    add_compute_type_commands(compute_subs, parents)


    parser_storage = subparsers.add_parser('storage', help='Storage Provider Class', parents=parents)
    def print_help(args):
        parser_storage.print_help()
    parser_storage.set_defaults(func=print_help)
    storage_subs = parser_storage.add_subparsers(dest='type', metavar='{type}')
    add_storage_type_commands(storage_subs, parents)

"""
Adds storage class commands and arguments to the subparser chain
"""
def add_storage_type_commands(subparsers, parents):
    parser_aws = subparsers.add_parser('aws', help='Aws type provider', parents=parents)
    parser_aws.set_defaults(func=add_provider)
    # TODO: We need the arg at this level if we want them in sequence positionally
    parser_aws.add_argument('--label', required=False, help='Name of the provider')
    parser_aws.add_argument('--skip-edit', required=False, help='Skip the interactive editor', action='store_true')
    add_aws_creds(parser_aws)
    add_aws_storage_config(parser_aws)

    parser_gc = subparsers.add_parser('gc', help='Google Cloud provider', parents=parents)
    parser_gc.set_defaults(func=add_provider)
    parser_gc.add_argument('--label', required=False, help='Name of the provider')
    parser_gc.add_argument('--skip-edit', required=False, help='Skip the interactive editor', action='store_true')
    add_gc_creds(parser_gc)
    add_gc_storage_config(parser_gc)

    parser_local = subparsers.add_parser('local', parents=parents)
    parser_local.set_defaults(func=add_provider)
    parser_local.add_argument('--label', required=False, help='Name of the provider')
    parser_local.add_argument('--skip-edit', required=False, help='Skip the interactive editor', action='store_true')
    add_local_storage_config(parser_local)

"""
Adds compute  class commands and arguments to the subparser chain
"""
def add_compute_type_commands(subparsers, parents):
    parser_aws = subparsers.add_parser('aws', help='Aws type provider', parents=parents)
    parser_aws.set_defaults(func=add_provider)
    parser_aws.add_argument('--label', required=False, help='Name of the provider')
    parser_aws.add_argument('--skip-edit', required=False, help='Skip the interactive editor', action='store_true')
    add_aws_creds(parser_aws)
    add_compute_config(parser_aws)

    parser_gc = subparsers.add_parser('gc', help='Google Cloud Compute provider', parents=parents)
    parser_gc.set_defaults(func=add_provider)
    parser_gc.add_argument('--label', required=False, help='Name of the provider')
    parser_gc.add_argument('--skip-edit', required=False, help='Skip the interactive editor', action='store_true')
    add_gc_creds(parser_gc)
    add_compute_config(parser_gc)

    parser_static = subparsers.add_parser('static', parents=parents)
    parser_static.add_argument('--label', required=False, help='Name of the provider')
    parser_static.add_argument('--skip-edit', required=False, help='Skip the interactive editor', action='store_true')
    parser_static.set_defaults(func=add_provider)
    add_compute_config(parser_static)

"""
Adds AWS cred arguemnts
"""
def add_aws_creds(parser):
    parser.add_argument('--aws-secret-access-key', required=False, help='AWS secret access key')
    parser.add_argument('--aws-access-key-id', required=False, help='AWS access key id')

"""
Adds GC cred arguemnts
"""
def add_gc_creds(parser):
    parser.add_argument('--gs-json-key-file', required=False, help='location of the Google Cloud key.json file')

"""
Adds AWS storage arguemnts
"""
def add_aws_storage_config(parser):
    parser.add_argument('--region', required=False, help='AWS region')
    parser.add_argument('--path', required=False, help='AWS Storge path')
    parser.add_argument('--bucket', required=False, help='AWS bucket name')

"""
Adds GC stroage arguments
"""
def add_gc_storage_config(parser):
    parser.add_argument('--path', required=False, help='GC Storge path')
    parser.add_argument('--bucket', required=False, help='GC bucket name')

def add_local_storage_config(parser):
    parser.add_argument('--path', required=False, help='Local Storge path')

"""
Adds common compute arguments to the parser
"""
def add_compute_config(parser):
    parser.add_argument('--queue-threshold', help='Queue threshold for the provider')
    parser.add_argument('--max-compute', help='Queue threshold for the provider')
    parser.add_argument('--machine-type', help='Machine Type for the provider')
    parser.add_argument('--disk-size', help='Disk Size for the provider in MB') #TODO: whare are the unit here?
    parser.add_argument('--swap-size', help='Swap Size for the provider in MB')
    parser.add_argument('--preemptible', help='Create a preemtible provider')
    parser.add_argument('--zone', help='Zone to use for this provider')
    parser.add_argument('--region', help='Region to use for this provider')


"""
Creates the assignement provider command
"""
def add_assign_command(subparsers, parents):
    parser = subparsers.add_parser('assign', parents=parents, help='Modify the configuration of a provider in Flywheel')
    parser.add_argument('type', help='The container type to which you want to assign providers.', choices=['group', 'project'])
    parser.add_argument('id', help='Id of the container to assign providers')
    parser.add_argument('--compute', required=False, help='Provider id to assign as the compute provider')
    parser.add_argument('--storage', required=False, help='Provider id to assign as the storage provider')
    parser.set_defaults(func=assign_provider)
    parser.set_defaults(parser=parser)
    return parser


"""
Actual data processing routing for adding providers.  All sub command route through this
function so we do need to check for which optional arguments are supplied.
"""
def add_provider(args):
    fw = create_flywheel_client()
    config = get_config_vals(class_=args.class_, type_=args.type)

    parse_args(args, config)
    
    if args.skip_edit:
        id_ = fw.add_provider(config)
        # log.info('Provider has been saved: {}'.format(id_))
        return id_
    else:
        process_edit(config, fw.add_provider)



"""
Providers the data processing for provider modification
We only allow changing of the label, config, and creds.
"""
def mod_provider(args):
    fw = create_flywheel_client()

    try:
        provider = fw.get_provider(args.id)
    except flywheel.ApiException as e:
        log.error(str(e))
        sys.exit(1)

    mod_data = {
        'label': provider.label,
        'config': provider.config if provider.config else {}
    }
    parse_args(args, mod_data)

    if provider.creds:
        mod_data['creds'] = provider.creds

    if args.skip_edit:
        fw.modify_provider(args.id, mod_data)
    else:
        process_edit(mod_data, fw.modify_provider, args.id)

    return


"""
Actual processing of provider assignment command
"""
def assign_provider(args):

    data = {
        'providers': {}
    }

    has_provider = False
    if hasattr(args, 'storage') and args.storage:
        data['providers']['storage'] = args.storage
        has_provider = True
    if hasattr(args, 'compute') and args.compute:
        data['providers']['compute'] = args.compute
        has_provider = True

    if not has_provider:
        log.error('You must provider either a Storage or Compute provider')
        sys.exit(1)

    fw = create_flywheel_client()
    try:
        if args.type == 'group':
            result = fw.modify_group(args.id, data)
        if args.type == 'project':
            result = fw.modify_project(args.di, data)
    except flywheel.ApiException as e:
        log.error(str(e))
        sys.exit(1)

    log.info('{} was saved.'.format(args.type))


"""
Provides a local system edior that allows the user to view the config in Yaml. 
Usees the current api to validate changes so that we can determine when the configuration
is correct and complete otherwise we look the user in the edit routine, unless user exits.

args: 
    config dictionary: The current object config to be injected
    function function: The callable sdk command to process the data
    id string: The id if we are going to be editing an existing provider
"""
def process_edit(config, function, id_=None):
    data = yaml.dump(config, default_flow_style=False)

    fd, path = tempfile.mkstemp('provider-config')
    with os.fdopen(fd, 'w') as f:
        print('# Edit this file and then save and exit to save the provider.', file=f)
        print(data, file=f)


    valid = False
    while not valid:

        edit_file(path)
        try:
            data = yaml.load(open(path))
        # TODO: limit this exception type down a bit
        except Exception as e:
            #log.exception(e)
            log.error(e.message)
            choice = input("\nInvalid YAML. Enter 'q' to quit. Or press any other key to re-edit the configuration\n")
            if choice == "q" or choice == "Q":
                log.info('Provider has not been saved')
                sys.exit(1)
            continue

        try:
            if id_ is not None:
                # On modify we only get a 200 back and no response data
                function(id_, data)
            else:
                provider = function(data)
        except flywheel.ApiException as e:
            log.error(str(e))

            choice = input("\nEnter 'q' to quit. Or press any other key to re-edit the configuration\n")
            if choice == "q" or choice == "Q":
                log.info('Provider has not been saved')
                sys.exit(1)
            continue

        valid = True

    if id_:
        log.info('Provider has been saved: {}'.format(id_))
    else:
        log.info('Provider has been saved: {}'.format(provider))
    
"""
Pull args off the command line and place them in the respective config locations
"""
def parse_args(args, config):

    if hasattr(args, 'aws_secret_access_key') and args.aws_secret_access_key:
        config['creds']['aws_secret_access_key'] = args.aws_secret_access_key
    if hasattr(args, 'aws_access_key_id') and args.aws_access_key_id:
        config['creds']['aws_access_key_id'] = args.aws_access_key_id

    if hasattr(args, 'label') and args.label:
        config['label'] = args.label
    if hasattr(args, 'path') and args.path:
        config['config']['path'] = args.path
    if hasattr(args, 'region') and args.region:
        config['config']['region'] = args.region
    if hasattr(args, 'bucket') and args.bucket:
        config['config']['bucket'] = args.bucket
    if hasattr(args, 'zone') and args.zone:
        config['config']['zone'] = args.zone
    if hasattr(args, 'queue_threshold') and args.queue_threshold:
        config['config']['queue_threshold'] = args.queue_threshold
    if hasattr(args, 'max_compute') and args.max_compute:
        config['config']['max_compute'] = args.max_compute
    if hasattr(args, 'machine_type') and args.machine_type:
        config['config']['machine_type'] = args.machine_type
    if hasattr(args, 'disk_size') and args.disk_size:
        config['config']['disk_size'] = args.disk_size
    if hasattr(args, 'swap_size') and args.swap_size:
        config['config']['swap_size'] = args.swap_size
    if hasattr(args, 'preemptible'):
        config['config']['preemptible'] = args.preemptible

    if hasattr(args, 'gs_json_key_file') and args.gs_json_key_file:
        with open(args.gs_json_key_file, 'r') as f:
            key = json.load(f)
            config['creds']['client_email'] = key['client_email']
            config['creds']['client_id'] = key['client_id']
            config['creds']['project_id'] = key['project_id']
            config['creds']['private_key_id'] = key['private_key_id']
            config['creds']['private_key'] = key['private_key']
            config['creds']['client_x509_cert_url'] = key['client_x509_cert_url']


"""
Provides a configuration dict that has all the needed fields for the specified type and class
so that data can be injected into this dic to present to the user.
"""
def get_config_vals(class_, type_):

    return_vals = {
        'provider_class': class_,
        'provider_type': type_,
        'label': ''
    }

    # All compute need base values
    if class_ == 'compute':
        return_vals['config'] = {
            "region": "",
            "zone": "",
            "queue_threshold": 1,
            "max_compute": 1,
            "machine_type": 1,
            "disk_size": 100,
            "swap_size": 100,
            "preemptible": False
        }


    # These are the non creds types
    if class_=='storage' and type_=='local':
        return_vals['config'] = {
            'path': ''
        }
        return_vals['creds'] = {}
        return return_vals

    if class_=='compute' and type_=='static':
        return_vals['creds'] = {}
        return return_vals


    ## Below are the creds versions

    if type_=='gc':
        return_vals['creds'] = {
            'client_email': '',
            'client_id': '',
            'project_id': '',
            'private_key': '',
            'private_key_id': '',
            'client_x509_cert_url': ''
        }

    if class_=='storage' and type_=='gc':
        return_vals['config'] = {
                'path': '' # This only has path until its moved from PyFs 
        }
        return return_vals


    # Aws types use the same creds
    if type_=='aws':
        return_vals['creds'] = {
            'aws_access_key_id': '',
            'aws_secret_access_key': ''
        }
    if class_ == 'storage' and type_=='aws':
        return_vals['config'] = {
            'bucket': '',
            'region': '',
            'path': ''
        }
        return return_vals

    return return_vals
