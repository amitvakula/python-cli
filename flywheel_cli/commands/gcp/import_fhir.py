import argparse
import sys

from .auth import get_token_id
from .import_ghc import add_job, log_import_job, upload_json, check_ghc_import_gear
from .profile import get_profile, create_profile_object
from ...errors import CliError
from ...sdk_impl import create_flywheel_client, create_flywheel_session

IMPORT_FHRI_DESC = """
Import FHIR resources from Google Healthcare API by resource ref. Resource reference format is the following:
<type>/<id>.
Read resource refs from STDIN if none were given as arguments. To discover FHIRs
available in GCP for import see `fw gcp query fhir help`.

examples:
  fw gcp import fhir --ref Patient/60433200-2aec-4eab-a226-31add8752b32 group/project
  fw gcp query fhir --refs 'family=Laird' --type 'Patient' | fw gcp import fhir group/project

"""


def add_command(subparsers):
    parser = subparsers.add_parser('fhir',
                                   help='Import FHIR resources from Google Healthcapiare API by references',
                                   description=IMPORT_FHRI_DESC,
                                   formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--project', metavar='NAME',
                        help='Project (defaults to your GCP profile project)')
    parser.add_argument('--location', metavar='NAME',
                        help='Location (defaults to your GCP profile location)')
    parser.add_argument('--dataset', metavar='NAME',
                        help='Dataset (defaults to your GCP profile dataset)')
    parser.add_argument('--fhirstore', metavar='NAME',
                        help='Fhirstore (defaults to your GCP profile fhirstore, if exists)')
    parser.add_argument('--ref', metavar='REF', action='append', dest='refs', default=[],
                        help='<type>/<resource_id> to import')
    parser.add_argument('--job-async', action='store_true',
                        help='Do not wait for import job to finish')
    parser.add_argument('--debug', action='store_true',
                        help='Run import gear in debug mode')
    parser.add_argument('container', metavar='CONTAINER',
                        help='Flywheel project to import into (group/proj)')

    parser.set_defaults(func=import_fhir)
    parser.set_defaults(parser=parser)
    return parser


def import_fhir(args):
    # TODO fw de-identify profiles
    # TODO gcp de-identify

    profile = get_profile()
    profile_object = create_profile_object('fhirStore', profile, args)
    api = create_flywheel_session()

    refs = args.refs or sys.stdin.read().split()
    if not refs:
        raise CliError('At least one ref required.')

    gear = check_ghc_import_gear(api)
    client = create_flywheel_client()
    project = client.lookup(args.container)
    refs_in_json_file_name = upload_json('fhir', project, refs, client)
    store_name = 'projects/{project}/locations/{location}/datasets/{dataset}/fhirStores/{fhirstore}'.format(**profile_object)
    job = add_job('fhirStore', api, gear, project, refs_in_json_file_name, store_name, get_token_id, args)
    log_import_job(args, client, job)
