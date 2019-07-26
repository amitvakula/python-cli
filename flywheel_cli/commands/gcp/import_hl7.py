import argparse
import sys

from .import_ghc import add_import_job, log_import_job, upload_import_ids_json, check_ghc_import_gear
from .profile import get_profile, create_profile_object
from ...errors import CliError

IMPORT_HL7_DESC = """
Import HL7 messages from Google Healthcare API by message id.
Read message ids from STDIN if none were given as arguments. To discover HL7 messages
available in GCP for import see `fw gcp query hl7 help`.

examples:
  fw gcp import hl7 --ref group/project
  fw gcp query hl7 --refs 'PatientId("XYZ", "MR")' | fw gcp import hl7 group/project

"""


def add_command(subparsers):
    parser = subparsers.add_parser('hl7',
                                   help='Import HL7 messages from Google Healthcapiare API by ids',
                                   description=IMPORT_HL7_DESC,
                                   formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--project', metavar='NAME',
                        help='Project (defaults to your GCP profile project)')
    parser.add_argument('--location', metavar='NAME',
                        help='Location (defaults to your GCP profile location)')
    parser.add_argument('--dataset', metavar='NAME',
                        help='Dataset (defaults to your GCP profile dataset)')
    parser.add_argument('--hl7store', metavar='NAME',
                        help='HL7 store (defaults to your GCP profile hl7store, if exists)')
    parser.add_argument('--ref', metavar='REF', action='append', dest='refs', default=[],
                        help='<id> to import')
    parser.add_argument('--job-async', action='store_true',
                        help='Do not wait for import job to finish')
    parser.add_argument('--debug', action='store_true',
                        help='Run import gear in debug mode')
    parser.add_argument('container', metavar='CONTAINER',
                        help='Flywheel project to import into (group/proj)')

    parser.set_defaults(func=import_hl7)
    parser.set_defaults(parser=parser)
    return parser


def import_hl7(args):
    # TODO fw de-identify profiles
    # TODO gcp de-identify

    profile = get_profile()
    profile_object = create_profile_object('hl7Store', profile, args)

    refs = args.refs or sys.stdin.read().split()
    if not refs:
        raise CliError('At least one ref required.')

    refs_in_json_file_name = upload_import_ids_json('hl7', args.container, refs)
    store_name = 'projects/{project}/locations/{location}/datasets/{dataset}/hl7V2Stores/{hl7store}'.format(**profile_object)
    job = add_import_job(args.container, refs_in_json_file_name, store_name, debug=args.debug)
    log_import_job(job, job_async=args.job_async)
