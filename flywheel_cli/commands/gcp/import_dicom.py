import argparse
import sys

from .auth import get_token_id
from .import_ghc import add_job, log_import_job, upload_json, check_ghc_import_gear
from .profile import get_profile, create_profile_object
from ...errors import CliError
from ...sdk_impl import create_flywheel_client, create_flywheel_session


IMPORT_DICOM_DESC = """
Import dicom series or whole studies from Google Healthcare API by InstanceUIDs.
Read UIDs from STDIN if none were given as arguments. To discover dicoms
available in GCP for import see `fw gcp query dicom help`.

examples:
  fw gcp import dicom --uid 1.2.840.113619.2.408... group/project
  fw gcp query dicom --uids 'AccessionNumber="AccNo"' | fw gcp import dicom group/project

"""


def add_command(subparsers):
    parser = subparsers.add_parser('dicom',
                                   help='Import dicoms from Google Healthcapiare API by InstanceUID',
                                   description=IMPORT_DICOM_DESC,
                                   formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--project', metavar='NAME',
                        help='Project (defaults to your GCP profile project)')
    parser.add_argument('--location', metavar='NAME',
                        help='Location (defaults to your GCP profile location)')
    parser.add_argument('--dataset', metavar='NAME',
                        help='Dataset (defaults to your GCP profile dataset)')
    parser.add_argument('--dicomstore', metavar='NAME',
                        help='Dicomstore (defaults to your GCP profile dicomstore)')
    parser.add_argument('--uid', metavar='UID', action='append', dest='uids', default=[],
                        help='Study/SeriesInstanceUID to import')
    parser.add_argument('--de-identify', action='store_true',
                        help='De-identify dicoms before import')
    parser.add_argument('--job-async', action='store_true',
                        help='Do not wait for import job to finish')
    parser.add_argument('container', metavar='CONTAINER',
                        help='Flywheel project to import into (group/proj)')

    parser.set_defaults(func=import_dicom)
    parser.set_defaults(parser=parser)
    return parser


def import_dicom(args):
    # TODO fw de-identify profiles
    # TODO gcp de-identify

    profile = get_profile()
    profile_object = create_profile_object('dicomStore', profile, args)
    api = create_flywheel_session()

    uids = args.uids or sys.stdin.read().split()
    if not uids:
        raise CliError('At least one UID required.')

    gear = check_ghc_import_gear(api)
    client = create_flywheel_client()
    project = client.lookup(args.container)
    uids_in_json_file_name = upload_json('dicom', project, uids, client)
    store_name = 'projects/{project}/locations/{location}/datasets/{dataset}/dicomStores/{dicomstore}'.format(**profile_object)
    job = add_job('dicomStore', api, gear, project, uids_in_json_file_name, store_name, get_token_id, args)
    log_import_job(args, client, job)
