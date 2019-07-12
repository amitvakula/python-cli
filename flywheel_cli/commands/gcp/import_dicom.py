import argparse
import datetime
import io
import json
import sys
import time

from .auth import get_token_id
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
        help='GCP project (default: {})'.format('project'))
    parser.add_argument('--location', metavar='NAME',
        help='Location (default: {})'.format('location'))
    parser.add_argument('--dataset', metavar='NAME',
        help='Dataset (default: {})'.format('dataset'))
    parser.add_argument('--dicomstore', metavar='NAME',
        help='Dicomstore (default: {})'.format('dicomstore'))
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

    def upload_uids_json(project, uids):

        uids_json = {'dicoms': [uid for uid in uids]}
        json_file = io.BytesIO(json.dumps(uids_json).encode('utf-8'))
        json_file.name = datetime.datetime.now().strftime('dicom-import-uids_%Y%m%d_%H%M%S.json')
        json_file = {'file': json_file}
        api.post('/projects/' + project._id + '/files', files=json_file)
        return json_file.get('file').name

    profile = get_profile()
    profile_object = create_profile_object('dicomStore', profile, args)

    uids = args.uids or sys.stdin.read().split()
    if not uids:
        raise CliError('At least one UID required.')
    api = create_flywheel_session()
    for gear in api.get('/gears'):
        if gear['gear']['name'] == 'ghc-import':
            break
    else:
        raise CliError('ghc-import gear is not installed on ' + api.baseurl.replace('/api', ''))
    client = create_flywheel_client()

    # TODO check whether it is a project
    project = client.lookup(args.container)
    uids_in_json_file_name = upload_uids_json(project, uids)
    store_name = 'projects/{project}/locations/{location}/datasets/{dataset}/dicomStores/{dicomstore}'.format(**profile_object)

    job = api.post('/jobs/add', json={
        'gear_id': gear['_id'],
        'destination': {'type': 'project', 'id': project._id},
        'inputs': {
                    'import_ids': {
                        'type': 'project',
                        'id': project._id,
                        'name': uids_in_json_file_name
                    },
                },
        'config': {
            'auth_token_id': get_token_id(),
            'hc_dicomstore': store_name,
            'de_identify': args.de_identify,
            'project_id': project._id,
        }
    })
    job_id = job['_id']
    print('Started ghc-import job ' + job_id)

    if not args.job_async:
        jobs_api = client.jobs_api
        last_log_entry = 0
        print('Waiting for import job to finish...')
        while True:
            time.sleep(1)
            logs = jobs_api.get_job_logs(job_id)['logs']
            for i in range(last_log_entry, len(logs)):
                sys.stdout.write(logs[i]['msg'])
                last_log_entry = i + 1
            job = jobs_api.get_job(job_id)
            if job.state in ['failed', 'complete']:
                print('Job ' + job.state)
                break
