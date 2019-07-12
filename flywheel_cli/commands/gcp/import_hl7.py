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
                        help='GCP project (default: {})'.format('project'))
    parser.add_argument('--location', metavar='NAME',
                        help='Location (default: {})'.format('location'))
    parser.add_argument('--dataset', metavar='NAME',
                        help='Dataset (default: {})'.format('dataset'))
    parser.add_argument('--hl7store', metavar='NAME',
                        help='HL7 store (default: {})'.format('hl7store'))
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

    def upload_refs_json(project, refs):

        refs_json = {'hl7s': [ref for ref in refs]}
        json_file = io.BytesIO(json.dumps(refs_json).encode('utf-8'))
        json_file.name = datetime.datetime.now().strftime('hl7-import-refs_%Y%m%d_%H%M%S.json')
        json_file = {'file': json_file}
        api.post('/projects/' + project._id + '/files', files=json_file)
        return json_file.get('file').name

    profile = get_profile()
    profile_object = create_profile_object('hl7Store', profile, args)
    refs = args.refs or sys.stdin.read().split()
    if not refs:
        raise CliError('At least one ref required.')
    api = create_flywheel_session()
    for gear in api.get('/gears'):
        if gear['gear']['name'] == 'ghc-import':
            break
    else:
        raise CliError('ghc-import gear is not installed on ' + api.baseurl.replace('/api', ''))
    client = create_flywheel_client()

    # TODO check whether it is a project
    project = client.lookup(args.container)
    refs_in_json_file_name = upload_refs_json(project, refs)
    store_name = 'projects/{project}/locations/{location}/datasets/{dataset}/hl7V2Stores/{hl7store}'.format(**profile_object)
    job = api.post('/jobs/add', json={
        'gear_id': gear['_id'],
        'destination': {'type': 'project', 'id': project._id},
        'inputs': {
                    'import_ids': {
                        'type': 'project',
                        'id': project._id,
                        'name': refs_in_json_file_name
                    },
                },
        'config': {
            'auth_token_id': get_token_id(),
            'hc_hl7store': store_name,
            'project_id': project._id,
            'log_level': 'DEBUG' if args.debug else 'INFO',
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
