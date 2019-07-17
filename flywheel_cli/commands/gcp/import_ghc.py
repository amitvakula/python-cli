import datetime
import io
import json
import sys
import time

from .auth import get_token_id
from ...errors import CliError
from ...sdk_impl import SdkUploadWrapper


def add_job(store, api, gear, project, json_file, store_name, get_token_id, args):

    def config_job(store, store_name, project, args):
        lowercase_store = store.lower()
        if store == 'dicomStore':
            return {
                    'auth_token_id': get_token_id(),
                    'hc_' + lowercase_store: store_name,
                    'de_identify': args.de_identify,
                    'project_id': project._id,
                    }
        return {
                'auth_token_id': get_token_id(),
                'hc_' + lowercase_store: store_name,
                'project_id': project._id,
                'log_level': 'DEBUG' if args.debug else 'INFO',
                }

    return api.post('/jobs/add', json={
            'gear_id': gear['_id'],
            'destination': {'type': 'project', 'id': project._id},
            'inputs': {
                        'import_ids': {
                            'type': 'project',
                            'id': project._id,
                            'name': json_file
                        },
                    },
            'config': config_job(store, store_name, project, args)
        })


def log_import_job(args, client, job):
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


def upload_json(file_type, project, identifiers, client):
    uploader = SdkUploadWrapper(client)
    identifiers_json = {file_type + 's': identifiers}
    json_file = io.BytesIO(json.dumps(identifiers_json).encode('utf-8'))
    json_file.name = datetime.datetime.now().strftime(file_type + '-import_%Y%m%d_%H%M%S.json')
    uploader.upload(project, json_file.name, json_file)
    return json_file.name


def check_ghc_import_gear(api):
    for gear in api.get('/gears'):
        if gear['gear']['name'] == 'ghc-import':
            return gear
    else:
        raise CliError('ghc-import gear is not installed on ' + api.baseurl.replace('/api', ''))
