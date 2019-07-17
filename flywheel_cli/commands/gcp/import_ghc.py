import datetime
import io
import json
import sys
import time

from .auth import get_token_id
from ...errors import CliError


def add_job(store, api, gear, project, uids_in_json_file_name, profile, get_token_id, args):
    store_name = profile.get(store)
    lowercase_store = store.lower()
    return api.post('/jobs/add', json={
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
                'hc_' + lowercase_store: store_name,
                # 'de_identify': args.de_identify or None,
                'project_id': project._id,
            }
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


def upload_json(file_type, project, identifiers, api):

    identifiers_json = {file_type + 's': identifiers}
    json_file = io.BytesIO(json.dumps(identifiers_json).encode('utf-8'))
    json_file.name = datetime.datetime.now().strftime(file_type + '-import_%Y%m%d_%H%M%S.json')
    json_file = {'file': json_file}
    api.post('/projects/' + project._id + '/files', files=json_file)
    return json_file.get('file').name


def check_ghc_import_gear(api):
    for gear in api.get('/gears'):
        if gear['gear']['name'] == 'ghc-import':
            return gear
            # break
    else:
        raise CliError('ghc-import gear is not installed on ' + api.baseurl.replace('/api', ''))
