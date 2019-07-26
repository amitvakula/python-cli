import datetime
import io
import json
import sys
import time

from .auth import get_token_id
from ...errors import CliError
from ...sdk_impl import SdkUploadWrapper, create_flywheel_client, create_flywheel_session

api = create_flywheel_session()
client = create_flywheel_client()


def check_ghc_import_gear():
    for gear in api.get('/gears'):
        if gear['gear']['name'] == 'ghc-import':
            return gear
    else:
        raise CliError('ghc-import gear is not installed on ' + api.baseurl.replace('/api', ''))

gear = check_ghc_import_gear()


def add_import_job(project_container, import_ids_file_name, store_name, de_identify=False, debug=False):
    project = client.lookup(project_container)
    store = store_name.split('/')[-2]
    if store.startswith('fhir'):
        store = 'fhirstore'
    if store.startswith('hl7'):
        store = 'hl7store'
    if store.startswith('dicom'):
        store = 'dicomstore'
    return api.post('/jobs/add', json={
            'gear_id': gear['_id'],
            'destination': {'type': 'project', 'id': project._id},
            'inputs': {
                        'import_ids': {
                            'type': 'project',
                            'id': project._id,
                            'name': import_ids_file_name
                        },
                    },
            'config': {
                'auth_token_id': get_token_id(),
                'hc_' + store: store_name,
                'de_identify': de_identify,
                'project_id': project._id,
                'log_level': 'DEBUG' if debug else 'INFO',
                }
        })


def log_import_job(job, job_async=False):
    job_id = job['_id']
    print('Started ghc-import job ' + job_id)
    if not job_async:
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


def upload_import_ids_json(file_type, project_container, identifiers):
    project = client.lookup(project_container)
    uploader = SdkUploadWrapper(client)
    identifiers_json = {file_type + 's': identifiers}
    json_file = io.BytesIO(json.dumps(identifiers_json).encode('utf-8'))
    json_file.name = datetime.datetime.now().strftime(file_type + '-import_%Y%m%d_%H%M%S.json')
    uploader.upload(project, json_file.name, json_file)
    return json_file.name
