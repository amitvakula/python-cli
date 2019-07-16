import os
import tempfile
import json
import copy

from unittest import mock

from flywheel_cli.commands.providers import add_provider, mod_provider
from flywheel_cli.importers.bruker_scan import format_timestamp_fn

def test_add_provider_gc_compute():
    gc_data = {
        'type': 'service_account',
        'project_id': 'project_123456',
        'private_key_id': 'my-private-key-id',
        'private_key': 'my-private-key',
        'client_email': 'my-client-email',
        'client_id': 'client_123456',
        'auth_uri': 'auth_uri_data',
        'token_uri': 'token_uri_data',
        'auth_provider_x509_cert_url': 'https://www.secretplace.com',
        'client_x509_cert_url': 'https://www.secretplace.com/cert_url'
    }

    args = mock.Mock()
    args.gs_json_key_file = 'path/to/file.json'
    args.class_ = 'compute'
    args.type = 'gc'
    args.aws_secret_access_key = None
    args.aws_access_key_id = None
    args.path = None
    args.label = 'I am label'
    args.region = 'my-region'
    args.zone = 'my-zone'
    args.bucket = None
    args.queue_threshold = 7
    args.max_compute = 44
    args.machine_type = 55
    args.disk_size = 101
    args.swap_size = 102
    args.preemptible = True
    args.skip_edit = False

    with mock.patch('flywheel_cli.commands.providers.create_flywheel_client'):
        with mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(gc_data))) as mock_file:
            with mock.patch('flywheel_cli.commands.providers.process_edit') as mock_edit:
                add_provider(args)

    # Stub out the data the way it should be retured to verfiy the values are populated
    expected_data = {
        'creds': {
            'project_id': gc_data['project_id'],
            'private_key_id': gc_data['private_key_id'],
            'private_key': gc_data['private_key'],
            'client_email': gc_data['client_email'],
            'client_id': gc_data['client_id'],
            'client_x509_cert_url': gc_data['client_x509_cert_url']
        },
        'config': {
            'region': args.region,
            'zone': args.zone,
            'queue_threshold': args.queue_threshold,
            'max_compute': args.max_compute,
            'machine_type': args.machine_type,
            'disk_size': args.disk_size,
            'swap_size': args.swap_size,
            'preemptible': args.preemptible,
        },
        'label': args.label,
        'provider_class': args.class_,
        'provider_type': args.type
    }
    mock_edit.assert_called_with(expected_data, mock.ANY)

def test_add_provider_aws_storage():
    args = mock.Mock()
    args.gs_json_key_file = None
    args.class_ = 'storage'
    args.type = 'aws'
    args.aws_secret_access_key = 'secret-key'
    args.aws_access_key_id = 'key-id'
    args.path = 'path/to/files'
    args.bucket = 'my-bucket'
    args.label = 'I am label'
    args.region = 'my-region'
    args.zone = None
    args.queue_threshold = None
    args.max_compute = None
    args.machine_type = None
    args.disk_size = None
    args.swap_size = None
    args.preemptible = None
    args.skip_edit = False

    with mock.patch('flywheel_cli.commands.providers.create_flywheel_client'):
        with mock.patch('flywheel_cli.commands.providers.process_edit') as mock_edit:
            add_provider(args)

    # Stub out the data the way it should be retured to verfiy the values are populated
    expected_data = {
        'creds': {
            'aws_secret_access_key': args.aws_secret_access_key,
            'aws_access_key_id': args.aws_access_key_id
        },
        'config': {
            'bucket': args.bucket,
            'path': args.path,
            'region': args.region,
            'preemptible' : None # This gets add magically on the mock when we check for its existance
        },
        'label': args.label,
        'provider_class': args.class_,
        'provider_type': args.type,
    }
    mock_edit.assert_called_with(expected_data, mock.ANY)
