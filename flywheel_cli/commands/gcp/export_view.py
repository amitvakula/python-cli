import argparse
import datetime
import re
import sys
import google.oauth2.credentials

from ...errors import CliError
from ...sdk_impl import create_flywheel_session
from ..view import lookup, lookup_view
from .auth import get_token
from .flywheel_gcp import GCP, GCPError
from .profile import get_profile
from google.cloud import bigquery



EXPORT_VIEW_DESC = """
Export saved Flywheel data-views by id or by label to a Google BigQuery table.
To find existing or create new saved data-views see `fw view help`.

examples:
  fw gcp export view \\
      --label 'flywheel/Anxiety Study/Subject Description' \\  # group/proj/view label
      'flywheel/Anxiety Study'                                # group/proj to run on
"""

def add_command(subparsers):
    parser = subparsers.add_parser('view',
        help='Export saved Flywheel data-views to Google BigQuery',
        description=EXPORT_VIEW_DESC,
        formatter_class=argparse.RawTextHelpFormatter)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--id',
        help='Export data-view by id')
    group.add_argument('--label', metavar='VIEW',
        help='Export data-view by label (group[/proj]/label or user/label)')

    parser.add_argument('container', metavar='CONTAINER',
        help='Container path to run data-view on (group/proj/subj/sess/acq)')

    # profile = get_profile()
    # project = profile.get('project')
    dataset = 'flywheel_views'

    parser.add_argument('--project', metavar='NAME',
        help='GCP project (default: {})'.format('project'))
    parser.add_argument('--dataset', metavar='NAME',
        help='Dataset (default: {})'.format('dataset'))
    parser.add_argument('--table', metavar='NAME', default=None,
        help='Table (default: username_YYYYMMDD_HHMMSS)')
    parser.add_argument('--bq_location', metavar='NAME',
        help='Export new BigQuery dataset location')


    parser.set_defaults(func=export_view)
    parser.set_defaults(parser=parser)
    return parser


def export_view(args):
    for param in ['project']:
        if not getattr(args, param, None):
            raise CliError(param + ' required')

    def create_bigquery_dataset(args, bigquery, bq_client):
        dataset = bigquery.Dataset('{}.{}'.format(args.project, args.bq_dataset))
        dataset.location = args.bq_location
        dataset = bq_client.create_dataset(dataset)

    credentials = google.oauth2.credentials.Credentials(get_token())
    bq_client = bigquery.Client(args.project, credentials)
    api = create_flywheel_session()
    if args.id:
        view = api.get('/views/' + args.id)
    else:
        view = lookup_view(args.label)
    params = {'containerId': lookup(args.container), 'format': 'csv'}
    print('Running saved data-view ' + view['_id'], file=sys.stderr)
    data = api.get('/views/' + view['_id'] + '/data', params=params)
    csv = '\n'.join('{},{}'.format(num or 'num', line) if line else ''
                    for num, line in enumerate(data.split('\n')) if line)
    if not args.table:
        user = api.get('/users/self')
        name = re.sub(r'\W', '', args.label or user['firstname'] + user['lastname'])
        args.table = datetime.datetime.now().strftime(name + '_%Y%m%d_%H%M%S')

    print('Exporting to BigQuery table ' + args.table, file=sys.stderr)
    gcp = GCP(get_token)
    datasets = bq_client.list_datasets()

    if args.dataset not in datasets:
        print('Creating dataset ' + args.dataset, file=sys.stderr)
        create_bigquery_dataset(args.project, args.dataset, bigquery, bq_client)
    gcp.bq.upload_csv(args.project, args.dataset, args.table, csv.encode('utf-8'))
