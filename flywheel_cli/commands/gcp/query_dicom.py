import argparse
import itertools
import sys
import json
import google.oauth2.credentials

from healthcare_api.client import Client, base
from .auth import get_token
from .profile import get_profile
from ...errors import CliError
from google.cloud import bigquery


QUERY_DICOM_DESC = """
Run assisted SQL query (only WHERE clause needed) on a BigQuery table exported
from Healthcare API. Assuming that Healthcare API dataset/dicomstore matches
BigQuery dataset/table name.

If a BigQuery table with the same name as the dicomstore doesn't exist yet
(or using `--export`), then the dicomstore is exported before the query.

examples:
  fw gcp query dicom --dataset ds --dicomstore ds 'AccessionNumber="test"'
  fw gcp query dicom 'PatientSex="M" and PatientAge>"040Y"'
"""

# get main cols of the 1st 100 series matching a given where clause
SQL_TEMPLATE = """
SELECT StudyInstanceUID, SeriesInstanceUID,
  COUNT(DISTINCT SOPInstanceUID) AS image_count,
  MIN(AccessionNumber) AS AccessionNumber,
  MIN(PatientID) AS PatientID,
  MIN(StudyID) AS StudyID,
  MIN(StudyDate) AS StudyDate,
  MIN(StudyTime) AS StudyTime,
  MIN(StudyDescription) AS StudyDescription,
  MIN(SeriesDate) AS SeriesDate,
  MIN(SeriesTime) AS SeriesTime,
  MIN(SeriesDescription) AS SeriesDescription
FROM {dataset}.{table}
WHERE {where}
GROUP BY StudyInstanceUID, SeriesInstanceUID
ORDER BY StudyInstanceUID, SeriesInstanceUID
"""


def add_command(subparsers):
    parser = subparsers.add_parser('dicom',
        help='Query dicoms in Healthcare API via BigQuery',
        description=QUERY_DICOM_DESC,
        formatter_class=argparse.RawTextHelpFormatter)
    group = parser.add_mutually_exclusive_group()

    # profile = get_profile()
    # if not profile:
    #     raise CliError('Please create your GCP profile first!')
    # print(profile)
    # base_path_elements = profile['dicomStore'].split('/')[1::2]
    # print(base_path_elements)
    # project = profile.get('project')
    # location = profile.get('location')
    # dataset = profile.get('hc_dataset')
    # dicomstore = profile.get('hc_dicomstore')

    parser.add_argument('--project', metavar='NAME', default='project',
        help='GCP project (default: {})'.format('project'))
    parser.add_argument('--location', metavar='NAME', default='location',
        help='Location (default: {})'.format('location'))
    parser.add_argument('--dataset', metavar='NAME', default='dataset',
        help='Dataset (default: {})'.format('dataset'))
    parser.add_argument('--dicomstore', metavar='NAME', default='dicomstore',
        help='Dicomstore / table (default: {})'.format('dicomstore'))
    parser.add_argument('--export', action='store_true',
        help='Export to BigQuery first (even if table exists)')
    parser.add_argument('--bq_dataset', metavar='NAME',
        help='Export to new BigQuery dataset (requires --export)')
    parser.add_argument('--bq_table', metavar='NAME',
        help='Export to new BigQuery table (requires --bq_dataset)')
    parser.add_argument('--bq_location', metavar='NAME',
        help='Export new BigQuery dataset location')
    parser.add_argument('--studies', action='store_true',
        help='Return studies only (hide series)')
    parser.add_argument('--uids', action='store_true',
        help='Return UIDs only (useful for import)')
    group.add_argument('--sql', metavar='SQL WHERE',
        help='SQL WHERE clause. Kindly provide it as a string')
    group.add_argument('--all', action='store_true',
        help='Return all records from BigQuery table')

    parser.set_defaults(func=query_dicom)
    parser.set_defaults(parser=parser)
    return parser


def query_dicom(args):
    for param in ['project', 'location', 'dataset', 'dicomstore']:
        if not getattr(args, param, None):
            raise CliError(param + ' required')

    def list_table_ids(tables):
        return [table.table_id for table in tables]

    def parse_query_result(query_job, query_result):
        result = {
            'query_id': query_job.job_id,
            'rows': [row for row in query_result]
            }
        return result

    def parse_query_condition(sql):
        if len(sql) == 0:
            return
        else:
            where = args.sql[1].split('=')
            return '{}="{}"'.format(where[0], where[1])

    def create_bigquery_dataset(args, bigquery, bq_client):
        dataset = bigquery.Dataset('{}.{}'.format(args.project, args.bq_dataset))
        dataset.location = args.bq_location
        dataset = bq_client.create_dataset(dataset)

    def export_controller(dataset_list, args, bigquery, bq_client):
        datasets = []
        for dataset in dataset_list:
            datasets.append(dataset.dataset_id)
        if args.bq_dataset in datasets:
            print('dataset already exists')
            hc_client.export_dicom_to_bigquery(store_name, 'bq://{}.{}.{}'.format(args.project, args.bq_dataset, args.bq_table or args.dicomstore))
        elif args.bq_dataset:
            print('new dataset detected')
            create_bigquery_dataset(args.project, args.bq_dataset, bigquery, bq_client)
            hc_client.export_dicom_to_bigquery(store_name, 'bq://{}.{}.{}'.format(args.project, args.bq_dataset, args.bq_table or args.dicomstore))
        elif args.dataset in datasets:
            print('dataset ok, but no table, creating...')
            hc_client.export_dicom_to_bigquery(store_name, 'bq://{}.{}.{}'.format(args.project, args.dataset, args.dicomstore))
        else:
            print('dataset not in bq yet, creating...')
            create_bigquery_dataset(args.project, args.dataset, bigquery, bq_client)
            hc_client.export_dicom_to_bigquery(store_name, 'bq://{}.{}.{}'.format(args.project, args.dataset, args.dicomstore))

    def create_where_clause(args):
        if args.all:
            return '1=1'
        elif args.sql:
            return ''.join(args.sql)
        else:
            where = input('Kindly provide your SQL WHERE clause: ')
            return where

    credentials = google.oauth2.credentials.Credentials(get_token())
    hc_client = Client(get_token)
    bq_client = bigquery.Client(args.project, credentials)
    profile = get_profile()

    store_name = profile['dicomStore']
    # print(store_name)
    # store_name = 'projects/{}/locations/{}/datasets/{}/dicomStores/{}'.format(args.project, args.location, args.dataset, args.dicomstore)
    tables = bq_client.list_tables('{}.{}'.format(args.project, args.dataset))

    if args.export or args.dicomstore not in list_table_ids(tables):
        print('Exporting Healthcare API dicomstore to BigQuery')
        export_controller(list(bq_client.list_datasets()), args, bigquery, bq_client)

    # TODO enable raw queries (?)
    query = SQL_TEMPLATE.format(dataset=args.dataset, table=args.dicomstore, where=create_where_clause(args))
    try:
        query_job = bq_client.query(query)  # API request
        query_result = query_job.result()
        result = parse_query_result(query_job, query_result)
    except base.GCPError as ex:
        raise CliError(str(ex))
    except Exception as e: raise

    hierarchy = result_to_hierarchy(result)
    summary = 'Query matched {total_studies} studies, {total_series} series, {total_images} images'.format(**hierarchy)
    print(summary, file=sys.stderr)

    if args.uids:
        print_uids(hierarchy, studies_only=args.studies)
    else:
        print_tree(hierarchy, studies_only=args.studies)


def result_to_hierarchy(result):
    total_studies = 0
    total_series = 0
    total_images = 0
    studies = []

    for _, rows in itertools.groupby(result['rows'], key=lambda row: row['StudyInstanceUID']):
        total_studies += 1
        series = []
        for row in rows:
            total_series += 1
            total_images += int(row['image_count'])
            series.append({
                'SeriesDate': row['SeriesDate'],
                'SeriesTime': row['SeriesTime'],
                'SeriesInstanceUID': row['SeriesInstanceUID'],
                'SeriesDescription': row['SeriesDescription'],
                'image_count': int(row['image_count']),
            })

        # use last iterated `row` to get study properties
        studies.append({
            'StudyDate': row.get('StudyDate'),
            'StudyTime': row.get('StudyTime'),
            'StudyInstanceUID': row.get('StudyInstanceUID'),
            'StudyDescription': row.get('StudyDescription'),
            'series_count': len(series),
            'series': sorted(series, key=lambda s: (s['SeriesDate'], s['SeriesTime']), reverse=True),
            'subject': row['PatientID'].rpartition('@')[0] or 'ex' + row['StudyID'],
        })

    return {
        'query_id': result['query_id'],
        'total_studies': total_studies,
        'total_series': total_series,
        'total_images': total_images,
        'studies': sorted(studies, key=lambda s: (s['StudyDate'], s['StudyTime']), reverse=True),
    }


def print_uids(hierarchy, studies_only=False):
    for study in hierarchy['studies']:
        if studies_only:
            print(study['StudyInstanceUID'])
        else:
            for series in study['series']:
                print(series['SeriesInstanceUID'])


def print_tree(hierarchy, studies_only=False):
    if studies_only:
        study_template = ' {StudyInstanceUID} ({StudyDate}, {StudyDescription})\n'
    else:
        study_template = ' {StudyInstanceUID} ({StudyDate}, {StudyDescription}, {series_count} series)\n'
        series_template = ' {SeriesInstanceUID} ({SeriesDescription}, {image_count} images)\n'

    # tree formatting prefixes
    PFX_SINGLE   = '──'
    PFX_FIRST    = '┬──'
    PFX_MID      = '├──'
    PFX_LAST     = '└──'
    PAR_PFX_MID  = '│   '
    PAR_PFX_LAST = '    '

    last_study_num = hierarchy['total_studies'] - 1
    for study_num, study in enumerate(hierarchy['studies']):
        last_study = study_num == last_study_num
        if last_study_num == 0:
            prefix = PFX_SINGLE
        elif study_num == 0:
            prefix = PFX_FIRST
        elif last_study:
            prefix = PFX_LAST
        else:
            prefix = PFX_MID

        sys.stdout.write(prefix + study_template.format(**study))

        if not studies_only:
            last_series_num = study['series_count'] - 1
            for series_num, series in enumerate(study['series']):
                last_series = series_num == last_series_num
                prefix = PAR_PFX_LAST if last_study else PAR_PFX_MID
                prefix += PFX_LAST if last_series else PFX_MID
                sys.stdout.write(prefix + series_template.format(**series))
