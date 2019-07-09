import argparse
import sys

from healthcare_api.client import Client, base
from .auth import get_token
from .profile import get_profile, create_profile_object
from ...errors import CliError

QUERY_FHIR_DESC = """
Search for FHIR resources in Healthcare API.
You can use FHIR's query language. See: https://www.hl7.org/fhir/search.html

examples:
  fw gcp query fhir --dataset ds --fhirstore fs --type Patient 'family=Laird'
  fw gcp query fhir --type Patient 'family=Laird'
"""


def add_command(subparsers):
    parser = subparsers.add_parser('fhir',
                                   help='Query FHIR resources in Healthcare API',
                                   description=QUERY_FHIR_DESC,
                                   formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--project', metavar='NAME',
                        help='GCP project (defaults to your GCP profile project)')
    parser.add_argument('--location', metavar='NAME',
                        help='Location (defaults to your GCP profile location)')
    parser.add_argument('--dataset', metavar='NAME',
                        help='Dataset (defaults to your GCP profile dataset)')
    parser.add_argument('--fhirstore', metavar='NAME',
                        help='FHIR store (defaults to your GCP profile fhirstore)')
    parser.add_argument('--type', metavar='TYPE', required=True,
                        help='FHIR resource type')
    parser.add_argument('query', metavar='QUERY', nargs=argparse.REMAINDER,
                        help='FHIR search query')

    parser.set_defaults(func=query_fhir)
    parser.set_defaults(parser=parser)
    return parser


def query_fhir(args):

    def create_query_object(args):
        query_object = {}
        for elem in args.query:
            key_and_value = elem.split('=')
            query_object[key_and_value[0]] = key_and_value[1]
        return query_object

    profile = get_profile()
    query_object = create_profile_object('fhirStore', profile, args)
    query = create_query_object(args)
    store_name = 'projects/{}/locations/{}/datasets/{}/fhirStores/{}'.format(query_object['project'], query_object['location'], query_object['dataset'], query_object['fhirstore'])
    hc_client = Client(get_token)
    resp = hc_client.search_fhir_resources(store_name, args.type, **query)
    refs = []
    for resource in resp['entry']:
        refs.append('{}/{}'.format(resource['resource']['resourceType'], resource['resource']['id']))
    summary = 'Query matched {} resources'.format(len(refs))
    print(summary, file=sys.stderr)
    for ref in refs:
        print(ref)
