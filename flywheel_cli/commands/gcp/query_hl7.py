import argparse
import sys

from .auth import get_token
from .profile import get_profile, create_profile_object
from ...errors import CliError
from healthcare_api.client import Client, base

QUERY_HL7_DESC = """
Search for HL7 messages in Healthcare API.
You can use Healthcare API provided query language. See the filter query param in the docs:
https://cloud.google.com/healthcare/docs/reference/rest/v1alpha2/projects.locations.datasets.hl7V2Stores.messages/list

examples:
  fw gcp query hl7 --dataset ds --hl7store hl7 'PatientId("XYZ", "MR")'
  fw gcp query hl7 'PatientId("XYZ", "MR")'
"""


def add_command(subparsers):
    parser = subparsers.add_parser('hl7',
                                   help='Query HL7 messages in Healthcare API',
                                   description=QUERY_HL7_DESC,
                                   formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--project', metavar='NAME',
                        help='GCP project (defaults to your GCP profile project)')
    parser.add_argument('--location', metavar='NAME',
                        help='Location (defaults to your GCP profile location)')
    parser.add_argument('--dataset', metavar='NAME',
                        help='Dataset (defaults to your GCP profile dataset)')
    parser.add_argument('--hl7store', metavar='NAME',
                        help='FHIR store (defaults to your GCP profile hl7store)')
    parser.add_argument('query', metavar='QUERY', nargs=argparse.REMAINDER,
                        help='HL7 filter query, kindly provide it in quotes ('')')

    parser.set_defaults(func=query_hl7)
    parser.set_defaults(parser=parser)
    return parser


def query_hl7(args):
    profile = get_profile()
    profile_object = create_profile_object('hl7Store', profile, args)
    hc_client = Client(get_token)
    store_name = 'projects/{project}/locations/{location}/datasets/{dataset}/hl7V2Stores/{hl7store}'.format(**profile_object)
    resp = hc_client.list_hl7v2_messages(store_name, filter_=args.query)
    ids = list(map(lambda x: x.split('/')[-1], resp['messages']))
    summary = 'Query matched {} resources'.format(len(ids))
    print(summary, file=sys.stderr)
    for ref in ids:
        print(ref)
