import argparse
import sys

from healthcare_api.client import Client, base
from .auth import get_token
# from .flywheel_gcp import GCP
from .profile import get_profile
from ...errors import CliError

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

    def create_query_object(profile=None, args=None):
        if profile:
            if profile['hl7Store']:
                profile_elements = profile['hl7Store'].split('/')[1::2]
                return {
                    'project': profile_elements[0],
                    'location': profile_elements[1],
                    'dataset': profile_elements[2],
                    'hl7store': profile_elements[3]
                }
            else:
                print("Kindly provide hl7store in your profile!")
                sys.exit(1)
        else:
            for param in ['project', 'location', 'dataset', 'hl7store']:
                if not getattr(args, param, None):
                    raise CliError(param + ' required')
            return {
                'project': args.project,
                'location': args.location,
                'dataset': args.dataset,
                'hl7store': args.hl7store
            }

    profile = get_profile()
    query_object = create_query_object(profile, args)
    hc_client = Client(get_token)
    store_name = 'projects/{}/locations/{}/datasets/{}/hl7V2Stores/{}'.format(query_object['project'], query_object['location'], query_object['dataset'], query_object['hl7store'])
    resp = hc_client.list_hl7v2_messages(store_name, filter_=args.query)
    ids = list(map(lambda x: x.split('/')[-1], resp['messages']))
    summary = 'Query matched {} resources'.format(len(ids))
    print(summary, file=sys.stderr)
    for ref in ids:
        print(ref)
