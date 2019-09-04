# add import statements

def add_command(subparsers, parents):
    parser = subparsers.add_parser('dicom', parents=parents, help='Import a folder of dicom files')
    #parser.add_argument('', help='')


    parser.set_defaults(func=) # function name
    parser.set_defaults(parser=parser)

    return parser

def func_name(args):
    