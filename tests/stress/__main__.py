from argparse import ArgumentParser
import sys
from importlib import import_module
from inspect import signature

def main():
    parser = ArgumentParser(description='Stress testing for Diskquota')
    parser.add_argument('case')
    args = parser.parse_known_args(sys.argv)
    case = import_module(args['case'])

    # Parse args of the run() function
    for arg in signature.parameters(case.run):
        parser.add_argument(f'--{arg}', required=True)
    args = parser.parse_args(sys.argv)

    # Call the run() function to do the job
    case.run(**vars(args))

if __name__ == '__main__':
    main()