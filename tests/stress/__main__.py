from argparse import ArgumentParser
import sys
from importlib import import_module
from inspect import signature

def main():
    parser = ArgumentParser(description='Stress testing for Diskquota')

    # Test case is the first positional argument
    parser.add_argument('test_case')
    args = parser.parse_known_args(sys.argv)

    # Import module dynamically according to the argument
    test_case = import_module(args['test_case'])

    # Parse args of the run() function
    for arg in signature.parameters(test_case.run):
        parser.add_argument(f'--{arg}', required=True)
    args = parser.parse_args(sys.argv)

    # Call the run() function to do the job
    test_case.run(**vars(args))

if __name__ == '__main__':
    main()