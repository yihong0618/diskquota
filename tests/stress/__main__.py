from argparse import ArgumentParser
from importlib import import_module
from inspect import signature

import os.path
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

def main():
    parser = ArgumentParser(description='Stress testing for Diskquota')

    # Test case is the first positional argument
    parser.add_argument('test_case')
    args, unknowns = parser.parse_known_args()

    # Import module dynamically based on the argument
    print(args)
    test_case = import_module(args.test_case)

    # Parse args of the run() function of the test case
    parser = ArgumentParser()
    params = signature(test_case.run).parameters
    for arg in params:
        parser.add_argument(f'--{arg}', required=True, type=params[arg].annotation)
    args = parser.parse_args(unknowns)

    # Call the run() function to do the job
    test_case.run(**vars(args))

if __name__ == '__main__':
    main()
