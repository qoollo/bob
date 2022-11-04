#!/usr/bin/python3

import subprocess, argparse, shlex, sys

def make_run_args(args):
     return {'--count':args.count, '-s':args.start}

def args_to_str(args_dict):
    bobp_args_str = str()
    for key in args_dict: 
        if args_dict.get(key) != None:
            bobp_args_str += f'{key} {args_dict.get(key)} '
    return bobp_args_str

parser = argparse.ArgumentParser(description='This script launches bob tests with given configuration.')
parser.add_argument('-c', dest='count', type=int, help='amount of entries to process', required=True)
parser.add_argument('-s', dest='start', type=int, help='starting index', default=0)

parsed_args = parser.parse_args()

bobt_args = args_to_str(make_run_args(parsed_args))

try:
    print(f'Running bobt {bobt_args.rstrip()}')
    p = subprocess.check_output(shlex.split(f'./bobt {bobt_args.rsplit()}')).decode('ascii')
    print(str(p))
    #TODO: add bobt output handling 
except subprocess.CalledProcessError as e:
    sys.exit(str(e.stderr))