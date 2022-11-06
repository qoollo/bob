#!/usr/bin/python3

import subprocess, argparse, shlex, sys, re

def make_run_args(args):
     return {'-c':args.count, '-s':args.start, '-e':args.end, '-a':'http://127.0.0.1:8000'}

def args_to_str(args_dict):
    bobp_args_str = str()
    for key in args_dict: 
        if args_dict.get(key) != None:
            bobp_args_str += f'{key} {args_dict.get(key)} '
    return bobp_args_str

parser = argparse.ArgumentParser(description='This script launches bob tests with given configuration.')
parser.add_argument('-c', dest='count', type=int, help='amount of entries to process', required=True)
parser.add_argument('-s', dest='start', type=int, help='starting index', default=0)
parser.add_argument('-e', dest='end', type=int, help='last index', default=0)

parsed_args = parser.parse_args()

bobt_args = args_to_str(make_run_args(parsed_args))

try:
    #run bobt
    print(f'Running bobt {bobt_args.rstrip()}')
    p = subprocess.check_output(shlex.split(f'bobt {bobt_args.rstrip()}'), stderr=subprocess.STDOUT).decode('ascii')
    print(str(p))
    #find all of summaries in output
    found_summaries = re.findall(r'\bSummary:\s[0-9]{1,}\/[0-9]{1,}\b', str(p))
    #exit if no summaries
    if not found_summaries:
        sys.exit('No bobt output found.')
    #find the last Summary and get its values
    summary = str(found_summaries[len(found_summaries) - 1]).replace('Summary: ', '').split('/')
    if summary[0] != summary[1]:
        sys.exit('Test failed, captured summary has incomplete score.')
except subprocess.CalledProcessError as e:
    sys.exit(str(e.stderr))