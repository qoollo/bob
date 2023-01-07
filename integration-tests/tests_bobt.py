#!/usr/bin/python3

import subprocess, argparse, shlex, sys, re

def make_run_args(args):
     return {'-c':args.count, '-s':args.start, '-e':args.end, '-a':f'http://127.0.0.1:{args.rest_min_port}', '--user':args.user, '--password':args.password}

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
parser.add_argument('-rest_min_port', dest='rest_min_port', type=int, required=True, help='Rest api port for the first node.')
parser.add_argument('--user', dest='user', type=str, help='Username for bob basic authentification')
parser.add_argument('--password', dest='password', type=str, help='Password for bob basic authentification')

parsed_args = parser.parse_args()

bobt_args = args_to_str(make_run_args(parsed_args))

try:
    #run bobt
    print(f'Running bobt {bobt_args.rstrip()}')
    p = subprocess.run(shlex.split(f'./bobt {bobt_args.rstrip()}'), capture_output=True).stdout.decode('ascii')
    print(str(p))
    #find all of summaries in output
    found_summaries = re.search(r'\bFinal\ssummary:\s[0-9]{1,}\/[0-9]{1,}\b', str(p))
    #exit if no summaries
    if not found_summaries:
        sys.exit('No bobt output found.')
    #find the last Summary and get its values
    summary = found_summaries.group(0).replace('Final summary: ', '').split('/')
    if summary[0] != summary[1]:
        sys.exit(f'Test failed, captured summary has incomplete score: {summary[0]} of {summary[1]}')
    else:
        print(f'Test succeeded: {summary[0]}/{summary[1]}')
except subprocess.CalledProcessError as e:
    sys.exit(str(e.stderr))