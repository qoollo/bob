#!/usr/bin/python3

import subprocess, argparse, shlex, sys, re, os
from time import sleep

run_options = ['put','get','exist']

def run_tests(behaviour, args):
    try:
        print(f'Running bobp -b {str(behaviour)} {args.rstrip()}')
        p = subprocess.check_output(shlex.split(f'./bobp -b {behaviour} {args.rstrip()}')).decode('ascii')
        print(str(p))
        if behaviour in {'put', 'get'}:
            if f'{behaviour} errors:' in str(p) or f'panicked' in str(p):
                sys.exit(f'{behaviour} test failed, see output')
            if behaviour == 'put':
                sleep(30)
        elif behaviour == 'exist':
            found_exist = re.search(r'\b[0-9]{1,}\sof\s[0-9]{1,}\b', str(p))
            if not found_exist:
                sys.exit(f"No {behaviour} output captured, check output")
            exists = found_exist.group(0).split(' of ')
            if exists[0] != exists[1]:
                sys.exit(f"{exists[0]} of {exists[1]} keys, {behaviour} test failed, see output")
            else:
                print(f"{exists[0]} of {exists[1]} keys")   
        else:
            sys.exit('Unknown behaviour.')     
    except subprocess.CalledProcessError as e:
        sys.exit(str(e.stderr))
    except Exception as e:
        sys.exit(str(e))

def get_run_args(mode, args, run_conf):
    return {'-c':args.count, '-l':args.payload, '-h':f'{args.node}', '-f':args.first, '-t':args.threads, '--mode':args.mode, '-k':args.keysize, '-p':run_conf.get(mode)}

parser = argparse.ArgumentParser(description='This script launches bob tests with given configuration.')
parser.add_argument('-c', dest='count', type=int, help='amount of entries to process', required=True)
parser.add_argument('-l', dest='payload', type=int, help='payload in bytes', required=True)
parser.add_argument('-n', dest='node', type=str, help='target node address', required=True)
parser.add_argument('-f', dest='first', type=int, help='first index', default=0)
parser.add_argument('-t', dest='threads', type=int, help='amount of working threads', default=1)
parser.add_argument('--mode', dest='mode', type=str, help='random or normal', choices=['random', 'normal'], default='normal')
parser.add_argument('-k', dest='keysize', type=int, help='size of binary key (8 or 16)', choices=[8, 16], default=8)
parser.add_argument('-nodes_amount', dest='nodes_amount', type=int, required=True, help='Amount of bob nodes.')
parser.add_argument('-min_port', dest='min_port', type=int, required=True, help='Port of the first bob container.')

parsed_args = parser.parse_args()

test_run_config = dict()
iter = 0
try:
    for item in run_options:
        test_run_config[item]=str(parsed_args.min_port + (iter % int(parsed_args.nodes_amount))) #used in get_run_args()
        iter += 1
except ValueError:
    sys.exit('Args had unexpected values.')

#run put/get/exist tests
for item in run_options:
    args_str = str()
    run_args = get_run_args(item, parsed_args, test_run_config)
    for key in run_args:
        if run_args.get(key) != None:
            args_str += f'{key} {run_args.get(key)} '
    run_tests(item, args_str)

