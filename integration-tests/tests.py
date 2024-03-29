#!/usr/bin/python3

import subprocess, argparse, shlex, re
from time import sleep
from misc_functions import print_then_exit

run_options = ['put','get','exist']

def run_tests(behaviour, args):
    try:
        print(f'Running bobp -b {str(behaviour)} {args.rstrip()}')
        p = subprocess.check_output(shlex.split(f'./bobp -b {behaviour} {args.rstrip()}')).decode('ascii')
        print(str(p))
        if behaviour in {'put', 'get'}:
            if not 'total err: 0' in str(p):
                print_then_exit(f'{behaviour} test failed, see output')
        elif behaviour == 'exist':
            found_exist = re.search(r'\b[0-9]{1,}\sof\s[0-9]{1,}\b', str(p))
            if not found_exist:
                print_then_exit(f"No {behaviour} output captured, check output")
            exists = found_exist.group(0).split(' of ')
            if exists[0] != exists[1]:
                print_then_exit(f"{exists[0]} of {exists[1]} keys, {behaviour} test failed, see output")
            else:
                print(f"{exists[0]} of {exists[1]} keys")
        else:
            print_then_exit('Unknown behaviour.')     
    except subprocess.CalledProcessError as e:
        print_then_exit(str(e.stderr))
    except Exception as e:
        print_then_exit(str(e))

def make_args(raw_args):
    args_str = ''
    for key in raw_args:
        if raw_args.get(key) != None:
            args_str += f'{key} {raw_args.get(key)} '
    return args_str

def run_doubled_exist_test(run_args, expected_exist_keys):
    try:
        run_args['-c'] = expected_exist_keys * 2 + 1
        run_args['-s'] = 5001
        run_args['-t'] = 1
        args = make_args(run_args)
        print(f'Running bobp -b exist {args.rstrip()}')
        p = subprocess.check_output(shlex.split(f'./bobp -b exist {args.rstrip()}')).decode('ascii')
        print(str(p))
        found_exist = re.search(r'\b[0-9]{1,}\sof\s[0-9]{1,}\b', str(p))
        if not found_exist:
            print_then_exit(f"No exist output captured, check output")
        exists = found_exist.group(0).split(' of ')
        if int(exists[0]) != expected_exist_keys:
            print_then_exit(f"{exists[0]} of {exists[1]} keys, expected {expected_exist_keys} of {exists[1]} instead, exist test failed, see output")
        else:
            print(f"{exists[0]} of {exists[1]} keys")
    except subprocess.CalledProcessError as e:
        print_then_exit(str(e.stderr))
    except Exception as e:
        print_then_exit(str(e))

def get_run_args(mode, args, run_conf):
    return {'-c':args.count, '-l':args.payload, '-h':f'{args.node}', '-f':args.first, '-t':args.threads, '--mode':args.mode, '-k':args.keysize, '-p':run_conf.get(mode), 
    '--user':args.user, '--password':args.password}

parser = argparse.ArgumentParser(description='This script launches bob tests with given configuration.')
parser.add_argument('-c', dest='count', type=int, help='amount of entries to process', required=True)
parser.add_argument('-l', dest='payload', type=int, help='payload in bytes', required=True)
parser.add_argument('-n', dest='node', type=str, help='target node address', required=True)
parser.add_argument('-f', dest='first', type=int, help='first index', default=0)
parser.add_argument('-t', dest='threads', type=int, help='amount of working threads', default=1)
parser.add_argument('--mode', dest='mode', type=str, help='random or normal', choices=['random', 'normal'], default='normal')
parser.add_argument('-k', dest='keysize', type=int, help='size of binary key (8 or 16)', choices=[8, 16], default=8)
parser.add_argument('-nodes_amount', dest='nodes_amount', type=int, required=True, help='Amount of bob nodes.')
parser.add_argument('-transport_min_port', dest='transport_min_port', type=int, required=True, help='Port of the first bob container.')
parser.add_argument('--user', dest='user', type=str, help='Username for bob basic authentification')
parser.add_argument('--password', dest='password', type=str, help='Password for bob basic authentification')

parsed_args = parser.parse_args()

test_run_config = dict()
iter = 0
try:
    for item in run_options:
        test_run_config[item]=str(parsed_args.transport_min_port + (iter % int(parsed_args.nodes_amount))) #used in get_run_args()
        iter += 1
except ValueError:
    print_then_exit('Args had unexpected values.')

#run put/get/exist tests
for item in run_options:
    args_str = str()
    run_args = get_run_args(item, parsed_args, test_run_config)
    args_str = make_args(run_args)
    run_tests(item, args_str)

#run doubled range exist
run_args = get_run_args(item, parsed_args, test_run_config)
run_doubled_exist_test(run_args, parsed_args.count)