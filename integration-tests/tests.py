#!/usr/bin/python3

import subprocess, argparse, shlex, sys

def run_tests(behaviour, args):
    try:
        print(f'Running bobp -b {str(behaviour)} {args.rstrip()}')
        p = subprocess.check_output(shlex.split(f'./bobp -b {behaviour} {args.rstrip()}'))
        print(str(p))
        if str(p).__contains__(f'{behaviour} errors:') or str(p).__contains__(f'panicked'):
            print(f'{behaviour} test failed, see output')
            sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(e)
        sys.exit(1)

def get_run_args(mode, args):
    return {'-c':args.count, '-l':args.payload, '-h':f'{args.node}', '-s':args.start, '-e':args.end, '-t':args.threads, '--mode':args.mode, '-k':args.keysize, '-p':test_run_config.get(mode)}

test_run_config = {'put':'8000', 'get':'8001'}

parser = argparse.ArgumentParser(description='This script launches bob tests with given configuration.')
parser.add_argument('-c', dest='count', type=int, help='amount of entries to process', required=True)
parser.add_argument('-l', dest='payload', type=int, help='payload in bytes', required=True)
parser.add_argument('-n', dest='node', type=str, help='target node address', required=True)
parser.add_argument('-s', dest='start', type=int, help='start index', default=0)
parser.add_argument('-e', dest='end', type=int, help='end index')
parser.add_argument('-t', dest='threads', type=int, help='amount of working threads', default=1)
parser.add_argument('--mode', dest='mode', type=str, help='random or normal', choices=['random', 'normal'], default='normal')
parser.add_argument('-k', dest='keysize', type=int, help='size of binary key (8 or 16)', choices=[8, 16], default=8)

parsed_args = parser.parse_args()

#run put/get tests
for item in ['put','get']:
    args_str = str()
    run_args = get_run_args(item, parsed_args)
    for key in run_args:
        if run_args.get(key) != None:
            args_str += f'{key} {run_args.get(key)} '
    run_tests(item, args_str)

