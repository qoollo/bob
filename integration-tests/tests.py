#!/usr/bin/python3

from asyncio.subprocess import PIPE
import subprocess, argparse, shlex, sys, re

def run_tests(behaviour, args):
    try:
        print(f'Running bobp -b {str(behaviour)} {args.rstrip()}')
        p = subprocess.check_output(shlex.split(f'./bobp -b {behaviour} {args.rstrip()}')).decode('ascii')
        print(str(p))
        if behaviour in {'put', 'get'}:
            if f'{behaviour} errors:' in str(p) or f'panicked' in str(p):
                sys.exit(f'{behaviour} test failed, see output')
        elif behaviour == 'exist':
            print(f'Running bobp -b {str(behaviour)} {args.rstrip()}')
            p = subprocess.check_output(shlex.split(f'./bobp -b {behaviour} {args.rstrip()}')).decode('ascii')
            print(str(p))
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

def get_run_args(mode, args):
    return {'-c':args.count, '-l':args.payload, '-h':f'{args.node}', '-s':args.start, '-e':args.end, '-t':args.threads, '--mode':args.mode, '-k':args.keysize, '-p':test_run_config.get(mode)}

test_run_config = {'put':'20000', 'get':'20001', 'exist':'20002'}

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

#run put/get/exist tests
for item in ['put','get','exist']:
    args_str = str()
    run_args = get_run_args(item, parsed_args)
    if item == 'exist':
        run_args.pop('-s')
    for key in run_args:
        if run_args.get(key) != None:
            args_str += f'{key} {run_args.get(key)} '
    run_tests(item, args_str)

