#!/usr/bin/python3

import subprocess, argparse, shlex, sys, os

parser = argparse.ArgumentParser(description='This script launches bob tests with given configuration.')
parser.add_argument('-c', dest='count', type=int, help='amount of entries to process', required=True)
parser.add_argument('-l', dest='payload', type=int, help='payload in bytes', required=True)
parser.add_argument('-n', dest='node', type=str, help='target node address', required=True)
parser.add_argument('-s', dest='start', type=int, help='start index', default=0)
parser.add_argument('-e', dest='end', type=int, help='end index')
parser.add_argument('-t', dest='threads', type=int, help='amount of working threads', default=1)
parser.add_argument('--mode', dest='mode', type=str, help='random or normal', choices=['random', 'normal'], default='normal')
parser.add_argument('-k', dest='keysize', type=int, help='size of binary key (8 or 16)', choices=[8, 16], default=8)

args = parser.parse_args()
final_args = {'-c':args.count, '-l':args.payload, '-h':args.node, '-s':args.start, '-e':args.end, '-t':args.threads, '--mode':args.mode, '-k':args.keysize}

args_str = str()
for (key) in final_args:
    if final_args.get(key) != None:
        args_str += f'{key} {final_args.get(key)} '


binaries_dir = f'../target/{os.getenv("TARGET")}/release'

#write/read tests
try:
    p = subprocess.check_output(shlex.split(f'{binaries_dir}/bobp -b put {args_str.rstrip()}'))
    print(str(p))
except subprocess.CalledProcessError as e:
    print(e)
    sys.exit()


try:
    p = subprocess.check_output(shlex.split(f'{binaries_dir}/bobp -b get {args_str.rstrip()}'))
    print(str(p))
except subprocess.CalledProcessError as e:
    print(e)
    sys.exit()

