#!/usr/bin/python3

import subprocess, argparse, shlex, sys, os
from python_on_whales import docker as d_cli
from python_on_whales.exceptions import *
from retry import *
from bob_backend_timer import ensure_backend_up

try:
    bob_nodes_amount_string = os.environ['BOB_NODES_AMOUNT']
except KeyError:
    sys.exit('Nodes amount is not set.')

def make_run_args(args, offset):
    return {'-c':args.count, '-l':args.payload, '-h':f'{args.node}', '-f':str(int(args.first) + offset), '-t':args.threads, '--mode':args.mode, '-k':args.keysize,
     '-p':str(int(bob_nodes_amount_string) + 20000 - 1)} 

def args_to_str(args_dict):
    bobp_args_str = str()
    for key in args_dict: 
        if args_dict.get(key) != None:
            bobp_args_str += f'{key} {args_dict.get(key)} '
    return bobp_args_str

parser = argparse.ArgumentParser(description='This script launches bob tests with given configuration.')
parser.add_argument('-c', dest='count', type=int, help='amount of entries to process', required=True)
parser.add_argument('-l', dest='payload', type=int, help='payload in bytes', required=True)
parser.add_argument('-n', dest='node', type=str, help='target node address', required=True)
parser.add_argument('-f', dest='first', type=int, help='first index', default=0)
parser.add_argument('-t', dest='threads', type=int, help='amount of working threads', default=1)
parser.add_argument('--mode', dest='mode', type=str, help='random or normal', choices=['random', 'normal'], default='normal')
parser.add_argument('-k', dest='keysize', type=int, help='size of binary key (8 or 16)', choices=[8, 16], default=8)

parsed_args = parser.parse_args()

#get container object mapping to ports
container_dict = {}
try:
    for i in range(int(bob_nodes_amount_string)):
        port_num = str(20000+i)
        container_dict[port_num] = str(d_cli.container.list(filters={'publish':f'{port_num}'})[0].id)
except KeyError:
    sys.exit('Nodes amount is not set.')
except ValueError:
    sys.exit('Amount of nodes has unexpected value.')

#runs put and stops nodes in cycle
written_count = 0
try:
    for i in range(int(bob_nodes_amount_string) - 1):
        #make correctly formatted args 
        dict_args = make_run_args(parsed_args, written_count)
        bobp_args = args_to_str(dict_args)
        #run put
        try:
            print(f'Bob logs on write node:\n{d_cli.container.logs(container_dict[dict_args["-p"]])}')
        except DockerException as e:
            sys.exit(e.stderr)
        print(f'Running bobp -b put {bobp_args.rstrip()}')
        p = subprocess.check_output(shlex.split(f'./bobp -b put {bobp_args.rstrip()}')).decode('ascii')
        print(str(p))
        if f'put errors:' in str(p) or f'panicked' in str(p):
            sys.exit(f'Put test failed, see output.')
        written_count += dict_args.get('-c')
        #stops one
        d_cli.container.stop(container_dict[str(20000 + i)])
        print(f'Bob node {i} stopped.\n')
        stopped_list = d_cli.container.list(filters={"status":"exited"})
        print('Stopped containers:\n')
        for i in range(len(stopped_list)):
            print(f'{stopped_list[i].id}\n') 
except subprocess.CalledProcessError as e:
    sys.exit(str(e.stderr))

try:
    for key, value in container_dict.items():
        print(f'Starting node on port {key}...')
        d_cli.container.start(value)
except DockerException as e:
    sys.exit(e.stderr)

try:
    ensure_backend_up(int(bob_nodes_amount_string))
except ValueError:
    sys.exit('Amount of nodes has unexpected value.')

try:
    dict_args = make_run_args(parsed_args, 0)
    dict_args['-c'] = str(written_count)
    bobp_args = args_to_str(dict_args)
    print(f'Running bobp -b get {bobp_args.rstrip()}')
    p = subprocess.check_output(shlex.split(f'./bobp -b get {bobp_args.rstrip()}')).decode('ascii')
    print(str(p))
    if f'get errors:' in str(p) or f'panicked' in str(p):
            sys.exit(f'Get test failed, see output.')
except subprocess.CalledProcessError as e:
    sys.exit(str(e.stderr))

