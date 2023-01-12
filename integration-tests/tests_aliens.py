#!/usr/bin/python3

import subprocess, argparse, shlex, sys, re
from time import sleep
from python_on_whales import docker as d_cli
from python_on_whales.exceptions import *
from retry import *
from bob_backend_timer import ensure_backend_up

run_options = ['get']

def make_run_args(args, offset, count):
    return {'-c':count, '-l':args.payload, '-h':f'{args.node}', '-f':str(int(args.first) + offset), '-t':args.threads, '--mode':args.mode, '-k':args.keysize,
     '-p':args.transport_min_port, '--user':args.user, '--password':args.password} 

def args_to_str(args_dict):
    bobp_args_str = str()
    for key in args_dict: 
        if args_dict.get(key) != None:
            bobp_args_str += f'{key} {args_dict.get(key)} '
    return bobp_args_str

def run_tests(behaviour, args):
    try:
        print(f'Running bobp -b {str(behaviour)} {args.rstrip()}')
        p = subprocess.check_output(shlex.split(f'./bobp -b {behaviour} {args.rstrip()}')).decode('ascii')
        print(str(p))
        if behaviour == 'get':
            if not 'total err: 0' in str(p):
                sys.exit(f'{behaviour} test failed, see output')
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

parser = argparse.ArgumentParser(description='This script launches bob tests with given configuration.')
parser.add_argument('-c', dest='count', type=int, help='amount of entries to process', required=True)
parser.add_argument('-l', dest='payload', type=int, help='payload in bytes', required=True)
parser.add_argument('-n', dest='node', type=str, help='target node address', required=True)
parser.add_argument('-f', dest='first', type=int, help='first index', default=0)
parser.add_argument('-t', dest='threads', type=int, help='amount of working threads', default=1)
parser.add_argument('--mode', dest='mode', type=str, help='random or normal', choices=['random', 'normal'], default='normal')
parser.add_argument('-k', dest='keysize', type=int, help='size of binary key (8 or 16)', choices=[8, 16], default=8)
parser.add_argument('--user', dest='user', type=str, help='Username for bob basic authentification')
parser.add_argument('--password', dest='password', type=str, help='Password for bob basic authentification')
parser.add_argument('-nodes_amount', dest='nodes_amount', type=int, required=True, help='Amount of bob nodes.')
parser.add_argument('-rest_min_port', dest='rest_min_port', type=int, required=True, help='Rest api port for the first node.')
parser.add_argument('-transport_min_port', dest='transport_min_port', type=int, required=True, help='Port of the first bob container.')
parser.add_argument('--cluster_start_waiting_time', dest='cluster_start_waiting_time', type=float, required=True, help='Offset for bobp.')
parsed_args = parser.parse_args()

#check if count is more than nodes amount
if parsed_args.count < parsed_args.nodes_amount:
    sys.exit('Amount of records cannot be less than nodes amount.')

record_amount = parsed_args.count // parsed_args.nodes_amount

#get container object mapping to ports
container_dict = {}
try:
    for i in range(parsed_args.nodes_amount):
        port_num = str(parsed_args.transport_min_port + i)
        container_dict[port_num] = str(d_cli.container.list(filters={'publish':f'{port_num}'})[0].id)
except KeyError:
    sys.exit('Nodes amount is not set.')
except ValueError:
    sys.exit('Amount of nodes has unexpected value.')

#runs put and stops nodes in cycle
written_count = 0
try:
    for i in range(1, parsed_args.nodes_amount + 1):
        #make correctly formatted args 
        dict_args = make_run_args(parsed_args, written_count, record_amount)
        bobp_args = args_to_str(dict_args)
        #run put
        print(f'Running bobp -b put {bobp_args.rstrip()}')
        p = subprocess.check_output(shlex.split(f'./bobp -b put {bobp_args.rstrip()}')).decode('ascii')
        print(str(p))
        if not 'total err: 0' in str(p):
            sys.exit(f'Put test failed, see output.')
        written_count += dict_args.get('-c')
        if not i == parsed_args.nodes_amount:
            #stops one
            sleep(10)
            d_cli.container.stop(container_dict[str(parsed_args.transport_min_port + i)])
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
    ensure_backend_up(parsed_args.nodes_amount, parsed_args.rest_min_port)
except ValueError:
    sys.exit('Amount of nodes has unexpected value.')

sleep(float(parsed_args.cluster_start_waiting_time)/1000 + 1)


dict_args = make_run_args(parsed_args, 0, str(written_count))
bobp_args = args_to_str(dict_args)
for item in run_options:
    run_tests(item, bobp_args)