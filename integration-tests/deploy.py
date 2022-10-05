#!/usr/bin/python3
import os, argparse, sys, subprocess, shlex, docker, python_on_whales
from time import sleep
from python_on_whales import docker as d_cli
from docker import errors as d_err
from docker import types as d_types

#collect arguments
parser = argparse.ArgumentParser(description='Deploys docker compose nodes.')

parser.add_argument('--path', dest='path', type=str, required=True, help='Takes in path to generated configs.')
parser.add_argument('-r', dest='replicas', type=int, required=True, help='Sets amount of replicas to create in cluster.')

exclusive = parser.add_mutually_exclusive_group(required=True)
exclusive.add_argument('-d', dest='vdisks_count', nargs='?', type=int, help='min - equal to number of pairs node-disk.')
exclusive.add_argument('-p', dest='vdisks_per_disk', nargs='?', type=int, help='number of vdisks per physical disk.')

args = parser.parse_args()
final_args = {'-r':args.replicas, '-d':args.vdisks_count, '-p':args.vdisks_per_disk}

#transform args to a string format
args_str = str()
for (key) in final_args:
    if final_args.get(key) != None:
        args_str += f'{key} {final_args.get(key)} '

#initilize docker client
client = docker.from_env()

#check for network existing and creating it
try:
    bobnet = client.networks.get('bob_net')
except d_err.NotFound:
    ipam_config = d_types.IPAMConfig(pool_configs=[d_types.IPAMPool(subnet='172.21.0.0/24', gateway='172.21.0.1')])
    bobnet = client.networks.create('bob_net', driver='bridge', ipam=ipam_config, attachable=True)

start_path = os.getcwdb()
good_path = os.path.abspath(args.path)

#check for configs to exist
try:
    if not 'cluster.yaml.bobnet' in os.listdir(good_path):
        sys.exit('Cluster config not found in the specifed directory.')
except FileNotFoundError as e:
    sys.exit(e)

#change execution permissions for binaries
try:
    os.chmod(path=f'./ccg', mode=0o771)
    os.chmod(path=f'./bobp', mode=0o771)
except OSError as e:
    sys.exit(e)

#run cluster generation
try:
    pr = subprocess.check_output(shlex.split(f'./ccg new -i {args.path}/cluster.yaml.bobnet -o {args.path}/cluster.yaml.bobnet {args_str.rstrip()}'))
    if str(pr).find('ERROR') != -1:
        sys.exit(str(pr))
except subprocess.CalledProcessError:
    sys.exit(pr.stderr)


try:
    os.chdir(good_path)
except FileNotFoundError:
    sys.exit('The path does not exist.')
except PermissionError:
    sys.exit(f'Access to {good_path} is denied.')
except NotADirectoryError:
    sys.exit('The specified path is not a directory.')

#run docker containers
try:
    client.networks.get('bob_net')
    d_cli.compose.up(detach=True)
    print(f'Services are initilized:\n{d_cli.container.list()}')
except d_err.NotFound:
    sys.exit('Docker network not found')

try:
    if len(d_cli.container.list()) < os.environ['BOB_NODES_AMOUNT']:
        sys.exit('One or more bob docker containers are not running.')
except KeyError:
    sys.exit('Nodes amount is not set.')




