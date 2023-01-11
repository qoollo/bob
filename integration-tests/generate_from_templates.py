#!/usr/bin/python3
from re import T
import argparse, os, sys
from jinja2 import Template
from quantiphy import Quantity

def pathified(string):
    return str(os.path.abspath(rf'{string}'))

parser = argparse.ArgumentParser(description='This script generates dockerfile and configs for bob deploy, based on input arguments.')
parser.add_argument('-a', dest='amount_of_nodes', type=int, required=True, help='sets the amount of nodes to create.')
parser.add_argument('-v', dest='version', type=str, required=True, help='sets docker image version (qoollo/bob:x.x.x.y.z)')
parser.add_argument('--log-config', dest='log_config', type=str, default='/bob/configs/logger.bobnet.yaml', help='logger config file.')
parser.add_argument('--users-config', dest='users_config', type=str, default='/bob/configs/users.bobnet.yaml', help='logger config file.')
parser.add_argument('-q', dest='quorum', type=str, default=2, help='min count of successful operations on replicas to consider operation successful.')
parser.add_argument('--operation-timeout', dest='operation_timeout', type=str, default='3sec', help='timeout for every GRPC operation.')
parser.add_argument('--check-interval', dest='check_interval', type=str, default='5000ms', help='interval for checking connections.')
parser.add_argument('--cluster-policy', dest='cluster_policy', type=str, default='quorum', choices=['simple', 'quorum'], help='simple - without checking status.')
parser.add_argument('--backend-type', dest='backend_type', type=str, default='pearl', choices=['in_memory', 'stub', 'pearl'], help='type of the backend.')
parser.add_argument('--cleanup-interval', dest='cleanup_interval', type=str, default='1h', help='interval for checking for blobs cleanup.')
parser.add_argument('--open-blobs-soft-limit', dest='open_blobs_soft_limit', type=int, default=2, help='soft limit for count of max blobs to remain in ram.')
parser.add_argument('--open-blobs-hard-limit', dest='open_blobs_hard_limit', type=int, default=10, help='hard limit for count of max blobs to remain in ram.')
parser.add_argument('--http-api-port', dest='http_api_port', type=int, default=8000, help='http port for api.')
parser.add_argument('-l', dest='bloom_filter_memory_limit', type=str, default='8 GiB', help='memory limit for all bloom filters. Unlimited if not specified.')
parser.add_argument('-u', dest='auth_type', type=str, default='None', choices=['Basic', 'None'], help='auth type for bob')
parser.add_argument('--index-memory-limit', dest='index_memory_limit', type=str, default='8 GiB', help='memory limit for all indexes')
parser.add_argument('--enable-aio', dest='enable_aio', type=str, default='true', choices=['true', 'false'], help='enables linux AIO.')
parser.add_argument('-p', dest='disks_events_logfile', type=str, default='/bob/log/bob_events.csv', help='path to logfile with info about disks states switches.')
parser.add_argument('-b', dest='max_blob_size', type=str, default='100mb')
parser.add_argument('--allow-duplicates', dest='allow_duplicates', type=str, default='true', choices=['true', 'false'], help='disables search for existing keys before write.')
parser.add_argument('-d', dest='max_data_in_blob', type=int, default=10000)
parser.add_argument('--blob-file-name-prefix', dest='blob_file_name_prefix', type=str, default='bob')
parser.add_argument('--fail-retry-timeout', dest='fail_retry_timeout', type=str, default='100ms', help='retry to reinit pearl backend after fail.')
parser.add_argument('--alien-disk', dest='alien_disk', type=str, default='d1')
parser.add_argument('--bloom-filter-max-buf-bits-count', dest='bloom_filter_max_buf_bits_count', type=int, default=10000, 
help='sets bloom filter buffer size in bits count, best value ~= max_data_in_blob.')
parser.add_argument('--root-dir-name', dest='root_dir_name', type=str, default='bob')
parser.add_argument('--alien-root-dir-name', dest='alien_root_dir_name', type=str, default='/bob/data/d1/alien')
parser.add_argument('-t', dest='timestamp_period', type=str, default='1m', help='period when new pearl directory created.')
parser.add_argument('--create-pearl-wait-delay', dest='create_pearl_wait_delay', type=str, default='100ms', help='each thread will wait this period if another thread creating pearl.')
parser.add_argument('--metrics-name', dest='metrics_name', type=str, default='bob', help='add base name for metrics.')
parser.add_argument('--graphite-enabled', dest='graphite_enabled', type=str, default='false', choices=['true', 'false'])
parser.add_argument('--prometheus-enabled', dest='prometheus_enabled', type=str, default='false', choices=['true', 'false'])
parser.add_argument('--path', dest='path', type=str, help='sets path to directory where configs will be generated.', default='/tmp')
parser.add_argument('-transport_min_port', dest='transport_min_port', type=int, required=True, help='Port of the first bob container.')
parser.add_argument('--user', dest='auth_user', type=str, help='Username for bob basic authentification', default='admin')
parser.add_argument('--password', dest='auth_password', type=str, help='Password for bob basic authentification', default='password')
parser.add_argument('-rest_min_port', dest='rest_min_port', type=int, required=True, help='Rest api port for the first node.')

args = parser.parse_args()

path = os.path.join(pathified(args.path),'generated_configs')

os.makedirs(path, exist_ok=True, mode=0o777)


if len(os.listdir(path)) != 0:
    sys.exit(f'Directory {path} not empty.')

try:
    original_umask = os.umask(0)
    for i in range(args.amount_of_nodes):
        os.makedirs(os.path.join(path, f'data{i}/d1'), exist_ok=True, mode=0o777)
finally:
    os.umask(original_umask)


#generate compose file

with open("Templates/compose_template.yml.j2", ) as compose:
    template = Template(compose.read())
    with open(os.path.join(path, 'docker-compose.yml'), 'w') as f:
        f.write(template.render(amount_of_nodes=args.amount_of_nodes, version=args.version, path=path, transport_min_port=args.transport_min_port, rest_min_port=args.rest_min_port))
        f.close

#generate node files
for item in range(args.amount_of_nodes):
    with open("Templates/node_template.yml.j2") as node: 
        template = Template(node.read())
        with open(os.path.join(path, f'node.yaml.bobnet{item}'), 'w') as f:
            f.write(template.render(vars(args), node_number=item))
            f.close

#generate cluster config
with open("Templates/cluster_template.yml.j2") as cluster: 
    template = Template(cluster.read())
    with open(os.path.join(path, 'cluster.yaml.bobnet'), 'w') as f:
        f.write(template.render(amount_of_nodes=args.amount_of_nodes))
        f.close

#generate logger config
with open("Templates/logger_template.yml.j2") as logger:
    template = Template(logger.read())
    with open(os.path.join(path, 'logger.bobnet.yaml'), 'w') as f:  
        f.write(template.render(path="/bob/log"))
        f.close

#generate users config
with open("Templates/users_template.yml.j2") as users:
    template = Template(users.read())
    with open(os.path.join(path, 'users.bobnet.yaml'), 'w') as f:
        f.write(template.render(auth_user=args.auth_user, auth_password=args.auth_password))
        f.close