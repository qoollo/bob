import requests, sys, json
from retry import *

def print_then_exit(exit_str):
    print(exit_str)
    sys.exit(exit_str)

def request_metrics(port):
    try:
        return requests.get(f"http://127.0.0.1:{port}/metrics")
    except requests.RequestException:
        raise UserWarning('Failed to get metrics, retrying...')
    
         
@retry((UserWarning), tries=10, delay=1, backoff=1.75, max_delay=15)
def ensure_backend_up(bob_nodes_amount, rest_min_port):
    try:
        for item in range(bob_nodes_amount):
            response = request_metrics(int(rest_min_port) + item)
            if response.status_code != 200:
                print(f'Failed to get response from bob instance {item}, retrying...')
                raise UserWarning(f'Timeout on getting response from node {item}.')
            else:
                metrics = json.loads(response.content.decode('ascii'))
                if metrics['metrics']['backend.backend_state']['value'] != 1:
                    print(f'Backend is not up on node {item}, waiting...')
                    raise UserWarning(f'Backend is down on node {item}.')
                else:
                    print(f'Node {item} is ready!')
        print('All nodes are ready!')
    except ValueError:
        print_then_exit('Amount of nodes has unexpected value.')
    except KeyError:
        raise UserWarning(f'No backend state metric.')
    