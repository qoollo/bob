import requests, os, sys, json
from retry import *

def request_metrics(port):
    try:
        return requests.get(f"http://0.0.0.0:{port}/metrics")
    except requests.RequestException:
        raise UserWarning('Failed to get metrics, retrying...')
    
         
@retry((UserWarning), tries=5, delay=1, backoff=2, max_delay=5)
def ensure_backend_up(bob_nodes_amount):
    try:
        for item in range(bob_nodes_amount):
            response = request_metrics(f"800{item}")
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
        sys.exit('Amount of nodes has unexpected value.')
    except KeyError:
        raise UserWarning(f'No backend state metric.')
    