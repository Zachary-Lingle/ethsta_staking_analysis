import json
import os
import sys
import time
import traceback

import requests
import random

o_path = os.path.abspath(os.path.dirname(__file__)).split('/')
o_path = '/'.join(o_path[:o_path.index('eth_analysis') + 1])
sys.path.append(o_path)

from sql import read_from_db
from crypto.rsa_key import encrypt

api_key = read_from_db('''select * from api_key where name = '{}';'''.format('poster'))[0]
keys = ['table', 'data', 'length', 'timestamp']


def generate_sample():
    size = 1000
    sample = {'id': random.randint(0, size), 'data': None, 'api_key': api_key.get('key')}
    url = 'https://ethsta.com/api/upload'
    return sample, url


def post(table, data_list, timestamp):
    if not data_list:
        print('Do Not Send Empty Message!')
        return
    sample, url = generate_sample()
    is_posted = None
    while not is_posted:
        try:
            sample['data'] = {'table': encrypt(public_key=api_key.get('rsa'), text=table), 'data_list': data_list,
                              'length': encrypt(public_key=api_key.get('rsa'), text=str(len(data_list))),
                              'timestamp': timestamp}
            result = requests.post(url=url, json=sample)
            result = result.json()
        except Exception as e:
            traceback.print_exc()
            time.sleep(2)
            continue
        if result.get('id') == sample.get('id'):
            is_posted = not result.get('is_error')
            if result.get('is_error'):
                print(result.get('describe'))
                time.sleep(2)
            else:
                print(result.get('data'))


if __name__ == "__main__":
    post('test', [], 11)
