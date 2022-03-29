import hashlib
import base64
import os
import sys
import time
import random

o_path = os.path.abspath(os.path.dirname(__file__)).split('/')
o_path = '/'.join(o_path[:o_path.index('eth_analysis') + 1])
sys.path.append(o_path)

from core.sql import generate_replace_sql_header, generate_replace_sql_values
from crypto.rsa_key import create_keys


def generate():
    num_256bit = str(random.getrandbits(256))
    hashed_num = hashlib.sha256(num_256bit.encode()).digest()
    char_pair = random.choice(['rA', 'aZ', 'gQ', 'hH', 'hG', 'aR', 'DD'])
    b64encoded_bytes = base64.b64encode(hashed_num, char_pair.encode())
    api_key = b64encoded_bytes.rstrip('='.encode()).decode()
    return api_key

    # update
    # UPDATE api_key SET authority = 65535 WHERE id = 1;
    # REPLACE INTO api_key ( `name`, `authority`, `key` ) VALUES ('zhen', 65535, 'FRGUaUKC4uaywdKaIQxnf0IOxZx0jTCOR3oG430xCxE');


def main():
    # name = input('Please Enter Name: ')
    # inputs = sys.argv[1:]
    inputs = ['zhen', 'all']
    if len(inputs) == 2:

        api_key = generate()
        print(inputs, api_key, len(api_key))
        if inputs[1] == 'all':
            authority = 0xFFFF
        else:
            authority = 0
        public_key, private_key = create_keys()
        api_key_dict = {'name': inputs[0], 'authority': authority, 'key': api_key, 'rsa': ''}
        sql = generate_replace_sql_header('api_key', api_key_dict.keys())
        server_api_key_dict = api_key_dict.copy()
        server_api_key_dict['rsa'] = private_key
        sql += generate_replace_sql_values(server_api_key_dict.values())
        print(sql[:-1] + ';')

        sql = generate_replace_sql_header('api_key', api_key_dict.keys())
        worker_api_key_dict = api_key_dict.copy()
        worker_api_key_dict['rsa'] = public_key
        sql += generate_replace_sql_values(worker_api_key_dict.values())
        print(sql[:-1] + ';')


if __name__ == '__main__':
    main()
