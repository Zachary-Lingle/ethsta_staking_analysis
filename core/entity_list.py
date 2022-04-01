import json
import os
import sys
import time

import requests

files = os.listdir('../result/')
files.sort()

o_path = os.path.abspath(os.path.dirname(__file__)).split('/')
o_path = '/'.join(o_path[:o_path.index('eth_analysis') + 1])
sys.path.append(o_path)

from core.sql import generate_replace_sql_header, generate_replace_sql_values, write_into_db, read_from_db, \
    generate_table
from core.poster import post


def get_entity_type():
    url = 'https://pro-api.coinmarketcap.com/v1/exchange/map'
    parameters = {
        'start': '1',
        'limit': '5000',
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': 'f6ee5821-80e6-4b32-86e0-11730bdcf422',
    }

    try:
        data = requests.get(url, params=parameters, headers=headers).json()
        print(data)
    except ConnectionError as e:
        print(e)

    with open('../info/entity_list_cmc.lst', 'w') as w_file:
        json.dump(data.get('data'), fp=w_file, indent=1)


def entity_list():
    files = list(os.listdir('../result/'))
    files.sort()
    filename = files[-1]
    with open('../result/{}'.format(filename), 'r') as r_file:
        deposit_info_list = json.load(fp=r_file)

    keys = ['id', 'entity']
    sql = generate_replace_sql_header('entity_list', keys=keys)
    index = 1
    for deposit_info in deposit_info_list:
        if deposit_info.get('entity') != 'others':
            sql += '''({},'{}'),'''.format(index, deposit_info.get('entity'))
            index += 1

    sql += '''(0,'others');'''
    print(sql)
    # keys = list(data[0].keys())
    # print(keys)
    # del keys[0]
    # keys.append('')
    # sql = generate_replace_sql_header('entity_list', keys)
    # for info in infos:
    #     del info['validator_count']
    #     sql += generate_replace_sql_values(values=info.values())
    # sql = sql[:-1] + ';'
    # print(sql)
    # write_into_db(sql=sql)


def entity_address():
    entity_info_list = read_from_db(sql='select * from entity_list;')
    address_statistics = read_from_db(
        sql='select count(*),entity_id from address_tag where version = 1.0 group by entity_id;')
    public_key_statistics = read_from_db(
        sql='select count(*),entity_id from address_tag where version = 2.0 group by entity_id;')
    entity_info_dict = {}
    for entity_info in entity_info_list:
        entity_info_dict[entity_info['id']] = entity_info
    for address_info in address_statistics:
        entity_info_dict[address_info['entity_id']]['address_count'] = address_info.get('count(*)', 0)
    for address_info in public_key_statistics:
        entity_info_dict[address_info['entity_id']]['public_key_count'] = address_info.get('count(*)', 0)

    sql = generate_replace_sql_header('entity_list', entity_info_list[0])
    for entity_info in entity_info_dict.values():
        sql += generate_replace_sql_values(values=entity_info.values())

    sql = sql[:-1] + ';'
    write_into_db(sql)
    post('entity_list', data_list=entity_info_list, timestamp=int(time.time()))


if __name__ == "__main__":
    # entity_list()
    # get_entity_type()
    entity_address()

