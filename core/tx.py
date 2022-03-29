import json
import os
import sys
import time

import requests
from concurrent.futures import ThreadPoolExecutor

o_path = os.path.abspath(os.path.dirname(__file__)).split('/')
o_path = '/'.join(o_path[:o_path.index('eth_analysis') + 1])
sys.path.append(o_path)

from sql import transform_data, generate_replace_sql_header, generate_replace_sql_values, write_into_db, read_from_db
from poster import post

api_key = read_from_db(sql='''select * from api_key where name = '{}';'''.format('etherscan'))[0]


def transform_wei_to_eth(value, digital=18):
    value = int(value)
    return round(value * pow(10, -18), digital)


def filter(filename):
    parts = []
    for part in filename.split('_'):
        if str.isdigit(part):
            parts.append(int(part))
    return parts


def get_latest_height():
    stats_url = 'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={}&closest=before&apikey={}'
    url = stats_url.format(int(time.time()), api_key.get('key'))
    stats = requests.get(url).json()
    if stats.get('status') != '1':
        stats = {}
    latest_height = int(stats.get('result', '14306420'))
    return latest_height


def get_total_supply_eth2():
    stats_url = 'https://api.etherscan.io/api?module=stats&action=ethsupply2&apikey={}'
    url = stats_url.format(api_key.get('key'))
    stats = requests.get(url).json()
    if stats.get('status') != '1':
        stats = {}
    result = stats.get('result')
    return result


def tx_to_mysql(data, table_name, filename=''):
    t_data = transform_data(data[0])
    sql = generate_replace_sql_header(table_name=table_name, keys=t_data.keys())
    index = 0
    for raw in data:
        value_sql = generate_replace_sql_values(raw.values())
        sql += value_sql
        index += 1
        if index % 1000 == 0:
            print(filename, 'reading ....', 'raw:{}'.format(index))
    sql = sql[:-1] + ';'
    write_into_db(sql=sql)
    print(filename, 'Finished', 'total_raw:{}'.format(index))


def download_staking_internal_tx(latest_height):
    tx_internal_list_url = 'https://api.etherscan.io/api?module=account&action=txlistinternal&address=0x00000000219ab540356cbb839cbe05303d7705fa&startblock={}&endblock={}&page={}&sort=asc&apikey={}'
    files = os.listdir('../internal_tx_list')
    files.sort(reverse=True)
    with open('../internal_tx_list/{}'.format(files[0]), 'r') as r_file:
        data = json.load(fp=r_file)
        start_block = data[-1].get('blockNumber')
    end_block = latest_height
    page = 1
    while start_block != end_block:
        if page != 1:
            start_block = end_block
        url = tx_internal_list_url.format(start_block, latest_height, 1, api_key.get('key'))
        print(url)
        index = 0
        data = requests.get(url=url).json().get('result')
        if not data:
            continue
        with open('../internal_tx_list/internal_txs_latest_block_{}_page_{:0>2d}.tx'.format(end_block, page),
                  'w') as w_file:
            json.dump(data, fp=w_file)
            filename = w_file.name
        tx_to_mysql(data, 'internal_transaction', filename)
        print(filename, 'Finished', data[0].get('blockNumber'), data[-1].get('blockNumber'), '\n')
        end_block = data[-1].get('blockNumber')
        page += 1
        time.sleep(2)


def download_staking_tx(latest_height):
    files = os.listdir('../tx_list')
    files.sort(reverse=True)
    with open('../tx_list/{}'.format(files[0]), 'r') as r_file:
        data = json.load(fp=r_file)
        start_block = data[-1].get('blockNumber')
    tx_internal_list_url = 'https://api.etherscan.io/api?module=account&action=txlist&address=0x00000000219ab540356cbb839cbe05303d7705fa&startblock={}&endblock={}&page={}&sort=asc&apikey={}'
    end_block = latest_height
    page = 1
    while start_block != end_block:
        if page != 1:
            start_block = end_block
        url = tx_internal_list_url.format(start_block, latest_height, 1, api_key.get('key'))
        print(url)
        data = requests.get(url=url).json().get('result')
        if not data:
            continue
        with open('../tx_list/txs_latest_block_{}_page_{:0>2d}.tx'.format(latest_height, page),
                  'w') as w_file:
            json.dump(data, fp=w_file)
            filename = w_file.name.split('/')[-1]
        tx_to_mysql(data, 'transaction', filename)
        print(filename, 'Finished', data[0].get('blockNumber'), data[-1].get('blockNumber'), '\n')
        end_block = data[-1].get('blockNumber')
        page += 1
        time.sleep(2)


def download_tx_by_address(address, offset=10000, start_block=0, end_block=9999999999):
    tx_list_url = 'https://api.etherscan.io/api?module=account&action=txlist&address={}&offset={}&startblock={}&endblock={}&page={}&sort=asc&apikey={}'
    url = tx_list_url.format(address, offset, start_block, end_block, 1, api_key.get('key'))
    tx_list = requests.get(url=url).json().get('result')
    return tx_list


def event_to_mysql(data):
    index = 0
    data[0]['public_key'] = ''
    t_data = transform_data(data[0])
    t_data['public_key'] = ''
    sql = generate_replace_sql_header(table_name='event', keys=t_data.keys())
    for raw in data:
        raw = transform_data(raw)
        data = raw.get('data')[2:]
        raw['public_key'] = data[48 * 8:60 * 8].upper()
        value_sql = generate_replace_sql_values(raw.values())
        sql += value_sql
        index += 1
    sql = sql[:-1] + ';'
    print('Reading ....', 'raw:{}'.format(index))
    write_into_db(sql=sql)
    print('Finished Writing to DB', 'total_raw:{}'.format(index))


def download_event(start_block, end_block, address, topic0):
    event_list_url = 'https://api.etherscan.io/api?module=logs&action=getLogs&fromBlock={}&toBlock={}&address={}&topic0={}&apikey={}'
    from_block = start_block
    page = 1
    while from_block < end_block:
        url = event_list_url.format(from_block, end_block, address, topic0, api_key.get('key'))
        data = requests.get(url=url).json().get('result')
        if data:
            if type(data) is not list:
                time.sleep(1)
                continue
            with open('../event/event_start_block_{}_end_block_{}_page_{:0>4d}.tx'.format(start_block, end_block, page),
                      'w') as w_file:
                json.dump(data, fp=w_file)
                filename = w_file.name
            event_to_mysql(data)
            print(filename, 'Finished', int(data[0].get('blockNumber'), 16), int(data[-1].get('blockNumber'), 16), '\n')
            data_max_block_number = int(data[-1].get('blockNumber'), 16)
            if len(data) < 1000 and data_max_block_number == from_block:
                break
            from_block = data_max_block_number
            page += 1



def download_staking_event(latest_height):
    address = '0x00000000219ab540356cBB839Cbe05303d7705Fa'
    topic0 = '0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5'
    files = os.listdir('../event')
    files.sort(reverse=True)
    with open('../event/{}'.format(files[0]), 'r') as r_file:
        data = json.load(fp=r_file)
        from_block = int(data[-1].get('blockNumber'), 16)
    step = 100000
    with ThreadPoolExecutor(max_workers=8) as executor:
        for start_block in range(from_block, latest_height, step):
            end_block = start_block + step
            executor.submit(download_event, start_block, end_block, address, topic0)


def get_address_tx(address, start_block=0, end_block=999999999, offset=10000):
    tx_list_url = 'https://api.etherscan.io/api?module=account&action=txlist&address={}&startblock={}&endblock={}&offset={}&sort=asc&apikey={}'
    url = tx_list_url.format(address, start_block, end_block, offset, api_key.get('key'))
    # print(url)
    data = None
    while not data or type(data) is not list:
        try:
            data = requests.get(url=url).json().get('result')
        except:
            time.sleep(1)
    return data


def get_address_balance_web(address, tag='latest'):
    balance_url = 'https://api.etherscan.io/api?module=account&action=balance&address={}&tag={}&apikey={}'
    url = balance_url.format(address, api_key.get('key'))
    # print(url)
    data = None
    while not data or type(data) is not str:
        try:
            data = requests.get(url=url).json().get('result')
        except:
            time.sleep(1)
    return data


def get_address_balance_mysql(before_height):
    txs = read_from_db(sql='select * from transaction where block_number < {} and is_error = 0;'.format(before_height))
    internal_txs = read_from_db(sql='select * from internal_transaction where block_number < {};'.format(before_height))

    total_tx_balance = 0.0
    for tx in txs:
        balance = round(int(tx.get('value')) * pow(10, -18), 8)
        total_tx_balance += balance
    total_internal_tx_balance = 0.0
    for tx in internal_txs:
        balance = round(int(tx.get('value')) * pow(10, -18), 8)
        total_tx_balance += balance

    return total_tx_balance, total_internal_tx_balance, total_tx_balance + total_internal_tx_balance


def save_eth_supply():
    result = get_total_supply_eth2()
    timestamp = int(time.time())
    result = transform_data(result)
    result['timestamp'] = timestamp - timestamp % 60
    sql = '''select * from eth_supply where timestamp = '{}';'''.format(result['timestamp'])
    # print(sql)
    ret_value = read_from_db(sql=sql)
    # print(ret_value)
    if type(ret_value) is tuple and len(ret_value) == 0:
        sql = generate_replace_sql_header('eth_supply', result.keys())
        sql += generate_replace_sql_values(result.values())
        sql = sql[:-1] + ';'
        write_into_db(sql=sql)

    eth_supply_list = read_from_db('select * from eth_supply order by timestamp desc limit 5000;')
    print('eth_supply', len(eth_supply_list), timestamp)
    # post('eth_supply', eth_supply_list, timestamp)


def save_staking_data():
    latest_height = get_latest_height()
    download_staking_internal_tx(latest_height)
    download_staking_tx(latest_height)
    download_staking_event(latest_height)


def save_event():
    latest_height = get_latest_height()
    download_staking_event(latest_height)


if __name__ == "__main__":
    cmd = sys.argv[1]
    cmds = {
        "save_eth_supply": save_eth_supply,
        "save_staking_data": save_staking_data,
        "save_event": save_event
    }

    if cmd in cmds:
        cmds[cmd]()
    else:
        print("invalid command, should be one of:", list(cmds.keys()))
