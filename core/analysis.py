import json
import os
import sys
import time

o_path = os.path.abspath(os.path.dirname(__file__)).split('/')
o_path = '/'.join(o_path[:o_path.index('eth_analysis') + 1])
sys.path.append(o_path)

from sql import read_from_db, write_into_db, generate_replace_sql_header, generate_replace_sql_values
from tagging import get_tag, get_tagging

from concurrent.futures import ThreadPoolExecutor
from tx import get_address_tx
from poster import post
from address import save_new_address

entity_name_list = read_from_db(sql='select id,entity from entity_list;')

entity_name_dict = {}
for entity in entity_name_list:
    entity_name_dict[entity['entity']] = entity['id']


def prepare_tx():
    total_value = 0
    print('loading transaction...')
    tx_dict = {}
    addresses = set()
    txs = read_from_db('''select * from transaction;''')
    for tx in txs:
        tx_dict[tx['hash']] = tx
        addresses.add(tx.get('from'))
        total_value += round(pow(10, -18) * int(tx.get('value')), 4)
    print('total_value : {}'.format(total_value))
    addresses = list(addresses)
    print('total_address : {}'.format(len(addresses)))
    with open('../address/total_address.tx', 'w') as w_file:
        json.dump(addresses, fp=w_file, indent=1)
    return tx_dict, addresses


def prepare_internal_tx():
    total_value = 0
    print('loading internal_transaction...')
    internal_tx_dict = {}
    txs = read_from_db('''select * from internal_transaction;''')
    for tx in txs:
        internal_tx_dict[tx['hash']] = internal_tx_dict.get(tx['hash'], {})
        internal_tx_dict[tx['hash']][tx['trace_id']] = tx
        total_value += round(pow(10, -18) * int(tx.get('value')), 4)
    print(total_value)
    return internal_tx_dict


def statistics_part_events(worker_name, max_workers, tx_hash_dict, tx_dict, internal_tx_dict):
    tagging = get_tag()
    index = 0
    entity_dict = {}
    address_tags = ''
    total_events = len(tx_hash_dict)
    part_events = total_events // max_workers
    start_index = worker_name * part_events
    if (worker_name + 1) * part_events > total_events:
        end_index = total_events
    else:
        end_index = (worker_name + 1) * part_events
    total_events = (end_index - start_index) // 100
    tx_hash_list = list(tx_hash_dict.keys())

    for tx_hash in tx_hash_list[start_index:end_index]:
        index += 1
        txs = tx_dict.get(tx_hash)
        if txs:
            txs = [txs]
            tx_type = 'address_tag'
        else:
            tx_type = 'contract_tag'
            txs = internal_tx_dict.get(tx_hash, {}).values()
        tag = None
        if txs:
            tags = tagging[tx_type]
            for tx in txs:
                from_address, value, tag = tx.get('from'), int(tx.get('value')), \
                                           entity_name_dict.get(tags.get(tx.get('from'), 'others'))
                value = round(pow(10, -18) * value, 4)
                entity_dict[tag] = entity_dict.get(tag, {'count': 0, 'total_value': 0, 'validator_count': 0})
                entity_dict[tag]['count'] += 1
                entity_dict[tag]['total_value'] += value
                entity_dict[tag]['validator_count'] += 1

        # if index % total_events == 0:
        #     print('Worker {:0>2d} analysis {}% {} {}... ...'.format(worker_name, round(index / total_events, 3),
        #                                                             start_index, end_index))
        if tag:
            public_keys = tx_hash_dict.get(tx_hash)
            for public_key in public_keys:
                address_tag = [public_key, tag, 2.0]
                address_tags += json.dumps(address_tag) + '\n'
    return entity_dict, address_tags


def statistics_staking_event(timestamp):
    tx_dict, _ = prepare_tx()
    internal_tx_dict = prepare_internal_tx()
    events = read_from_db('select transaction_hash,public_key from event where time_stamp < {};'.format(timestamp))
    max_workers = 20
    entity_dict = {}
    tx_hash_dict = {}
    for event in events:
        tx_hash_dict[event.get('transaction_hash')] = tx_hash_dict.get(event.get('transaction_hash'), [])
        tx_hash_dict[event.get('transaction_hash')].append(event.get('public_key'))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for worker_name in range(0, max_workers):
            future = executor.submit(statistics_part_events, worker_name, max_workers, tx_hash_dict, tx_dict,
                                     internal_tx_dict)
            futures.append(future)
    total_count, total_value, validator_count = 0, 0, 0
    sql_file = open('../info/address.tag', 'w')
    for future in futures:
        result_dict, address_tags = future.result()
        for tag, info in result_dict.items():
            entity_dict[tag] = entity_dict.get(tag, {'count': 0, 'total_value': 0, 'validator_count': 0})
            entity_dict[tag]['count'] += info.get('count')
            entity_dict[tag]['total_value'] += info.get('total_value')
            entity_dict[tag]['validator_count'] += info.get('validator_count')
            total_count += info.get('count')
            total_value += info.get('total_value')
            validator_count += info.get('validator_count')
        print('# timestamp :{},time_str :{}'.format(timestamp,
                                                    time.strftime("%Y%m%d %H:%M:%S %Z", time.localtime(timestamp))))
        print(address_tags, file=sql_file)
    entity_info_list = []

    for entity, info in entity_dict.items():
        info['percent_of_count'] = round(info['count'] / total_count, 8)
        info['percent_of_value'] = round(info['total_value'] / total_value, 8)
        info['percent_of_validator'] = round(info['validator_count'] / validator_count, 8)
        info['entity'] = entity
        info['timestamp'] = timestamp
        entity_info_list.append(info)

    ts_str = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(timestamp))
    with open('../result/result_{}.json'.format(ts_str), 'w') as w_file:
        json.dump(entity_info_list, w_file)

    sql = generate_replace_sql_header('deposit_status', entity_info_list[0].keys())
    for entity_info in entity_info_list:
        sql += generate_replace_sql_values(values=entity_info.values())
    sql = sql[:-1] + ';'
    write_into_db(sql=sql)

    return entity_dict, entity_info_list


def download_tx(worker_name, max_workers, addresses):
    index = 0
    total_address = len(addresses)
    part_events = total_address // max_workers
    start_index = worker_name * part_events
    if (worker_name + 1) * part_events > total_address:
        end_index = total_address
    else:
        end_index = (worker_name + 1) * part_events
    total_address = (end_index - start_index)
    time.sleep(worker_name / 2)
    print('Worker {:0>2d} start...'.format(worker_name))
    for address in addresses[start_index:end_index]:
        filename = '{}.tx'.format(address)
        index += 1
        # print('Worker: {:0>2d} Address: {}'.format(worker_name, address))
        address_txs = get_address_tx(address, offset=1000)
        with open('../address/{}'.format(filename), 'w') as w_file:
            json.dump(address_txs, fp=w_file)
            print('Worker {:0>2d}'.format(worker_name), filename, 'Finished', int(address_txs[0].get('blockNumber')),
                  int(address_txs[-1].get('blockNumber')), index, '/', total_address)
        # if index % total_address == 0:
        #     print('Worker {:0>2d} analysis {}% ... ...'.format(worker_name, round(index / (total_address * 10), 3)))


def download_scan_tx_tag():
    with open('../address/total_address.tx', 'r') as r_file:
        addresses = json.load(fp=r_file)
    addresses.sort()
    max_workers = 10
    files = list(os.listdir('../address'))
    n_addresses = []
    for address in addresses:
        filename = '{}.tx'.format(address)
        if filename not in files:
            n_addresses.append(address)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for worker_name in range(0, max_workers):
            executor.submit(download_tx, worker_name, max_workers, n_addresses)


def tag_staking_address():
    tagging = get_tagging()
    with open('../address/total_address.tx', 'r') as r_file:
        addresses = json.load(fp=r_file)

    staking_address_tag = {}
    no_tag_list = []
    for address in addresses:
        tag = tagging.get(address)
        if tag:
            staking_address_tag[address] = tag
        else:
            no_tag_list.append(address)

    for address in no_tag_list:
        try:
            with open('../address/{}.tx'.format(address), 'r') as r_file:
                tx_list = json.load(fp=r_file)
        except:
            tx_list = get_address_tx(address, offset=1000)
            with open('../address/{}.tx'.format(address), 'w') as w_file:
                json.dump(tx_list, fp=w_file)
        tag_set = set()
        for tx in tx_list:
            from_address = tx.get('from')
            tag = tagging.get(from_address)
            if tag:
                tag_set.add(tag)
        if len(tag_set) == 1:
            staking_address_tag[address] = list(tag_set)[0]
        elif len(tag_set) > 1:
            staking_address_tag[address] = list(tag_set)
        else:
            staking_address_tag[address] = 'others'

    # sorted_tuple = sorted(staking_address_tag.items(), key=lambda x: x[1].get('count'), reverse=True)
    # staking_address_tag = dict((x, y) for x, y in sorted_tuple)

    with open('../info/staking_address_tag.info', 'w') as w_file:
        json.dump(staking_address_tag, fp=w_file, indent=1)

    return staking_address_tag


if __name__ == "__main__":
    interval = 24 * 60 * 60
    timestamp = int(time.time())
    timestamp = timestamp - timestamp % interval
    is_exist = read_from_db(sql='select * from deposit_status where timestamp = {};'.format(timestamp))
    if not is_exist:
        prepare_tx()
        download_scan_tx_tag()
        tag_staking_address()
        _, entity_info_list = statistics_staking_event(timestamp)
        post('deposit_status', entity_info_list, timestamp)
        save_new_address()
