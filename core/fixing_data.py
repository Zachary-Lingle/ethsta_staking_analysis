import json
import os
import sys
import time

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

o_path = os.path.abspath(os.path.dirname(__file__)).split('/')
o_path = '/'.join(o_path[:o_path.index('eth_analysis') + 1])
sys.path.append(o_path)

from analysis import statistics_part_events, prepare_tx, prepare_internal_tx, download_scan_tx_tag, tag_staking_address
from sql import read_from_db, generate_replace_sql_header, generate_replace_sql_values, write_into_db


def statistics_staking_event(tx_dict, internal_tx_dict, timestamp, is_save_to_db):
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

    print(timestamp, time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(timestamp)))
    print(total_count, total_value, total_count)
    entity_info_list = []

    for entity, info in entity_dict.items():
        info['percent_of_count'] = round(info['count'] / total_count, 8)
        info['percent_of_value'] = round(info['total_value'] / total_value, 8)
        info['percent_of_validator'] = round(info['validator_count'] / validator_count, 8)
        info['entity'] = entity
        info['timestamp'] = timestamp
        entity_info_list.append(info)

    if is_save_to_db:
        sql = generate_replace_sql_header('deposit_status', entity_info_list[0].keys())
        for entity_info in entity_info_list:
            sql += generate_replace_sql_values(values=entity_info.values())
        sql = sql[:-1] + ';'
        write_into_db(sql=sql)

    ts_str = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(timestamp))
    with open('../result/result_{}.json'.format(ts_str), 'w') as w_file:
        json.dump(entity_info_list, w_file)

    return entity_dict, entity_info_list


def analysis_data_gap(obj_type='files', interval=24 * 60 * 60, input_time_str=None):
    time_end = int(time.time())
    time_end = time_end - time_end % interval + interval
    if obj_type == 'files':
        files = os.listdir('../result')
        files.sort()
        time_str = time.strptime(files[0].replace('result_', '').replace('.json', ''), '%Y-%m-%d-%H-%M-%S')
        time_start = int(time.mktime(time_str))
        gaps = []
        for ts in range(time_start, time_end, interval):
            ts_str = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(ts))
            file = 'result_{}.json'.format(ts_str)
            if file in files:
                continue
            gaps.append(ts)
            # print(len(gaps), ts, ts_str)
        return gaps
    elif obj_type == 'db':
        time_points = read_from_db(sql='select timestamp from deposit_status group by timestamp;')
        time_points = [t['timestamp'] for t in time_points]
        gaps = []
        for ts in range(time_points[0], time_end, interval):
            if ts in time_points:
                continue
            gaps.append(ts)
            # print(len(gaps), ts, time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(ts)))
        return gaps
    elif obj_type == 'input' and input_time_str:
        try:
            time_str = time.strptime(input_time_str, '%Y-%m-%d-%H-%M-%S')
            time_start = int(time.mktime(time_str))
            time_points = read_from_db(sql='select timestamp from deposit_status group by timestamp;')
            time_points = [t['timestamp'] for t in time_points]
            gaps = []
            for ts in range(time_start, time_end, interval):
                if ts in time_points:
                    continue
                gaps.append(ts)
                # print(len(gaps), ts, time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(ts)))
            return gaps
        except:
            pass
    return []


def fix_gaps(obj_type='files', interval=24 * 60 * 60, input_time_str=None, is_save_to_db=False):
    tx_dict, _ = prepare_tx()
    internal_tx_dict = prepare_internal_tx()
    gaps = analysis_data_gap(obj_type=obj_type, interval=interval, input_time_str=input_time_str)
    print(len(gaps), json.dumps(gaps))
    for timestamp in gaps:
        statistics_staking_event(tx_dict.copy(), internal_tx_dict.copy(), timestamp, is_save_to_db)


if __name__ == "__main__":
    download_scan_tx_tag()
    tag_staking_address()
    # fix_gaps()
    fix_gaps(obj_type='input', input_time_str='2020-11-20-08-00-00', is_save_to_db=True)
