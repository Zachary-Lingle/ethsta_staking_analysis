import json
import os
import sys
import time

files = os.listdir('../result/')
files.sort()

o_path = os.path.abspath(os.path.dirname(__file__)).split('/')
o_path = '/'.join(o_path[:o_path.index('eth_analysis') + 1])
sys.path.append(o_path)

from sql import generate_replace_sql_header, generate_replace_sql_values, write_into_db, read_from_db
from poster import post

for file in files:
    with open('../result/{}'.format(file), 'r') as r_file:
        entity_info_list = json.load(fp=r_file)
    if not entity_info_list:
        continue
    time_str = time.strptime(file.replace('result_', '').replace('.json', ''), '%Y-%m-%d-%H-%M-%S')
    timestamp = int(time.mktime(time_str))
    is_exist = read_from_db(sql='select * from deposit_status where timestamp = {};'.format(timestamp))
    if not is_exist:
        keys = list(entity_info_list[0].keys())
        sql = generate_replace_sql_header('deposit_status', entity_info_list[0].keys())
        for entity_info in entity_info_list:
            sql += generate_replace_sql_values(values=entity_info.values())
        sql = sql[:-1] + ';'
        write_into_db(sql=sql)
        post('deposit_status', entity_info_list, entity_info_list[0].get('timestamp'))
        print('Finish :{}'.format(file))
    else:
        print('Skip :{}'.format(file))
