import json
import os
import sys
import time

o_path = os.path.abspath(os.path.dirname(__file__)).split('/')
o_path = '/'.join(o_path[:o_path.index('eth_analysis') + 1])
sys.path.append(o_path)

from sql import read_from_db, write_into_db, generate_replace_sql_header, generate_replace_sql_values
from poster import post
from tagging import get_tag

entity_list = read_from_db(sql='select id,entity from entity_list;')

entity_dict = {}
for entity in entity_list:
    entity_dict[entity['entity']] = entity['id']


def save_new_address():
    filepath = '../info/address.tag'
    tags = get_tag()
    address_dict = {}
    keys = ['address', 'entity_id', 'version']
    for tag_type, address_info in tags.items():
        for address, entity in address_info.items():
            address_tag = {'address': address, 'entity_id': entity_dict.get(entity, 0), 'version': 1.0}
            address_dict[address] = address_tag
    with open(filepath, 'r') as r_file:
        for l in r_file:
            l = l.strip()
            if not l:
                continue
            if l[0] == '#':
                print(l[1:])
            address_info = json.loads(l)
            address_dict[address_info[0]] = dict(zip(keys, address_info))

    address_set = set(address_dict.keys())

    address_list = read_from_db('select address from address_tag;')
    db_address_set = set()
    for address in address_list:
        db_address_set.add(address['address'])
    print(len(address_set), len(db_address_set))
    address_set = address_set.difference(db_address_set)
    print('Getting {} Address(es)'.format(len(address_set)))
    if not address_set:
        return
    address_list = list(address_set)
    sql = generate_replace_sql_header(table_name='address_tag', keys=keys)
    address_info_list = []
    index = 0
    for address in address_list:
        address_info = address_dict.get(address)
        address_info_list.append(address_info)
        sql += generate_replace_sql_values(address_info.values())
        index += 1
        if index % 2000 == 0:
            post(table='address_tag', data_list=address_info_list, timestamp=int(time.time()))
            address_info_list.clear()
            print('Posted {} lines'.format(index))
            sql = sql[:-1] + ';'
            write_into_db(sql)
            sql = generate_replace_sql_header(table_name='address_tag', keys=keys)
            print('Finish {} lines'.format(index))
    if index % 2000 != 0:
        post(table='address_tag', data_list=address_info_list, timestamp=int(time.time()))
        address_info_list.clear()
        print('Posted {} lines'.format(index))
        sql = sql[:-1] + ';'
        write_into_db(sql)
        print('Finish {} lines'.format(index))


def entity_address_to_csv():
    sql = 'select * from address_tag where version = 1.0;'
    address_list = read_from_db(sql=sql)
    address_v1_dict = {}

    for address in address_list:
        address['version'] = 'ETH{:.1f}'.format(address['version'])
        address_v1_dict[address.get('entity')] = address_v1_dict.get(address.get('entity'), [])
        address_v1_dict[address.get('entity')].append(address)

    for entity, address_v1_list in address_v1_dict.items():
        with open('../csv/{}_eth1.0.csv'.format(entity), 'w') as w_file:
            print(','.join(address_v1_list[0].keys()), file=w_file)
            for address in address_v1_list:
                print(','.join(address.values()), file=w_file)

    sql = 'select * from address_tag where version = 2.0;'
    address_list = read_from_db(sql=sql)
    address_v2_dict = {}

    for address in address_list:
        address['version'] = 'ETH{:.1f}'.format(address['version'])
        address_v2_dict[address.get('entity')] = address_v2_dict.get(address.get('entity'), [])
        address_v2_dict[address.get('entity')].append(address)

    for entity, address_v2_list in address_v2_dict.items():
        with open('../csv/{}_eth2.0.csv'.format(entity), 'w') as w_file:
            print(','.join(address_v2_list[0].keys()), file=w_file)
            for address in address_v2_list:
                print(','.join(address.values()), file=w_file)


if __name__ == "__main__":
    save_new_address()
    # entity_address_to_csv()
