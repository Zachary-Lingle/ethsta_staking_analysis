import json
import os
from sql import generate_replace_sql_header, generate_replace_sql_values, write_into_db, read_from_db


def get_entity_list():
    entity_list = read_from_db(sql='select entity from entity_list;')
    entity_list = [entity['entity'] for entity in entity_list]
    return entity_list


def get_tagging():
    with open('../info/address_tags.info', 'r') as r_file:
        tagging = json.load(fp=r_file)
    return tagging


def get_tag():
    entity_list = get_entity_list()
    files = ['internal_staking_address.info', 'staking_address_tag.info']
    tagging = {'contract_tag': 0, 'address_tag': 1}
    for tag_type, index in tagging.items():
        tagging[tag_type] = {}
        with open('../info/{}'.format(files[index]), 'r') as r_file:
            info_dict = json.load(r_file)
            for address, info in info_dict.items():
                if type(info) is str or type(info) is list:
                    tag = info
                else:
                    tag = info.get('tag')
                if tag and ':' in tag:
                    tag = tag.split(':')[0]
                tagging[tag_type][address] = tag
                if not tag or type(tag) is list or 'Contract Address' in tag:
                    tagging[tag_type][address] = 'others'

    return tagging


def save_to_db():
    tags = get_tag()
    address_tags = []
    sql = generate_replace_sql_header('address_tag', ['address', 'entity_id', 'version'])
    for tag_type, address_dict in tags.items():
        for address, entity in address_dict.items():
            if entity != 'others':
                address_tag = {'address': address, 'entity_id': entity, 'version': 1.0}
                address_tags.append(address_tag)
                sql += generate_replace_sql_values(address_tag.values())
    sql = sql[:-1] + ';'
    # print(sql)
    write_into_db(sql=sql)
    return address_tags


if __name__ == "__main__":
    get_tag()
    # save_to_db()
