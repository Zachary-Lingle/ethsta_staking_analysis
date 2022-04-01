import json
import os

import pymysql
import re

db = pymysql.connect(host='localhost',
                     user='ps',
                     password='',
                     database='eth_analysis',
                     charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
                     )

str_list = ['gas_price', 'gas', 'gas_limit', 'value', 'gas_used', 'trace_id']


def camel_to_case(name):
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    return name


def transform_data(data):
    n_data = {}
    if type(data) is not dict:
        print(data)
    for key, value in data.items():
        key = camel_to_case(key)
        if type(value) is list and len(value) == 1:
            value = value[0]
        elif '0x' == value[:2] and len(value) < 11 and (key not in str_list):
            if value == '0x':
                value = 0
            else:
                value = int(value, 16)
        elif value.isdigit() and (key not in str_list):
            value = int(value)
        n_data[key] = value
    return n_data


def generate_table(table_name, primary_key, data):
    sql = '''DROP TABLE IF EXISTS `{}`;\nCREATE TABLE `{}` (\n{},\nPRIMARY KEY (`{}`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;'''
    columns = []
    for key, value in data.items():
        column = "`" + str(key) + "`"
        if type(value) is int:
            column += ' {}'.format('INT NOT NULL DEFAULT 0')
        elif type(value) is float:
            column += ' {}'.format('DOUBLE  NOT NULL DEFAULT 0')
        elif type(value) is str:
            if len(value) <= 255:
                column += ' {}'.format('CHAR(255)')
            else:
                column += ' {}'.format('TEXT')
        columns.append(column)
    columns = ', \n'.join(str(c) for c in columns)
    with open('../sql/{}.sql'.format(table_name), 'w') as w_file:
        print(sql.format(table_name, table_name, columns, primary_key), file=w_file)


def generate_replace_sql_header(table_name, keys):
    columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in keys)
    sql = "REPLACE INTO {} ( {} )".format(table_name, columns) + ' VALUES '
    return sql


def generate_replace_sql_values(values):
    columns = []
    for value in values:
        if type(value) is int or type(value) is float:
            column = '{}'.format(value)
        elif type(value) is str:
            column = '\'{}\''.format(value)
        columns.append(column)
    columns = ', '.join(str(c) for c in columns)
    sql = '(' + columns + '),'
    return sql


def analyze_staking_internal_tx():
    files = list(os.listdir('../internal_tx_list'))
    files.sort()
    for file in files:
        index = 0
        with open('../internal_tx_list/{}'.format(file), 'r') as r_file:
            data = json.load(fp=r_file)
            t_data = transform_data(data[0])
            sql = generate_replace_sql_header(table_name='internal_transaction', keys=t_data.keys())
            for raw in data:
                value_sql = generate_replace_sql_values(raw.values())
                sql += value_sql
                index += 1
                if index % 1000 == 0:
                    print(file, 'reading ....', 'raw:{}'.format(index))
            sql = sql[:-1] + ';'
        write_into_db(sql=sql)
        print(file, 'Finished', 'total_raw:{}'.format(index))


def analyze_staking_tx():
    files = list(os.listdir('../tx_list'))
    files.sort()
    for file in files:
        index = 0
        with open('../tx_list/{}'.format(file), 'r') as r_file:
            data = json.load(fp=r_file)
            t_data = transform_data(data[0])
            sql = generate_replace_sql_header(table_name='transaction', keys=t_data.keys())
            for raw in data:
                value_sql = generate_replace_sql_values(raw.values())
                sql += value_sql
                index += 1
                if index % 1000 == 0:
                    print(file, 'reading ....', 'raw:{}'.format(index))
            sql = sql[:-1] + ';'
        write_into_db(sql=sql)
        print(file, 'Finished', 'total_raw:{}'.format(index))


def analyze_staking_event():
    files = list(os.listdir('../event'))
    files.sort()
    txids = set()
    for file in files:
        index = 0
        with open('../event/{}'.format(file), 'r') as r_file:
            data = json.load(fp=r_file)
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
                if index % 1000 == 0:
                    print(file, 'reading ....', 'raw:{}'.format(index))
            sql = sql[:-1] + ';'
        # print(sql)
        write_into_db(sql=sql)
        print(file, 'Finished', 'total_raw:{}'.format(index))


def read_from_db(sql):
    try:
        with db.cursor() as cursor:
            cursor.execute(sql)
            db.commit()
            data = cursor.fetchall()
    except:
        data = []

    return data


def write_into_db(sql):
    try:
        with db.cursor() as cursor:
            cursor.execute(sql)
            db.commit()
    except Exception as e:
        print("error")
        print(e)
        # print(sql)
        db.rollback()


if __name__ == '__main__':
    tables_in_db = read_from_db('show tables;')
    tables = [t['Tables_in_eth_analysis'] for t in tables_in_db]
    print(tables)
