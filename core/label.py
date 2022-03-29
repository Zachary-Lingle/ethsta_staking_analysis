import json
import os
import random
import time
import yaml
import requests
from bs4 import BeautifulSoup


def is_number(s):
    if s.count(".") == 1:
        if s[0] == ".":
            return False
        s = s.replace(".", "")
        return s.isdigit()
    elif s.count(".") == 0:
        return s.isdigit()
    else:
        return False


def read_header():
    with open("../conf/header.yaml", "r") as stream:
        try:
            header_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return header_dict


headers = read_header()


def clear_txts(sub_txts, target=''):
    while target in sub_txts:
        sub_txts.remove(target)
    return sub_txts


def download_labels_html():
    url = 'https://etherscan.io/labelcloud'
    content = requests.get(url=url, headers=headers, timeout=10).content
    with open('../labels/label.html', 'w') as file:
        file.write(content.decode())


def analysis_labels_html():
    with open('../labels/labels.html', 'r') as file:
        doc = file.read()

    soup = BeautifulSoup(doc, 'html.parser')
    entities = {}

    for span in soup.main.find_all('a'):
        if span.text and 'Accounts' in span.text:
            entities[span.attrs.get('href').split('/')[-1]] = span.attrs.get('href')
    with open('../labels/labels.info', 'w') as w_file:
        json.dump(entities, fp=w_file, indent=1)


def generate_url():
    urls = {}
    with open('../labels/labels.info', 'r') as file:
        entities = file.readlines()
    for entity in entities:
        entity = entity.strip()
        parts = entity.lower().split(' ')
        parts = clear_txts(parts)
        url = 'https://etherscan.io/{}?subcatid=undefined&size=100&start=0&col=1&order=asc'.format(
            '-'.join(parts))
        urls[entity] = url
    return urls


def download_label_html():
    files = list(os.listdir('../html'))
    with open('../labels/labels.info', 'r') as r_file:
        labels = json.load(fp=r_file)
    # entities = {'huobi': '/accounts/label/huobi'}
    for label, url in labels.items():
        url = 'https://etherscan.io{}?subcatid=undefined&size=100&start=0&col=1&order=asc'.format(url)
        filename = '{}.html'.format(label)
        if filename not in files:
            print(url)
            try:
                doc = requests.get(url, headers=headers, timeout=10)
                if 'one more step' in doc.text:
                    print('{} failed! one more step'.format(label))
                    continue
            except Exception as e:
                print('{} failed!'.format(label), e)
                continue
            with open('../html/{}'.format(filename), 'w') as w_file:
                print(doc.content, file=w_file)
                print('download {}'.format(label))

            time.sleep(random.random() * 10 + 1)


def download_contract_name():
    with open('../info/internal_staking_address.info', 'r') as r_file:
        address_dict = json.load(fp=r_file)
        index = 1
    for address, info in address_dict.items():
        if info.get('tag'):
            continue
        print('{:0>5d}'.format(index), address, info)
        url = 'https://etherscan.io/address/{}'.format(address)
        print(url)
        index += 1
        if info.get('count') == 2 and info.get('value') == 32.0:
            info['tag'] = 'RocketMinipool'
        else:
            doc = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(doc.content, 'html.parser')
            tag = soup.find('title').text.split('|')[0].strip()
            if 'Contract Address' in tag and ('Contract Address' in info['tag'] or not info['tag']):
                info['tag'] = tag
            time.sleep(2)
        print(info['tag'], '\n')
        with open('../info/internal_staking_address.info', 'w') as w_file:
            json.dump(address_dict, fp=w_file, indent=1)


def analysis_label_html():
    address_tags = {}
    for file in os.listdir('../html/'):
        if 'html' not in file:
            continue
        entity = file.replace('.html', '')
        with open('../html/{}'.format(file), 'r') as r_file:
            # print('reading {}'.format(file))
            doc = r_file.read()

        soup = BeautifulSoup(doc, 'html.parser')
        try:
            main_tag = soup.main.find_all('td')
            tags = {}
            for i in range(0, len(main_tag), 4):
                address, tag, balance, tx_count = main_tag[0 + i].text, main_tag[1 + i].text, main_tag[2 + i].text, \
                                                  main_tag[3 + i].text
                address_tags[address] = tag
                tag = tag.strip().replace(' ', '_')
                if not tag:
                    tag = file.replace('.html', '')
                tags[address] = tag.strip().replace(' ', '_')
        except:
            print('{} Failed!'.format(entity))
    with open('../labels/address_tags.info', 'w') as w_file:
        json.dump(address_tags, fp=w_file, indent=1)
    return address_tags


def analysis_address_tag(address_tags=None):
    if not address_tags:
        with open('../labels/address_tags.info', 'r') as r_file:
            address_tags = json.load(fp=r_file)
    n_address_tags, tag_address = {}, {}
    useless_info = ['Network', 'LUSD', 'Token', 'Contract', 'Staking']
    useless_tags = ['NullAddress', 'Fake', 'FAKE', '$', 'UpbitHacker', '0x']
    for address, tag in address_tags.items():
        skip = False
        address, tag = address.strip(), tag.strip()
        for useless_tag in useless_tags:
            if useless_tag in tag:
                skip = True
        if tag and not skip:
            n_tag = tag.split(':')[0]
            if n_tag[-5:] == 'Ether':
                continue
            if n_tag:
                parts = n_tag.split(' ')
                for index in range(len(parts) - 1, -1, -1):
                    if is_number(parts[index]):
                        del parts[index]
                    elif parts[index] in useless_info:
                        del parts[index]
                n_tag = ''.join(parts)
                if not n_tag:
                    continue

                n_address_tags[address] = n_tag
                tag_address[n_tag] = tag_address.get(n_tag, [])
                tag_address[n_tag].append(address)
        with open('../info/address_tags.info', 'w') as w_file:
            json.dump(n_address_tags, fp=w_file, indent=1)
    return n_address_tags, tag_address


if __name__ == "__main__":
    # At First Download label from Etherscan
    download_labels_html()
    analysis_labels_html()
    download_label_html()
    address_tags = analysis_label_html()
    analysis_address_tag(address_tags)

    # When You Finish Analyzing Internal TX
    # download_contract_name()
