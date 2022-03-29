import json

import rsa
import os
import sys
import time
import random

o_path = os.path.abspath(os.path.dirname(__file__)).split('/')
o_path = '/'.join(o_path[:o_path.index('eth_analysis') + 1])
sys.path.append(o_path)


def create_keys():
    pub_key, priv_key = rsa.newkeys(1024, poolsize=2)
    public_key = pub_key.save_pkcs1().decode()
    private_key = priv_key.save_pkcs1().decode()
    return public_key, private_key


def encrypt(public_key, text):
    p = public_key.encode()
    pubkey = rsa.PublicKey.load_pkcs1(p)
    original_text = text.encode('raw_unicode_escape')
    crypt_text = rsa.encrypt(original_text, pubkey)
    return crypt_text.decode('raw_unicode_escape')


def decrypt(private_key, crypt_text):
    p = private_key.encode()
    crypt_text = crypt_text.encode('raw_unicode_escape')
    privkey = rsa.PrivateKey.load_pkcs1(p)
    lase_text = rsa.decrypt(crypt_text, privkey).decode()

    return lase_text


if __name__ == '__main__':
    public_key, private_key = create_keys()
    crypt_text = encrypt(public_key=public_key, text='{}'.format(100))
    lase_text = decrypt(private_key=private_key, crypt_text=crypt_text)
    print(lase_text)
