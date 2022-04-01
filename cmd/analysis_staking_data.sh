#!/bin/bash
cd ../core/;
python3 tx.py save_staking_data;
python3 analysis.py
python3 address.py
python3 entity_list.py