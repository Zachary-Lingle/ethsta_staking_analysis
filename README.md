# ethsta_staking_analysis
This repository conducts data analytics on Ethereum staking pools, aka the entities that control the validators of the beacon chain. See our post on [ethresear.ch](https://ethresear.ch/) for more details.
## Getting started
### Prerequisites
1. Create some folders by running ```prepare.sh```.
```bash
sh ~/cmd/prepare.sh
```

2. Install ```python3``` and ```pip3```.
3. Install python dependencies.
```bash
pip3 install -r requirements.txt
```

4. Import MySQL tables.
```bash
mysql -u{user_name} -p{} -D eth_analysis < {sql_file_name}.sql
```

5. Go to https://etherscan.io/login to sign up your account and create your API key. Then save the API key to MySQL.
```bash
mysql -u{user_name} -p{}
use eth_analysis;
INSERT INTO `api_key` VALUES 
('etherscan','XXXXXXXXXX API_KEY XXXXXXXXXX',0,'');
```

6. Create header.yaml in conf directory and configure the settings of HTML header.
```yaml
Content-Type: 
Accept-Encoding: 
Accept-Language: 
Cache-Control: 
cookcookie: 
user-agent:
```

### Run
7. Run ```analysis_staking_data.sh``` to start. See its file content for detailed steps. 
```bash
sh ~/cmd/analysis_staking_data.sh
```

### Contribute
Submit an issue or talk to us on [ethresear.ch](https://ethresear.ch/).
### License
Apache License 2.0
