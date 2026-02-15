# 1) getting up tulip
```shell
 submodule add https://github.com/OpenAttackDefenseTools/tulip vendor/tulip
 git submodule update --init --recursive
 ```

# 2) env
```shell
cp .env.example .env
mkdir -p data/traffic data/arkime-raw data/zeek-logs
```

# 3) getting up the system
```shell
docker compose up -d --build
```

# 4) configuring ips and ports for tulip:
#    vendor/tulip/services/api/configurations.py
#    rebuilding ONLY api cz no reason to rebuild the whole thing:
```shell
docker compose up -d --build tulip-api
```