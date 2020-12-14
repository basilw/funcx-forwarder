#!/bin/bash
python3 -c "import funcx; print('funcx Version : ', funcx.__version__)"
python3 -c "import funcx_endpoint; print('funcx_endpoint Version : ', funcx_endpoint.__version__)"
python3 -c "import funcx_forwarder; print('funcx_forwarder Version : ', funcx_forwarder.__version__)"

echo "DEBUG: Overriding REDIS_HOST: $REDIS_HOST with FUNCX_REDIS_MASTER_SERVICE_HOST: $FUNCX_REDIS_MASTER_SERVICE_HOST"
export REDIS_HOST=$FUNCX_REDIS_MASTER_SERVICE_HOST

cat <<EOF > wait_for_redis.py
import redis
import time
import os

hostname=os.environ.get('REDIS_HOST')
port=os.environ.get('REDIS_PORT')
connected=False
print(f"Attempting to connect to REDIS at {hostname}:{port}")
for i in range(10):
    try:
        redis_client = redis.StrictRedis(host=hostname, port=port, decode_responses=True)
        redis_client.ping()
    except Exception:
        print("Trying")
        time.sleep(6)
    else:
        print("Connected!")
        connected=True
        break
if not connected:
    print("Failed to connect to REDIS with 10 attempts over 60s")
    exit(-1)
EOF

python3 wait_for_redis.py

echo "Starting Forwarder against REDIS server $REDIS_HOST : $REDIS_PORT"
# forwarder-service -a $ADVERTISED_FORWARDER_ADDRESS -p 8080 --redishost $REDIS_HOST --redisport $REDIS_PORT
# YADU: REMOVE DEBUG FLAG FOR PRODUCTION:

if [[ -z "${ADVERTISED_FORWARDER_ADDRESS}" ]]; then
    ADVERTISED_FORWARDER_ADDRESS=`wget http://169.254.169.254/latest/meta-data/public-ipv4; cat public-ipv4`
fi

forwarder-service -a $ADVERTISED_FORWARDER_ADDRESS -p 8080 --redishost $REDIS_HOST --redisport $REDIS_PORT -d --stream_logs

