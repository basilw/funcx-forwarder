import requests
import json


address = 'http://127.0.0.1:8088'
endpoint_id = 'test_endpoint'
endpoint_address = 'borgmachine'
redis_address = '127.0.0.1'
client_key = 'client_key_string'



r = requests.post(address + '/register',
                  json={'endpoint_id': endpoint_id,
                        'redis_address': redis_address,
                        'endpoint_addr': endpoint_address,
                        'client_key': client_key,
                  }
)

print(r)
