from pymemcache.client import base
import time

# ip = 'localhost'
ip = 'localhost'

# Don't forget to run `memcached' before running this next line:
client = base.Client((ip, 11211))

# Once the client is instantiated, you can access the cache:
# key1 = 'some_key'
# client.set(key1, 'some value')

# Retrieve previously set data again:
key = "123"

for i in range(100):
    t1 = time.time()
    value = client.get(key)
    rttMicroSeconds = (time.time() -t1)*1000000

    # time.sleep(0.1)
    client.set(key, value, noreply=False)
    
    print(rttMicroSeconds)

# print(value)