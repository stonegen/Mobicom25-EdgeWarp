import imp
import redis
import string
import random
import datetime
import time
import pickle
from common.state import State
from common.store import Store
from common.messageType import MessageType
import socket
import sys
import os
import struct
from common.util import UtilityFunctions
from multiprocessing import Process, Lock
from configuration.config import Config

keys = ["user:123:key1"] # 1,048,576
keys.append("user:123:key2") # 20,971,520
keys.append("user:123:key3") # 5,242,880
keys.append("user:123:key4") # 5,242,880
keys.append("user:123:key5") # 10,485,760
keys.append("user:123:key6") # 5,253

lock = Lock()

# config = config()
# config.loadConfig("configuration/config.json")


keyCounter = 0
LOCAL_HOST = "localhost"
LOCAL_PORT = 6379
TARGET_HOST = "203.135.63.29"
TARGET_PORT = 6379


lock = Lock()
extStore = Store(LOCAL_HOST, LOCAL_PORT, "testpass", lock)
extStore.Connect() 

K = 1024
M = 1024*1024





value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5*K))
extStore.Set(keys[0], value)
temp, state = extStore.Get(keys[0])
stateList = list(state)
randomByte = random.randint(1, 255)
print("Random bytes = ", randomByte)
stateList[0] = randomByte
state = bytes(stateList)

extStore.Set(keys[0], state)

temp, state = extStore.Get(keys[0])
print(state[0])
stateList = list(state)
stateList[0] = 55
state = bytes(stateList)

# print(state[0])
# print(type(state))

for key in keys:
    isThere, value = extStore.Get(key)
    print("Key = {0} having length = {1} is retrived.".format(key, len(value)))


# value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5*K))
# extStore.Set(keys[0], value)

# value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5*K))
# extStore.Set(keys[1], value)

# value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 100*K))
# extStore.Set(keys[2], value)

# value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 1*M))
# extStore.Set(keys[3], value)

# value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 1*M))
# extStore.Set(keys[4], value)

# value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 1*M))
# extStore.Set(keys[5], value)

# isKeyFound, value = extStore.Get(keys[5])
# print(len(value), type(value))

print("---------------------------------------------------------")
print("All keys retrieved.")
print("---------------------------------------------------------")


