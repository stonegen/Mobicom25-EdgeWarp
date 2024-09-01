# import redis
# import string
# import random
# import datetime
# import time
# import pickle
# from common.state import State
# from common.store import Store
# from common.messageType import MessageType
# import lz4.frame
# import socket
# import sys
# import struct
# from common.util import UtilityFunctions
# from _thread import *
# import threading
# from threading import Lock

# lock = Lock()

# keys = ["user:123:key1"] # 1,048,576
# keys.append("user:123:key2") # 20,971,520
# keys.append("user:123:key3") # 5,242,880
# keys.append("user:123:key4") # 5,242,880
# keys.append("user:123:key5") # 10,485,760
# keys.append("user:123:key6") # 5,253

# # Load configuration paramters from file.
# configuration = config()
# config = configuration.loadConfig("configuration/config.json")

# keyCounter = 0
# LOCAL_HOST = "localhost"
# LOCAL_PORT = 6379
# TARGET_HOST = "203.135.63.29"
# TARGET_PORT = 6379


# extStore = Store(config["store"]["ipAddress"], config["store"]["portNumber"], config["store"]["password"])
# extStore.Connect() 

# N = 5*1024*1024

# for key in keys:
#     value = ''.join(random.choices(string.ascii_uppercase +
#                              string.digits, k = N))
#     extStore.Set(key, value)
#     print("Key = {0} having length = {1} is stored.".format(key, len(value)))

# print("---------------------------------------------------------")
# print("All keys stored.")
# print("---------------------------------------------------------")


# migrationCompleted = True

# def callBack(status):
#     print("State migration status = {0}".format(status))
#     migrationCompleted = True

# # Store all the keys once
# value = ''.join(random.choices(string.ascii_uppercase +
#                 string.digits, k = N))
# for key in keys:
#     extStore.Set(key, value)
#     print("Key = {0}, Length = {1}".format(key, len(value)))

# startTime = time.time()
# backgroundSync = False
# finalSyncDone = False
# while(True):

#     keyCounter += 1

#     if finalSyncDone == False:
#         keyIndex = keyCounter % len(keys)
#         isFound, dummyState = extStore.Get(keys[keyIndex])
#         if isFound:
#             value = ''.join(random.choices(string.ascii_uppercase +
#                     string.digits, k = 1024))
#             extStore.Set(keys[keyIndex], value)
#             # print("Updated key = {0}".format(keys[keyIndex]))
#         time.sleep(0.1)

#     # Start background syc after 3 seconds
#     if time.time() - startTime > 3 and backgroundSync == False:
#         # Sync thread starting
#         # extStore.SyncBackground(TARGET_HOST, callBack)
#         start_new_thread(extStore.SyncBackground, (TARGET_HOST, TARGET_PORT, callBack,))
#         backgroundSync = True
#         print("Background sync thread starting!")
    
#     if time.time() - startTime > 5 and finalSyncDone == False:
#         print("Final sync thread starting!")
#         extStore.event.set()
#         finalSyncDone = True






# # N = 5*1024*1024
# # # N = 5*1024

# # key = "user:123:key6"
# # value = ''.join(random.choices(string.ascii_uppercase +
# #                              string.digits, k = N))
# # # print(value1)
# # r = redis.Redis(host='localhost', port=6379, db=0, password="testpass")

# # startTime = time.time()
# # r.set("Testabc", value)
# # endTime = time.time()
# # timeElapsed = endTime - startTime
# # print("String length = {0} bytes, Time for set = {1} \
# #     milliseconds.".format(len(value), timeElapsed*1000))

# # startTime = time.time()
# # r.set("Testabc", value)
# # endTime = time.time()
# # timeElapsed = endTime - startTime
# # print("String length = {0} bytes, Time for set = {1} \
# #     milliseconds.".format(len(value), timeElapsed*1000))


# # keys = ["user:123:key1"] # 1,048,576
# # keys.append("user:123:key2") # 20,971,520
# # keys.append("user:123:key3") # 5,242,880
# # keys.append("user:123:key4") # 5,242,880
# # keys.append("user:123:key5") # 10,485,760
# # keys.append("user:123:key6") # 5,253

# # # print(len(r.get("user:123:key1")))
# # # print(len(r.get("user:123:key2")))
# # # print(len(r.get("user:123:key3")))
# # # print(len(r.get("user:123:key4")))
# # # print(len(r.get("user:123:key5")))

# # for key in keys:
# #     value = ''.join(random.choices(string.ascii_uppercase +
# #                              string.digits, k = N))
# #     r.set(key, value)
# #     print(len(r.get(key)))
# # startTime = time.time()

# # a = r.migrate('203.135.63.29', 6379, key, 0, 5000, auth="testpass", replace=True)
# # endTime = time.time()
# # timeElapsed = endTime - startTime
# # print("String length = {0} bytes, Time for migration = {1} \
# #     milliseconds.".format(len(value), timeElapsed*1000))
