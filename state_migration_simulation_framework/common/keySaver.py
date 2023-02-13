import imp
import redis
import string
import random
import datetime
import time
import pickle
from common.state import State
from common.redisStore import RedisStore
from common.memcacheStore import MemcacheStore
from common.messageType import MessageType
import socket
import sys
import os
import struct
from common.util import UtilityFunctions
from multiprocessing import Process, Lock
from configuration.config import Config

class KeySaver:
    def CreateKeys(self, localIp, storePortNumber, totalVariables, storeType):
        lock = Lock()

        lock = Lock()

        K = 1024

        extStore = None
        if storeType == "Redis":
            print("Redis store.")
            extStore = RedisStore(localIp, storePortNumber, "testpass", lock)
        elif storeType == "Memcache":
            print("Memcache store.")
            extStore = MemcacheStore(localIp, storePortNumber, "testpass", lock)

        # extStore = RedisStore(localIp, storePortNumber, "testpass", lock)
        # extStore = MemcacheStore(localIp, storePortNumber, "testpass", lock)
        extStore.Connect() 

        keys = []

        for i in range(1, totalVariables + 1):
            newKey = "user:123:key" + str(i)
            keys.append(newKey)

        # self.defaultApp(keys, extStore, K)
        # self.carMap(keys, extStore, K)
        self.mobileVr(keys, extStore, K)
        # self.onlineGame(keys, extStore, K)

        
        return keys


    def defaultApp(self, keys, extStore, K):
        print("Default app state stored.")
        counter = 1
        totalKeys = len(keys)

        for key in keys:
            # For first one thrid keys, keep this multiplier which makes the value 5KB
            multiplier = 5

            # if (counter == 1):
            #     multiplier = 181 # for carmap paper.

            # For second set of 1/3rd keys, the value size is 50KB
            if counter > totalKeys/3 and counter <= 2*totalKeys/3:
                multiplier = 50
            # For third set of 1/3rd keys, the value size is 1MB
            elif counter > 2*totalKeys/3:
                # multiplier = 1024
                multiplier = 1023

            print("Generating key {} having size = {}".format(key, multiplier*K))
            value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = multiplier*K))
            extStore.Set(key, value)



            # extStore.Get(key)
            # print("Stored size = {}.".format(exit(ex)))

            counter += 1

    def carMap(self, keys, extStore, K):
        print("Car map state stored.")
        counter = 1
        totalKeys = len(keys)

        for key in keys:
            # For first one thrid keys, keep this multiplier which makes the value 5KB
            multiplier = 5

            if (counter == 1):
                multiplier = 181 # for carmap paper.

            # For second set of 1/3rd keys, the value size is 50KB
            if counter > totalKeys/3 and counter <= 2*totalKeys/3:
                multiplier = 1
            # For third set of 1/3rd keys, the value size is 1MB
            elif counter > 2*totalKeys/3:
                # multiplier = 1024
                multiplier = 1

            print("Generating key {} having size = {}".format(key, multiplier*K))
            value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = multiplier*K))
            extStore.Set(key, value)

            counter += 1
    
    def mobileVr(self, keys, extStore, K):
        print("Mobile VR state stored.")
        counter = 1
        totalKeys = len(keys)

        for key in keys:
            # For first one thrid keys, keep this multiplier which makes the value 5KB
            multiplier = 5

            if (counter == 1):
                multiplier = 50 
            elif counter == 2:
                 multiplier = 100
            elif counter == 3:
                 multiplier = 500
            elif counter == 4:
                 multiplier = 400
            elif counter == 5:
                 multiplier = 900
            elif counter == 6:
                 multiplier = 950
            elif counter == 7:
                 multiplier = 985

            print("Generating key {} having size = {}".format(key, multiplier*K))
            value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = multiplier*K))
            extStore.Set(key, value)

            counter += 1

    def onlineGame(self, keys, extStore, K):
        print("Online game state stored.")

        counter = 1
        totalKeys = len(keys)

        for key in keys:
            # For first one thrid keys, keep this multiplier which makes the value 5KB
            multiplier = 5

            if (counter == 1):
                multiplier = 10 
            elif counter == 2:
                 multiplier = 20
            elif counter == 3:
                 multiplier = 30
            elif counter == 4:
                 multiplier = 500
            elif counter == 5:
                 multiplier = 600
            elif counter == 6:
                 multiplier = 800
            elif counter == 7:
                 multiplier = 900

            print("Generating key {} having size = {}".format(key, multiplier*K))
            value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = multiplier*K))
            extStore.Set(key, value)

            counter += 1