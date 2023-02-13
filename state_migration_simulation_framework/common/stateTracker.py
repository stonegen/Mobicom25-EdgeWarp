from operator import ne
import time
import redis
from _thread import *
import threading
import sys
from threading import Lock

class History:
    def __init__(self, updateTime, key):
        self.updateTime = updateTime
        self.syncTime = 0
        self.key = key
        self.updateCounter = 0
        self.syncCounter = 0
        self.creationTime = time.time()
        self.keySize = 0
        self.updateFrequency = 0

    def Fequency(self):
        frequency = 0

        # Wait for at least three updates to calculate frequency
        if self.updateCounter >= 3:
            duration = time.time() - self.creationTime
            frequency = self.updateCounter / duration
            self.updateFrequency = frequency

        return frequency

   

class StateTracker:
    def __init__(self, lock):
        self.stateDict = {}
        self.lock = lock
        self.updateRateUpperLimit = 10
        self.bigKeyUpperLimit = 10*1024 # 100KB: This should be based on backend bandwidth but hard-coded for now.

    def AddKeys(self, key, size):
        self.stateDict[key] = History(time.time(), key)
        self.stateDict[key].keySize = size

    
    def UpdateKey(self, key):
        try:
            if key in self.stateDict:
                self.stateDict[key].updateTime = time.time()
            else:
                self.stateDict[key] = History(time.time(), key)

            self.stateDict[key].updateCounter += 1
        finally:
            pass

    def UpdateSyncTime(self, keys):
        try:
            for key in keys:
                self.stateDict[key].syncTime = time.time()
                self.stateDict[key].syncCounter += 1
        finally:
            pass

    def GetOldestUpdate(self):
        
        tempTime = sys.float_info.max
        tempKey = None
        keys = []
        try:
            for key, Val in self.stateDict.items():
                # The key idea of below if condition is that if a key is small (it blocking migration will take less time) and 
                # its update frequency is high, then leave its migration for final state sync state. It will save bandwidth and
                # avoid the impact during background sync.

                if Val.keySize > self.bigKeyUpperLimit:
                    if tempTime > Val.updateTime and Val.syncTime < Val.updateTime:
                        tempTime = Val.updateTime
                        tempKey = key
                else:
                    if Val.Fequency() <= self.updateRateUpperLimit:
                        if tempTime > Val.updateTime and Val.syncTime < Val.updateTime:
                            tempTime = Val.updateTime
                            tempKey = key

        finally:
            pass

        if tempKey != None:
            keys.append(tempKey)
        
        return keys


    
    def GetOutOfSyncKeys(self):
        # print(self.stateDict, flush=True)
        keys = []

        try:
            for key, Val in self.stateDict.items():
                if Val.updateTime > Val.syncTime:
                    keys.append(key)

        finally:
            pass

        
        return keys

    

    def GetLatestUpdateTime(self):

        try:
            tempItem = list(self.stateDict.keys())[0]

            for key, Val in self.stateDict.items():
                if tempItem.updateTime < Val.updateTime:
                    tempItem.updateTime = Val.updateTime

        finally:
            pass
            
        return tempItem.updateTime

    
    # def Sort(self):

    def PrintKeys(self):
        
        try:
            for key, Val in self.stateDict.items():
                # print("key = {0}, Update = {1}, Sync  time= {2}, sync counter = {3}.".format(key, Val.updateTime, Val.syncTime, Val.syncCounter))
                print("key = {}, Size = {} bytes, Update counter = {}, Update frequency = {}, sync counter = {}.".format(key, Val.keySize, Val.updateCounter, int(Val.Fequency()), Val.syncCounter))

        finally:
            pass

    def AllSyncDone(self):
        try:
            for key, val in self.stateDict.items():
                if val.updateTime > val.syncTime:
                    return False
            return True

        finally:
            pass
