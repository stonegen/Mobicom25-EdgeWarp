from concurrent.futures import thread
from typing import KeysView
import redis
from common.stateTracker import StateTracker
import time
from threading import Event


class RedisStore:

    TimeOut = 10000

    def __init__(self, ip, port, password, lock):
        self.ip = ip
        self.port = port
        self.password = password
        self.tracker = StateTracker(lock)
        self.event = Event()
        self.senderEvent = Event()
        self.lock = lock

    def Connect(self):
        self.redisClient = redis.Redis(host = self.ip, port = self.port, db=0, password=self.password)

    def Close(self):
        self.redisClient.close()

    def Set(self, key, value):
        self.redisClient.set(key, value)

        self.lock.acquire()
        self.tracker.UpdateKey(key)
        self.lock.release()

        
    def Get(self, key):
        isFound = True
        value = self.redisClient.get(key)

        if value == None:
            isFound = False


        return isFound, value

    def Migrate(self, destinationMachineIp, destinationPort, keys):
        status = self.redisClient.migrate(destinationMachineIp, destinationPort, keys, destination_db=0, timeout=10000, auth=self.password, replace=True, copy = True)

        # print(destinationMachineIp, destinationPort, keys)
        if status == b'OK':
            self.lock.acquire()
            self.tracker.UpdateSyncTime(keys)
            self.lock.release()
            return True
        else:
            return False

    
    def MigrateCopy(self, targetHost, keys):
        redisClientLocal = redis.Redis(host = self.ip, port = self.port, db=0, password=self.password)
        redisClientRemote = redis.Redis(host = targetHost, port = self.port, db=0, password=self.password)


        for key in keys:
            value = redisClientLocal.get(key)
            status = redisClientRemote.set(key, value)
                
            if status:
                syncedKeys = [key]
                self.lock.acquire()
                self.tracker.UpdateSyncTime(syncedKeys)
                self.lock.release()
            else:
                return False

        #===================================================
        # for key in keys:
        #     value1 = redisClientLocal.get(key)
        #     value2 = redisClientRemote.get(key)
        #     if value1 != value2:
        #             # print(value1)
        #             # print("\n\n\n\n\n")
        #             # print(value2)
        #             print("Key where consistency not satisfied = ", key)
        #             return False
        # print("Consistency satisfied.")
        #======================================================

        return True
    
    def SyncBackgroundCopy(self, targetHost, targetPort):
        redisClientLocal = redis.Redis(host = self.ip, port = self.port, db=0, password=self.password)
        redisClientRemote = redis.Redis(host = targetHost, port = self.port, db=0, password=self.password)
        
        # print("Background sync thread started.")
        while(True):
            self.lock.acquire()
            outOfSyncKeys = self.tracker.GetOldestUpdate()
            self.lock.release()
            # outOfSyncKeys = self.tracker.GetOutOfSyncKeys()

            for key in outOfSyncKeys:
                value = redisClientLocal.get(key)
                status = redisClientRemote.set(key, value)
                
                if status:
                    syncedKeys = [key]
                    self.lock.acquire()
                    self.tracker.UpdateSyncTime(syncedKeys)
                    self.lock.release()
            if len(outOfSyncKeys) == 0:
                time.sleep(0.01)
            

            if self.event.is_set():
                self.event.clear()
                self.lock.acquire()
                keys = self.tracker.GetOutOfSyncKeys()
                self.lock.release()
                # print("Blocking synced keys = {}.".format(keys))
                if len(keys) > 0:
                    for key in keys:
                        value = redisClientLocal.get(key)
                        status = redisClientRemote.set(key, value)
                        if status:
                            syncedKeys = [key]
                        self.lock.acquire()
                        self.tracker.UpdateSyncTime(syncedKeys)
                        self.lock.release()

                return

    def SyncBackgroundMigrate(self, targetHost, targetPort):
        redisClient = redis.Redis(host = self.ip, port = self.port, db=0, password=self.password)
        
        # while(self.tracker.AllSyncDone() == False):
        while(True):
            self.lock.acquire()
            outOfSyncKeys = self.tracker.GetOldestUpdate()
            self.lock.release()
            
            # outOfSyncKeys = self.tracker.GetOutOfSyncKeys()

            if len(outOfSyncKeys) > 0:
                status = redisClient.migrate(host=targetHost, port=self.port, keys=outOfSyncKeys, destination_db=0, \
                    timeout= 10000, auth="testpass", replace=True, copy = True)
                
                if status == b'OK':
                    self.lock.acquire()
                    self.tracker.UpdateSyncTime(outOfSyncKeys)
                    self.lock.release()
            

            if self.event.is_set():
                self.event.clear()
                self.lock.acquire()
                keys = self.tracker.GetOutOfSyncKeys()
                self.lock.release()
                if len(keys) > 0:
                    self.Migrate(targetHost, targetPort, keys)
                    self.lock.acquire()
                    self.tracker.UpdateSyncTime(keys)
                    self.lock.release()

                return