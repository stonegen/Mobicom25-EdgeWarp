from concurrent.futures import thread
from typing import KeysView
from pymemcache.client import base
from common.stateTracker import StateTracker
import time
from threading import Event


class MemcacheStore:

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
        # self.memcacheClient = memcache.memcache(host = self.ip, port = self.port, db=0, password=self.password)
        self.memcacheClient = base.Client((self.ip, 11211))

    def Close(self):
        self.memcacheClient.close()

    def Set(self, key, value):
        self.memcacheClient.set(key, value, noreply=False)

        self.lock.acquire()
        self.tracker.UpdateKey(key)
        self.lock.release()

        
    def Get(self, key):
        isFound = True
        value = self.memcacheClient.get(key)

        if value == None:
            isFound = False


        return isFound, value

    def Migrate(self, destinationMachineIp, destinationPort, keys):
        memcacheClientLocal = base.Client((self.ip, 11211))
        memcacheClientRemote = base.Client((destinationMachineIp, 11211))


        for key in keys:
            value = memcacheClientLocal.get(key)
            status = memcacheClientRemote.set(key, value)
                
            if status:
                syncedKeys = [key]
                self.lock.acquire()
                self.tracker.UpdateSyncTime(syncedKeys)
                self.lock.release()
            else:
                return False

        return True
    
    def MigrateCopy(self, targetHost, keys):
        memcacheClientLocal = base.Client((self.ip, 11211))
        memcacheClientRemote = base.Client((targetHost, 11211))


        for key in keys:
            value = memcacheClientLocal.get(key)
            status = memcacheClientRemote.set(key, value)
                
            if status:
                syncedKeys = [key]
                self.lock.acquire()
                self.tracker.UpdateSyncTime(syncedKeys)
                self.lock.release()
            else:
                return False

        return True
    
    def SyncBackgroundCopy(self, targetHost, targetPort):
        memcacheClientLocal = base.Client((self.ip, 11211))
        memcacheClientRemote = base.Client((targetHost, 11211))
        
        # print("Background sync thread started.")
        while(True):
            self.lock.acquire()
            outOfSyncKeys = self.tracker.GetOldestUpdate()
            self.lock.release()
            # outOfSyncKeys = self.tracker.GetOutOfSyncKeys()

            for key in outOfSyncKeys:
                
                value = memcacheClientLocal.get(key)
                status = memcacheClientRemote.set(key, value)
                
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
                        value = memcacheClientLocal.get(key)
                        status = memcacheClientRemote.set(key, value)
                        if status:
                            syncedKeys = [key]
                        self.lock.acquire()
                        self.tracker.UpdateSyncTime(syncedKeys)
                        self.lock.release()

                return