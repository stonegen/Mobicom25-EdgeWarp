from concurrent.futures import thread
from typing import KeysView
import redis
from common.stateTracker import StateTracker
import time
import socket
from threading import Event,Thread
from common.logger import logger
import select
from configuration.configParameters import ConfigParameters
from configuration.config import Config


"""
    Explaination :
        Wrapper classs & Methods for Redis datastore 
"""

class RedisStore:



    TimeOut = 10000

    def __init__(self, ip, port, password, asyncType ,lock):
        self.ip = ip
        self.port = port
        self.password = password
        self.tracker = StateTracker(lock)
        self.event = Event()
        self.senderEvent = Event()
        self.lock = lock
        self.asyncType = asyncType ## expect this value to be either 0/1
        self.eventAsyncReplyRecieved = Event() ## event for listening to replies after Async migration call
        self.stopAsyncListening = Event() ## event used to prevent async migration replies listener once the mobility handover is recieved

        ## Extracting the Configuration Items : We will need it in creating the events Properly 
        configuration = Config("configuration/config.json")
        self.configParameters = configuration.GetConfigParameters()    




    def Connect(self):
        self.redisClient = redis.Redis(host = self.ip, port = self.port, db=0)

    def Close(self):
        self.redisClient.close()

    def Set(self, key, value,size = None):
        self.redisClient.set(key, value)
        if(size == None):
            self.lock.acquire()
            self.tracker.UpdateKey(key)
            self.lock.release()
        else :
            self.lock.acquire()
            self.tracker.hashTable.add_node(key, size , 1)
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
        redisClientLocal = redis.Redis(host = self.ip, port = self.port, db=0)
        redisClientRemote = redis.Redis(host = targetHost, port = self.port, db=0)


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
    
    
    def startListening(self, receiverHost, receiverPort):
        self.receiverHost = receiverHost
        self.receiverPort = receiverPort
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.receiverHost, self.receiverPort))
        self.server_socket.listen()
        self.server_socket.setblocking(False)
        self.client_conn = None
        print(f"Listening on {self.receiverHost}:{self.receiverPort}...")

    def accept_new_connection(self):
        self.client_conn, addr = self.server_socket.accept()
        self.client_conn.setblocking(False)  # Non-blocking mode for client connection
        print(f"Connected by {addr}")

    def checkForMessages(self):
        if self.client_conn is None:
            ready_to_read, _, _ = select.select([self.server_socket], [], [], 1)
            if ready_to_read:
                self.accept_new_connection()
        else:
            ready_to_read, _, _ = select.select([self.client_conn], [], [], 1)
            if ready_to_read:
                data = self.client_conn.recv(1024)
                if data:
                    # print(f"Message received: {data.decode('utf-8')}")
                    # self.eventAsyncReplyReceived.set()
                    return True
                else:
                    # Client closed the connection
                    self.client_conn.close()
                    self.client_conn = None
                    print("Client disconnected")
        return False




    # def checkForMessages(self):
    #     ready_to_read, _, _ = select.select([self.server_socket], [], [], 1)
    #     if ready_to_read:
    #         conn, addr = self.server_socket.accept()
    #         with conn:
    #             data = conn.recv(1024)
    #             if data:
    #                 print(f"Message received: {data.decode('utf-8')}")
    #                 # self.eventAsyncReplyReceived.set()
    #                 return True
    #     return False


    def listenToAsyncReplies(self, recieverHost, recieverPort):
        """
            Explaination :
            This function will listen to the following details & will perform appropriate action 
            via signaling and changing state of the shared variable between itself & the caller
        """
        def listen():

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((recieverHost, recieverPort))
                s.listen()
                print(f"Listening on {recieverHost}:{recieverPort}...")

                ## We will continously listen to the Asyn Replies till the mobility hint is offered to us  
                while (not (self.stopAsyncListening.is_set())):
                    conn, addr = s.accept()
                    with conn:
                        # print(f"Connected by {addr}")
                        while not self.stopAsyncListening.is_set():
                            data = conn.recv(1024)
                            if data:
                                # print("Message received:", data.decode('utf-8'))
                                self.eventAsyncReplyRecieved.set()
                            else :
                                break

                self.stopAsyncListening.clear()
        
        listen_thread = Thread(target=listen)
        listen_thread.start()



    
    def SyncBackgroundCopy(self, targetHost, targetPort, eventLogger : logger , recieverHost = None, recieverPort = None, keySize = 1024):
        """
            Explaination :
            This function repetitively checks for the keys that are out of Sync and get-sets the 
            updated keys by finding the Oldest updated key on each iteration. 
            However once it gets the signal, it gets all the out of Sync keys and get-sets them
            to the remote server. 

            Parameters description :
            targetHost --> remote DataStore's IP addr
            targetPort --> remote DataStore's Port Number
            recieverHost --> IP addr to listen for Asyn Redis replies 
            recieverPort --> Port to listen for Asyn Redis replies
        """
        ## Initialising the logger :
        redisClientLocal = redis.Redis(host = self.ip, port = self.port, db=0)

        # redisClientRemote = redis.Redis(host = targetHost, port = targetPort, db=0)
        if(self.asyncType) :
            print("async\n")
            # self.accept_new_connection()
            self.startListening(recieverHost, recieverPort)
            # self.listenToAsyncReplies(recieverHost , recieverPort)
        else :
            redisClientRemote = redis.Redis(host = targetHost, port = targetPort, db=0)

        # print("Background sync thread started.")
        while(True):
            t1 = time.time()
            self.lock.acquire()
            outOfSyncKeys = self.tracker.GetOldestUpdate(self.configParameters.minOldestUpdates, self.configParameters.maxOldestUpdates)
            self.lock.release()
            t2 = time.time()
            # print("time it takes to Get oldest states : ", (t2  - t1)*1000 , " ms")
            # outOfSyncKeys = self.tracker.GetOutOfSyncKeys()

            ## Checking which type of Async Migration we want :
            if (self.asyncType) :

                # Creating & Executing Migration Command :
                if(len(outOfSyncKeys) != 0):

                    migration_command = f'MIGRATE ASYNC {targetHost} {targetPort} {recieverHost} {recieverPort} "" 0 5000000 REPLACE KEYS ' + ' '.join(outOfSyncKeys)
                    t1 = time.time()
                    
                    immediateResponse = redisClientLocal.execute_command(migration_command) 
                    # print(f'Immediate Response after Async Migration : {immediateResponse}\n')
                    while self.checkForMessages() == False :
                        continue
                    t2 = time.time()
                        # Briefly sleep to prevent busy-waiting
                    # self.eventAsyncReplyRecieved.wait()
                    
                    # self.eventAsyncReplyRecieved.clear() 
                    ## Assuming Responses are correct for now :
                    eventLogger.LogEventTimes("Hint", str(len(outOfSyncKeys)) , str(keySize) , t1 , t2)
                    t1 = time.time()
                    self.lock.acquire()
                    # self.tracker.UpdateSyncTime(outOfSyncKeys)
                    self.tracker.moveMigratedKeys(outOfSyncKeys , t1)
                    self.lock.release()
                    t2 = time.time()
                    # eventLogger.LogEventTimes("hint", str(len(outOfSyncKeys)) , str(keySize) , t1 , t2)
                    # print("time it takes to update states : ", (t2  - t1)*1000 , " ms")
                    # self.asyncType = 0                    

                else :

                    time.sleep(0.01)

                # print("hereeeee")
                if self.event.is_set():
                    t1 = time.time()
                    # self.stopAsyncListening.set()
                    self.event.clear()
                    self.lock.acquire()
                    keys = self.tracker.GetOutOfSyncKeys()
                    self.lock.release()
                    
                    if(len(keys) > 0) :
                        # migration_command = f'MIGRATE SYNC {targetHost} {targetPort} "" 0 50000000 REPLACE KEYS ' + ' '.join(keys)
                        migration_command = f'MIGRATE ASYNC {targetHost} {targetPort} {recieverHost} {recieverPort} "" 0 5000000 REPLACE KEYS ' + ' '.join(keys)

                        migrateResponse = redisClientLocal.execute_command(migration_command)

                        ## Waiting for the reply :
                        while self.checkForMessages() == False :
                            continue
                        
                        ## For now assuming that responses are correct :
                        self.lock.acquire()
                        # self.tracker.UpdateSyncTime(keys)
                        self.tracker.moveMigratedKeys(keys)
                        self.lock.release()
                    t2 = time.time()
                    eventLogger.LogEventTimes("HandOver", str(len(keys)) , str(keySize) , t1 , t2)
                    print("thread is returning!")
                    return

            else :    
                ## We may have to change this to get all of the possible Oldest Keys

                
                if(len(outOfSyncKeys) != 0):
                    t1 = time.time()
                    for key in outOfSyncKeys:
                        value = redisClientLocal.get(key)
                        status = redisClientRemote.set(key, value)
                        print(f"Status is {status}")

                    t2 = time.time()                
                        
                    self.lock.acquire()
                    self.tracker.moveMigratedKeys(outOfSyncKeys , t1)
                    self.lock.release()

                 
                    eventLogger.LogEventTimes("hint", str(len(outOfSyncKeys)) , str(keySize) , t1 , t2)

                else :
                    time.sleep(0.01)
                
                "Checking in non blocking way that whether signal has been released or not"
                if self.event.is_set():
                    t1 = time.time()
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
                            self.tracker.moveMigratedKeys(syncedKeys)
                            self.lock.release()
                    t2 = time.time()                
                    eventLogger.LogEventTimes("HandOver", str(len(keys)) , str(keySize) , t1 , t2)
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