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
import multiprocessing 
from multiprocessing import Process
import asyncio
from queue import Queue


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
        self.completeWorstCaseEvent = Event()
        self.asyncHintComplete = Event()
        self.lock = lock
        self.asyncType = asyncType ## expect this value to be either 0/1
        self.NotMigration = False
        ## Extracting the Configuration Items : We will need it in creating the events Properly 
        configuration = Config("configuration/config.json")
        self.configParameters = configuration.GetConfigParameters()  
        self.t2 = 0  
        self.t1 = 0
        ## This port is a backup port incase we have to send both the Async migration during hint and handover 
        self.backupPort = 6485
        self.handoverExceptional = 0 # This flag indicates that we have concurrent Async migration during hint and Handover 
        self.handoverExceptionalTime = 0


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
        elif(size == -1) :
            return 
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

    def Migrate(self, targetHost, targetPort, keys , keySize , multiprocessQueue : Queue):
        t1 = time.time()
        migration_command = f'MIGRATE SYNC {targetHost} {targetPort} "" 0 5000000 REPLACE COPY KEYS ' + ' '.join(keys)
        status = self.redisClient.execute_command(migration_command)
        t2 = time.time()
        if status == b'OK':
            multiprocessQueue.put(["Handover", str(len(keys)) , str(keySize) , t1 , t2])
            return True
        else:
            return False

    
    async def handle_client(self, reader, writer, reply_event , closingFlag):
        while closingFlag == False:
            try :
                data = await asyncio.wait_for(reader.read(1024), timeout=3.0)
                if data:
                    str_data = data.decode('utf-8')
                    self.t2 = time.time()
                    reply_event.set()
                    continue

            except asyncio.TimeoutError:
               continue

    async def start_listening(self, receiver_host, receiver_port, reply_event , closingFlag):
        server = await asyncio.start_server(
            lambda r, w: self.handle_client(r, w, reply_event , closingFlag ), 
            receiver_host, receiver_port ,
            reuse_address=True   # Allow reusing the port
        )

        print(f"Server started on {receiver_host}:{receiver_port}")
        return server  # Return the server instance
       
    async def stop_server(self, server_task, server):
        server.close()  # Close the server
        await server.wait_closed()  # Wait for the server to close
        server_task.cancel()  # Cancel the task
        try:
            await server_task  # Ensure the task is fully canceled
        except asyncio.CancelledError:
            print("Server task canceled successfully.")
            return




    def startAsyncHandOver(self, targetHost, targetPort,  multiprocessQueue : Queue , recieverHost = None, recieverPort = None, keySize = 1024):
        
        asyncio.run(self.SyncHandOver(targetHost, targetPort, multiprocessQueue , recieverHost , recieverPort, keySize))




    async def SyncHandOver(self, targetHost, targetPort,  multiprocessQueue : Queue , recieverHost = None, recieverPort = None, keySize = 1024):

        ## Connect to the redis Instances :
        self.handoverExceptional = False
        redisClientLocal = redis.Redis(host = self.ip, port = self.port, db=0)
        redisClientRemote = redis.Redis(host = targetHost, port = targetPort, db=0)

        ## Backup :
        reply_eventH = asyncio.Event()  
        serverH = await self.start_listening(recieverHost, recieverPort , reply_eventH , self.handoverExceptional)
        listen_taskH = asyncio.create_task(serverH.serve_forever())  # Run the server in the background

        t1 = time.time()
        self.completeWorstCaseEvent.clear()
        self.asyncHintComplete.wait()
        self.lock.acquire()
        keys = self.tracker.GetOutOfSyncKeys()
        self.lock.release()
        
        if(len(keys) > 0) :
            
            migration_command = f'MIGRATE ASYNC {targetHost} {targetPort} {recieverHost} {self.backupPort} "" 0 5000000 REPLACE KEYS ' + ' '.join(keys)
            migrateResponse = redisClientLocal.execute_command(migration_command) ## For now assuming that responses are correct 
            await reply_eventH.wait()
            reply_eventH.clear()                         
            self.lock.acquire()
            self.tracker.moveMigratedKeys(keys ,t1)
            self.lock.release()

        t2 = time.time()
        self.asyncHintComplete.clear()
        print("Final HandOver Time : " , len(keys) )
        print((t2-t1)*1000)
        multiprocessQueue.put(["Handover", str(len(keys)) , str(keySize) , t1 , t2])
        ## Closing connection :
        self.handoverExceptional = True
        migration_command = f'MIGRATE CLOSE {recieverHost} {recieverPort} {targetHost} {targetPort}'
        migrateResponse = redisClientLocal.execute_command(migration_command)
        print("ASYNCHRONOUS BACKGROUND THREAD FROM SOURCE REDIS : ", migrateResponse)
        migrateResponse = redisClientRemote.execute_command(migration_command)
        print("ASYNCHRONOUS BACKGROUND THREAD FROM DESTINATION REDIS : ", migrateResponse)
        await self.stop_server(listen_taskH, serverH)
        print("HandOver Returning !")
        
        pending = asyncio.all_tasks()
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                print(f"Task canceled")

        print("Async migration loop finished")
    


    def startAsyncMigration(self, targetHost, targetPort, multiprocessQueue : multiprocessing.Queue , recieverHost = None, recieverPort = None, keySize = 1024):
        asyncio.run(self.SyncBackgroundCopy(targetHost, targetPort, multiprocessQueue , recieverHost , recieverPort, keySize))


    async def SyncBackgroundCopy(self, targetHost, targetPort, multiprocessQueue : multiprocessing.Queue , recieverHost = None, recieverPort = None, keySize = 1024):
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

        redisClientLocal = redis.Redis(host = self.ip, port = self.port, db=0)
        redisClientRemote = redis.Redis(host = targetHost, port = targetPort, db=0)
        print("Adding listener on the : ", recieverHost , recieverPort)
        self.NotMigration = False
        if(self.asyncType) :

            reply_event = asyncio.Event()  
            server = await self.start_listening(recieverHost, recieverPort, reply_event , self.NotMigration)
            listen_task = asyncio.create_task(server.serve_forever())  # Run the server in the background


        while(True):

            ## Securely aquiring the keys for migration 
            self.lock.acquire()
            outOfSyncKeys = self.tracker.GetOldestUpdate(self.configParameters.minOldestUpdates, self.configParameters.maxOldestUpdates)
            self.lock.release()

            ## Checking which type of Async Migration we want :
            if (self.asyncType) :

                # Creating & Executing Migration Command :
                if(len(outOfSyncKeys) != 0 and not self.event.is_set()):
                    
                    # self.asyncHintComplete.set()
                    self.asyncHintComplete.clear()
                    t1 = time.time() 
                    migration_command = f'MIGRATE ASYNC {targetHost} {targetPort} {recieverHost} {recieverPort} "" 0 5000000 REPLACE KEYS ' + ' '.join(outOfSyncKeys)
                    _ = redisClientLocal.execute_command(migration_command) ## Assuming we will be recieving positive response for now            
                    self.lock.acquire()
                    self.tracker.moveMigratedKeys(outOfSyncKeys , t1)
                    self.lock.release()
                    self.asyncHintComplete.set()
                    await reply_event.wait()
                    t2 = self.t2
                    reply_event.clear()
                    multiprocessQueue.put(["Hint", str(len(outOfSyncKeys)) , str(keySize) , t1 , t2])
                    ## Safely updating the Status of the Synchronised Keys
                
                if(len(outOfSyncKeys) == 0 and not self.event.is_set()):
                    await asyncio.sleep(0.005) ## we assume the 5ms will be the smallest update window for request by client

                if self.event.is_set():
                    self.asyncHintComplete.set()
                    self.event.clear()
                    self.NotMigration = True 
                    await self.stop_server(listen_task, server)
                    migration_command = f'MIGRATE CLOSE {recieverHost} {recieverPort} {targetHost} {targetPort}'
                    migrateResponse = redisClientLocal.execute_command(migration_command)
                    print("ASYNCHRONOUS BACKGROUND THREAD FROM SOURCE REDIS  : ", migrateResponse)
                    migrateResponse = redisClientRemote.execute_command(migration_command)
                    print("ASYNCHRONOUS BACKGROUND THREAD FROM DESTINATION REDIS : ", migrateResponse)
                    break

            else :    

                ## Older Asynchronous Migration Implementation 
                if(len(outOfSyncKeys) != 0) and not (self.event.is_set()):
                    t1 = time.time()
                    for key in outOfSyncKeys:
                        value = redisClientLocal.get(key)
                        status = redisClientRemote.set(key, value)
                    t2 = time.time()                

                    self.lock.acquire()
                    self.tracker.moveMigratedKeys(outOfSyncKeys , t1)
                    self.lock.release()

                    multiprocessQueue.put(["Hint", str(len(outOfSyncKeys)) , str(keySize) , t1 , t2])
                else :
                    await asyncio.sleep(0.01)
                
                "Checking in non blocking way that whether signal has been released or not"
                if self.event.is_set() and self.completeWorstCaseEvent.is_set():
                    t1 = time.time()
                    self.lock.acquire()
                    keys = self.tracker.GetOutOfSyncKeys()
                    self.lock.release()
                    self.event.clear()
                    self.completeWorstCaseEvent.clear()
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
                    multiprocessQueue.put(["Handover", str(len(keys)) , str(keySize) , t1 , t2])
                    break  
                             

        await self.stop_server(listen_task, server)             
        pending = asyncio.all_tasks()
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                print(f"Task canceled")

        print("Async migration loop finished")