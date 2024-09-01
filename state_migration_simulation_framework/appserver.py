# import imp
from click import pass_context
from pyparsing import nullDebugAction
import redis
import string
import random
import datetime
import time
import pickle
from common.redisStore import RedisStore
from common.memcacheStore import MemcacheStore
from common.state import State
from configuration.config import Config
from common import stateTracker
from common.state import State
from common.messageType import MessageType
import socket
import struct
from common.util import UtilityFunctions
from _thread import *
import threading
# from multiprocessing import Process, Lock ,Event,Queue,Manager
# from multiprocessing.managers import BaseManager
from mobility_handler.mobilityHandler import MobilityHandler
from queue import Queue
from configuration.configParameters import ConfigParameters
from threading import Thread,Lock
from common.keySaver import KeySaver
from common.stateModifier import StateModifier
from common.logger import logger
import multiprocessing



# Load configuration paramters from file.
configuration = Config("configuration/config.json")
configParameters = configuration.GetConfigParameters()

mobilityHandler = MobilityHandler(configParameters.localIp, configParameters.mobilityPort)
mobilityQueue = Queue()


lock = Lock()
mobilityHadlerExpectedDataLen = 5
userId = "123"
EndServer = False


def sendData(connection, data):
    try:
        connection.sendall(data)
    except socket.error as e:
        print("Failed to send packet: Detail = {}.".format(e))
        pass

def RedisStoreCreator(localIp, storePortNumber, storePassword, asyncType, lock):
    return RedisStore(localIp, storePortNumber, storePassword, asyncType, lock)

def serve(conn, addr, lock, mobilityQueue, mobilityHadlerExpectedDataLen, clientsServed):
    
    configParameters = configuration.GetConfigParameters()
    # print("-----------------------------------------------------------------------------")
    if (configParameters.isDefaultMigrationEnabled):
        message = "{} default scheme. App sever connected by [{}] = {}.".format(configParameters.storeType, clientsServed, addr)
    else:
        message = "{} modified scheme. App sever connected by [{}] = {}.".format(configParameters.storeType, clientsServed, addr)

    extStore = None
    if configParameters.storeType == "Redis":
        extStore = RedisStore(configParameters.localIp, configParameters.storePortNumber,configParameters.storePasswrod, configParameters.asyncType ,lock)
    elif configParameters.storeType == "Memcache":
        extStore = MemcacheStore(configParameters.localIp, configParameters.storePortNumber,configParameters.storePasswrod, lock)
    extStore.Connect()  

    eventLogger = logger()
    multiProcessQueue = multiprocessing.Queue()
    stateModifier = StateModifier(extStore, configParameters.dynamicVariables)
    keysaver = KeySaver()
    keys = keysaver.CreateKeys(extStore, configParameters.totalVariables, configParameters.keySize, configParameters.numberOfClients, configParameters.appName)

    backgroundThread = Thread(target=extStore.startAsyncMigration, args=(configParameters.remoteIp, configParameters.remotePortNumer, multiProcessQueue, configParameters.localIp , 6484 , configParameters.keySize))
    
    ## Background Thread 2 is only required in the case of Redis Handover via Asynchronous Migration :
    if(configParameters.isDefaultMigrationEnabled == 0 and configParameters.asyncType == 1): 
        backgroundThread2 = Thread(target=extStore.startAsyncHandOver, args=(configParameters.remoteIp, configParameters.remotePortNumer, multiProcessQueue, configParameters.localIp , 6485 , configParameters.keySize))

    EVENT_TYPE = "Normal"
    counter = 0
    expcetedDataLength = 137
    shallServe = True
    isAppExitTimerStarted = False
    appExitStartTime = 0

    while shallServe:

        data = conn.recv(expcetedDataLength)
        if data:
            try:
                recivedRequest = pickle.loads(data)
                userID = recivedRequest.id
                
                if recivedRequest.receivedFrom == MessageType.Client:
                    counter += 1
                    stringId = str(userID)
                    isFound, userStateBytes = extStore.Get(stringId)
                    if isFound and counter > 1:
                        
                        t1 = time.time()
                        # print("Updating keys")
                        stateModifier.Update(keys)
                        # State from external store
                        userState = pickle.loads(userStateBytes)
                        messageExternalStore = struct.unpack('dii', userState.getMessage())
                        # State from client
                        messageClient = struct.unpack('dii', recivedRequest.getMessage())
                        buffer = struct.pack('dii', messageClient[0], messageClient[1], messageExternalStore[2] + 1)
                        # Set reply to client
                        userState.setMessage(buffer)
                        serializedState = pickle.dumps(userState)
                        sendData(conn, serializedState)
                        t2R = time.time()
                        eventLogger.LogResponseTimes( EVENT_TYPE, stateModifier.counter , ((t2R - t1) * 1000))
                    else:
                    
                        messageClient = struct.unpack('dii', recivedRequest.getMessage())
                        buffer = struct.pack('dii', messageClient[0], messageClient[1], 0)
                        # Setting the msg of client into store if not found
                        recivedRequest.setMessage(buffer)
                        serverResponse = pickle.dumps(recivedRequest)
                        extStore.Set(stringId, serverResponse ,-1)
                        sendData(conn, serverResponse)
                    
                elif recivedRequest.receivedFrom == MessageType.MobilityHandoverAck:
                    shallServe = False # User served. Now exit this thread. A new user will open new thread.

                else :
                    messageClient = struct.unpack('dii', recivedRequest.getMessage())
                    buffer = struct.pack('dii', messageClient[0], messageClient[1], 0)
                    recivedRequest.setMessage(buffer)
                    serverResponse = pickle.dumps(recivedRequest)
                    sendData(conn, serverResponse)
            


            except Exception as ex:
                print("Oops! ",ex, " occurred.")
                pass

        # Check if any message from mobility handler is in queue.
        if mobilityQueue.empty() == False:
            message = mobilityQueue.get()
            # A valid message is of length 5 bytes for now.
            if len(message) >= mobilityHadlerExpectedDataLen:
                ipBytes = message[0:4] # First four bytes for IP
                ip = '.'.join(f'{c}' for c in ipBytes)
                messageType = message[4] # Last byte for message type. 0: mobility hint, 1: mobility handover.
                # print("Event type = ", messageType, ", IP = ", ip)

                if messageType == MessageType.MobilityHint.value: # Mobility hint received.
                    print("Mobility Hint Message Recieved ! \n")
                    EVENT_TYPE = "Hint"
                    if configParameters.isDefaultMigrationEnabled == 0:
                        print("Background Thread started !")
                        backgroundThread.start()

                    mobilityState = State(456, MessageType.MobilityHint)
                    buffer = UtilityFunctions.ip2int(ip)
                    mobilityState.setMessage(buffer)
                    serverResponse = pickle.dumps(mobilityState)
                    serverResponse += bytearray(12) # Just to make size of every message equal to 137
                    sendData(conn, serverResponse)

                elif messageType == MessageType.MobilityHandover.value: # Mobility handover message received.
                    print(f"Mobility Handover Message Recieved ! {configParameters.isDefaultMigrationEnabled} \n")
                    t1 = time.time()
                    mobilityState = State(456, MessageType.MobilityHandover)
                    buffer = UtilityFunctions.ip2int(ip)
                    mobilityState.setMessage(buffer)
                    serverResponse = pickle.dumps(mobilityState)
                    if configParameters.isDefaultMigrationEnabled == 1:
                        status = extStore.Migrate(configParameters.remoteIp, configParameters.remotePortNumer, keys, configParameters.keySize ,multiProcessQueue)                  
                        if status == True:
                            serverResponse += bytearray(12) # Just to make size of every message equal to 137
                            sendData(conn, serverResponse) 
                            ## Storing the data from the Queue to the file :
                            while not multiProcessQueue.empty():
                                message = multiProcessQueue.get()
                                eventLogger.LogEventTimes(message[0], message[1], message[2], message[3], message[4])
                                
                            isAppExitTimerStarted = True
                            extStore.Close()
                        else:
                            print("******* Failed to sync state. ********")
                    else:
                    
                        # Send event to the backgrond sync that mobility handover is triggered.
                        extStore.t1 = time.time()
                        extStore.event.set()
                        ## We will be replicating the worst Case Scenario i.e. Just Before the Handover signal, all dynamic states 
                        ## will be altered (if this is mentioned in the Configuration File)
                        if(configParameters.WorstCase == 1):
                            stateModifier.Update(keys)
                        extStore.completeWorstCaseEvent.set()
                        
                        if(configParameters.isDefaultMigrationEnabled == 0 and configParameters.asyncType == 1): 
                            backgroundThread2.start()
                        backgroundThread.join()
                        if(configParameters.isDefaultMigrationEnabled == 0 and configParameters.asyncType == 1): 
                            backgroundThread2.join()
                        extStore.event.clear()
                        extStore.Close()
                    
                        ## Storing the data from the Queue to the file :
                        while not multiProcessQueue.empty():
                            message = multiProcessQueue.get()
                            eventLogger.LogEventTimes(message[0], message[1], message[2], message[3], message[4])
    
                        migrationTimeMs = (time.time() - t1) * 1000
                        serverResponse += bytearray(12) # Just to make size of every message equal to 137
                        sendData(conn, serverResponse)
                        isAppExitTimerStarted = True # Start App exit timer. This is just to make 
                        # sure that socket is not closed before sending the mobility handover message
                        # to the client device. Can have better mechanism to handle it.
                        appExitStartTime = time.time()
                        
            
                    migrationTimeMs = (time.time() - t1) * 1000
                    print(migrationTimeMs)
                    

        if isAppExitTimerStarted == True:
            if (time.time() - appExitStartTime) >= 1: # Timeout 1000 ms
                print("App exit timeout. Closing.")
                EndServer = True
                shallServe = False
                conn.close()
            


sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('', configParameters.portNumer))
sock.listen()
print("App server listening on: ", configParameters.localIp,":",configParameters.portNumer)


mobilityProcess = Thread(target = mobilityHandler.Process, args=(mobilityQueue, mobilityHadlerExpectedDataLen,))
mobilityProcess.start()
clientsServed = 1

# Keep listening to accpet new connections.
## For the sake of simulation with one client we will limit ourselves with just 1 thread and 1 client

while True:

    conn, addr = sock.accept()
    # clientThread = Process(target=serve, args=(conn, addr, lock, mobilityQueue, mobilityHadlerExpectedDataLen, clientsServed))
   
    serve(conn, addr, lock, mobilityQueue, mobilityHadlerExpectedDataLen, clientsServed)
    # clientThread.start()
    clientsServed += 1
    # clientThread.join()
    mobilityProcess.join()
    print("Exiting Bye !")
    sock.close()
    break

    
            



