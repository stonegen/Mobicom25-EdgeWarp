import imp
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
import sys
import os
import struct
from common.util import UtilityFunctions
from _thread import *
import threading
from multiprocessing import Process, Lock
from mobility_handler.mobilityHandler import MobilityHandler
from queue import Queue
from configuration.configParameters import ConfigParameters
from threading import Thread
from common.keySaver import KeySaver
import multiprocessing 
from common.stateModifier import StateModifier


# Load configuration paramters from file.
configuration = Config("configuration/config.json")
configParameters = configuration.GetConfigParameters()

mobilityHandler = MobilityHandler(configParameters.localIp, 10001)
mobilityQueue = Queue()

lock = Lock()

N = 5*1024*1024

mobilityHadlerExpectedDataLen = 5

userId = "123"

def  modifyState(externalStore, key):
    isKeyFound, state = externalStore.Get(key)

    if isKeyFound:
        stateList = list(state)
        randomByte = random.randint(0, 255)
        stateList[0] = randomByte
        state = bytes(stateList)
        externalStore.Set(key, state)

def sendData(connection, data):
    try:
        connection.sendall(data)
    except socket.error as e:
        # print("Failed to send packet: Detail = {}.".format(e))
        pass

def serve(conn, addr, lock, mobilityQueue, mobilityHadlerExpectedDataLen, clientsServed):
    configParameters = configuration.GetConfigParameters()

    # print("-----------------------------------------------------------------------------")
    if (configParameters.isDefaultMigrationEnabled):
        message = "{} default scheme. App sever connected by [{}] = {}.".format(configParameters.storeType, clientsServed, addr)
        # print(message)
    else:
        message = "{} modified scheme. App sever connected by [{}] = {}.".format(configParameters.storeType, clientsServed, addr)
        # print(message)


    extStore = None
    if configParameters.storeType == "Redis":
        extStore = RedisStore(configParameters.localIp, configParameters.storePortNumber, \
            configParameters.storePasswrod, lock)
    elif configParameters.storeType == "Memcache":
        extStore = MemcacheStore(configParameters.localIp, configParameters.storePortNumber, \
            configParameters.storePasswrod, lock)
    extStore.Connect()  

    stateModifier = StateModifier(extStore, configParameters.dynamicVariables)

    counter = 0

    backgroundThread = threading.Thread()

    # keysavor = KeySaver()
    # keys = keysavor.CreateKeys(configParameters.localIp, configParameters.storePortNumber, configParameters.totalVariables, configParameters.storeType)


    keys = ["user:123:key1"] # 1,048,576
    keys.append("user:123:key2") # 1,048,576
    keys.append("user:123:key3") # 1,048,576
    keys.append("user:123:key4") # 1,048,576
    keys.append("user:123:key5") # 1,048,576
    keys.append("user:123:key6") # 1,048,576
    keys.append(userId)

    mobilityHadlerExpectedDataLen = 5

    expcetedDataLength = 137

    for key in keys:
        isKeyExists, value = extStore.Get(key)
        if isKeyExists:
            extStore.tracker.AddKeys(key, len(value));
            # print("Key {} exists with length = {}.".format(key, len(value)))
    # print(extStore.tracker.PrintKeys())

    backgroundThread = Thread(target=extStore.SyncBackgroundCopy, args=(configParameters.remoteIp, configParameters.remotePortNumer, ))
    # backgroundThread = Thread(target=extStore.SyncBackgroundMigrate, args=(configParameters.remoteIp, configParameters.remotePortNumer, ))

    shallServe = True

    isAppExitTimerStarted = False
    appExitStartTime = 0
    
    while shallServe:
        data = conn.recv(expcetedDataLength)
        if data:

            try:
                recivedRequest = pickle.loads(data)
                userID = recivedRequest.id
                

                # A request is received from client.
                if recivedRequest.receivedFrom == MessageType.Client:
                    counter += 1
                    stringId = str(userID)
                    # time.sleep(0.002)
                    t1 = time.time()
                    isFound, userStateBytes = extStore.Get(stringId)
                    # rttMicroSeconds = (time.time() - t1) * 1000000
                    # text = "Req. No. = {}, Time = {} micro seconds, key = {}, key length = {}, found = {}, client no. {}". \
                    #     format(counter, rttMicroSeconds, stringId, len(userStateBytes), isFound, clientsServed)
                    # print(text)
                    if isFound:
                        # ----------------------------------------------------------------------------------
                        stateModifier.Update(keys)
                        # for i in range(configParameters.dynamicVariables):
                        #     # modifyState(extStore, keys[counter % configParameters.dynamicVariables])
                        #     # break
                        #     modifyState(extStore, keys[i])
                        # --------------------------------------------------------------------------------

                        # State from external store
                        userState = pickle.loads(userStateBytes)
                        
                        messageExternalStore = struct.unpack('dii', userState.getMessage())

                        # State from client
                        messageClient = struct.unpack('dii', recivedRequest.getMessage())
                        
                        buffer = struct.pack('dii', messageClient[0], messageClient[1], messageExternalStore[2] + 1)
                        # Set reply to client
                        userState.setMessage(buffer)

                        serializedState = pickle.dumps(userState)

                        # t2 = time.time()
                        extStore.Set(stringId, serializedState)

                        # delta_t2 = (time.time() - t2) * 1000000
                        # print("Key set time = {}, key size = {}, client = {}".format(delta_t2, len(serializedState), clientsServed))

                        sendData(conn, serializedState)
                    else:
                        messageClient = struct.unpack('dii', recivedRequest.getMessage())

                        
                        buffer = struct.pack('dii', messageClient[0], messageClient[1], 0)
                        # Set reply to client
                        recivedRequest.setMessage(buffer)
                        serverResponse = pickle.dumps(recivedRequest)
                        extStore.Set(stringId, serverResponse)
                        sendData(conn, serverResponse)
                elif recivedRequest.receivedFrom == MessageType.MobilityHandoverAck:
                    # print("Mobility handover ACK received. Closing.")
                    shallServe = False # User served. Now exit this thread. A new user will open new thread.
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

                if messageType == 0: # Mobility hint received.
                    if configParameters.isDefaultMigrationEnabled == False:
                        backgroundThread.start()
                        mobilityState = State(456, MessageType.MobilityHint)
                        buffer = UtilityFunctions.ip2int(ip)
                        mobilityState.setMessage(buffer)
                        serverResponse = pickle.dumps(mobilityState)
                        serverResponse += bytearray(12) # Just to make size of every message equal to 137
                        sendData(conn, serverResponse)
                elif messageType == 1: # Mobility handover message received.
                    t1 = time.time()
                    mobilityState = State(456, MessageType.MobilityHandover)
                    buffer = UtilityFunctions.ip2int(ip)
                    mobilityState.setMessage(buffer)
                    serverResponse = pickle.dumps(mobilityState)
                    if configParameters.isDefaultMigrationEnabled == True:
                        status = extStore.Migrate(configParameters.remoteIp, configParameters.storePortNumber, keys)
                        # extStore.tracker.PrintKeys()                       
                        if status == True:
                            serverResponse += bytearray(12) # Just to make size of every message equal to 137
                            sendData(conn, serverResponse) 
                        else:
                            print("******* Failed to sync state. ********")
                    else:
                        # Send event to the backgrond sync that mobility handover is triggered.
                        extStore.event.set()
                        backgroundThread.join()
                        extStore.Close()

                        migrationTimeMs = (time.time() - t1) * 1000
                        # print("Blocking migration time = {0} milliseconds.".format(migrationTimeMs))
                        serverResponse += bytearray(12) # Just to make size of every message equal to 137
                        sendData(conn, serverResponse)
                        # extStore.tracker.PrintKeys()
                        isAppExitTimerStarted = True # Start App exit timer. This is just to make 
                        # sure that socket is not closed before sending the mobility handover message
                        # to the client device. Can have better mechanism to handle it.
                        appExitStartTime = time.time()
                        
            
                    migrationTimeMs = (time.time() - t1) * 1000
                    # print("Migration time = {0} milliseconds.".format(migrationTimeMs))
                    print(migrationTimeMs)

        if isAppExitTimerStarted == True:
            if (time.time() - appExitStartTime) >= 1: # Timeout 1000 ms
                print("App exit timeout. Closing.")
                shallServe = False

    # print("Client = {} disconnected".format(addr))


sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('', configParameters.portNumer))
sock.listen()
print("App server listening on: ", configParameters.localIp,":",configParameters.portNumer)


start_new_thread(mobilityHandler.Process, (mobilityQueue, mobilityHadlerExpectedDataLen,))

clientsServed = 1

# Keep listening to accpet new connections.
while True:

    conn, addr = sock.accept()

    # New client arrivted. Serve it in separte thread.
    start_new_thread(serve, (conn, addr, lock, mobilityQueue, mobilityHadlerExpectedDataLen, clientsServed, ))

    clientsServed += 1

    
            



