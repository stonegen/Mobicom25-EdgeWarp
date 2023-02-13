import socket
import sys
import time
from common.state import State
from common.messageType import MessageType
import pickle
import socket
import struct
import argparse
import ipaddress
from common.util import UtilityFunctions

parser = argparse.ArgumentParser()
parser.add_argument('--t1', type=float,
                        default=10, help='mobility to occur in t1 seconds')
parser.add_argument('--t2', type=float,
                        default=13, help='start app mobility at t2 seconds.')

args = parser.parse_args()

t1Passed = False
t2Passed = False

appStartTime = time.time()

HOST = 'localhost'  # The server's hostname or IP address
PORT = 10000        # The port used by the server

clientCounter = 0
serverCounter = 0

# message = struct.pack('fii', time.time(), 5, 6)
# clientState.setMessage(message)
# print("Testing   : ", clientState.getMessage())

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    while True:
        message = struct.pack('dii', time.time(), clientCounter, serverCounter)
        clientState = State(123, MessageType.Client)
        clientState.setMessage(message)
        serialized = pickle.dumps(clientState)
        s.sendall(serialized)


        # Received response
        serverResponse = s.recv(1024)
        clientState = pickle.loads(serverResponse)

        # Reponse is for client application
        if clientState.receivedFrom == MessageType.Client:
            message = struct.unpack('dii', clientState.getMessage())
            print("Time= {0} microseconds, Client = {1}, Server = {2}".\
            format((time.time() - message[0])*1000000, message[1], message[2]))

        # Response is for mobility handler
        elif clientState.receivedFrom == MessageType.MH_MobilityPredicted:
            print("Mobility prediction response.")
        elif clientState.receivedFrom == MessageType.MH_MobilityStarted:
            print("Mobility started response.")
            newHostIp = UtilityFunctions.int2ip(clientState.getMessage())
            print("App server response = ", newHostIp)
        
        
        time.sleep(1)
        clientCounter += 1

        currentTime = time.time() - appStartTime
        
        if t1Passed == False and currentTime >= args.t1:
            t1Passed = True

            # Tell server that mobility is to occur in few seconds
            print("T1 passed.")
            clientState = State(456, MessageType.MH_MobilityPredicted)
            serialized = pickle.dumps(clientState)
            s.sendall(serialized)
        if t2Passed == False and currentTime >= args.t2:
            t2Passed = True

            # Tell server to stop serving the user. Server will provide
            # IP of the destination server when state sync is done.
            print("T2 passed.")
            clientState = State(456, MessageType.MH_MobilityStarted)
            serialized = pickle.dumps(clientState)
            s.sendall(serialized)



