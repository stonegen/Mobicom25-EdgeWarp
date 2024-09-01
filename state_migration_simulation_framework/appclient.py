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
from configuration.config import Config
import random


## Extracting the Configuration to Run Client Appropriately :
configuration = Config("configuration/config.json")
configParameters = configuration.GetConfigParameters()


t1Passed = False
t2Passed = False

appStartTime = time.time()

HOST = configParameters.localIp   # The server's hostname or IP address
PORT = configParameters.portNumer # The port used by the server


MOBILITY_HOST = configParameters.localIp
MOBILITY_PORT = configParameters.mobilityPort

MOBILITY_HINT_TIME = configParameters.hintTime
MOBILITY_HANDOVER_TIME = configParameters.handoverTime 

MESSAGE_INTERVAL = (1/configParameters.clientRequestRate) ## The time interval between two updates in the code. One Update can change more \
                                                            ## than one key values

## Before Starting Code we would introduce a slight random delay in milliseconds to have randomness


# Generate a random integer between 0 and 100
random_number = random.randint(0, 100)
time.sleep((random_number/100))


clientCounter = 0
serverCounter = 0


print(" Mobility HINT will occur at : ", MOBILITY_HINT_TIME , "Mobility HANDOVER will occur at is : ", MOBILITY_HANDOVER_TIME , " seconds")


def connect_and_send(sock, state):
    """
    The following function is only there for client to send mobility-related messages to the server
    """
    try:
        sock.sendall(state)
        print(f"Sent message: {state}")
    except Exception as e:
        print(f"Failed to send message: {e}")


print("Message interval : ", MESSAGE_INTERVAL)
# Opening a persistent connection to the mobility server
mobility_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
mobility_sock.connect((MOBILITY_HOST, MOBILITY_PORT))

updateSendMessage = 0

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    while True:
        
        t1 = time.time()
        message = struct.pack('dii', time.time(), clientCounter, serverCounter)
        if( t2Passed != True and ((time.time() - (updateSendMessage + MESSAGE_INTERVAL) ) > 0)):
        # if( t2Passed != True ):
            clientState = State(123, MessageType.Client)
        else :
            clientState = State(123, MessageType.Default)
        clientState.setMessage(message)
        serialized = pickle.dumps(clientState)
        s.sendall(serialized)


        # Received response
        serverResponse = s.recv(1024)
        clientState = pickle.loads(serverResponse)

        # Reponse is for client application
        if clientState.receivedFrom == MessageType.Client:
            t2 = time.time()
            updateSendMessage = t2
            message = struct.unpack('dii', clientState.getMessage())
            # print(f"Response Time = {(t2-t1) * 1000000 } microseconds")

        # Response is for mobility handler
        elif clientState.receivedFrom == MessageType.MobilityHint:
            print("Mobility prediction response.")
        elif clientState.receivedFrom == MessageType.MobilityHandover:
            print("Mobility started response.")
            print("Exiting Server Bye !")
            break
        
        clientCounter += 1

        currentTime = time.time() - appStartTime
        # print("Current time is : ", currentTime)
        
        if t1Passed == False and currentTime >= MOBILITY_HINT_TIME:
            t1Passed = True

            print("T1 passed.")
            ip_bytes = socket.inet_aton(HOST)
            message = struct.pack('!4sB', ip_bytes, MessageType.MobilityHint.value)
            connect_and_send(mobility_sock, message)


        if t2Passed == False and currentTime >= MOBILITY_HANDOVER_TIME:
            t2Passed = True
            print("T2 passed. Waiting for the Handover's reply to exit")

            """
               For now evaluations will be performed 
                on single client with single Base Station & Edge Server
                - Yes in here client is giving the hand over and hint related message. Obviously it's just simulations for 
                our APIs to see their efficiency. In Paper we have our Core responsible for these messages In Sha Allah
            """
            ip_bytes = socket.inet_aton(HOST)
            message = struct.pack('!4sB', ip_bytes, MessageType.MobilityHandover.value)
            connect_and_send(mobility_sock, message)
        
        








