import socket
import sys
import time
from common.state import State
import pickle
import socket

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 10000        # The port used by the server

clientState = State(123)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    while True:
        clientState.clientCounter += 1
        clientState.startTime = time.time()
        serialized = pickle.dumps(clientState)
        s.sendall(serialized)
        serverResponse = s.recv(1024)
        # print(serverResponse)
        clientState = pickle.loads(serverResponse)
        print("Time= {0} microseconds, Value = {1}.".\
            format((time.time() - clientState.startTime)*1000000, clientState.serverCounter))
        time.sleep(1)
