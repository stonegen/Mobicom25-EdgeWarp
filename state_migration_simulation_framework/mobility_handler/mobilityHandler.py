import socket
import queue
from time import sleep

class MobilityHandler:

    def __init__(self, serverIP, serverPort):
        self.ip = serverIP
        self.port = serverPort
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((serverIP, serverPort))
        self.sock.listen()
        # print("MEC Mobility handler started ...")

    def Process(self, dataQueue, mobilityHadlerExpectedDataLen):
        while True:
            conn, addr = self.sock.accept()
            # print("MEC mobility handler connected by: ", addr)

            keepServing = True

            while keepServing:
                data = conn.recv(mobilityHadlerExpectedDataLen)

                if len(data) >= mobilityHadlerExpectedDataLen:
                    messageType = data[4]

                    if messageType == 0: # Mobility hint
                        dataQueue.put(data)
                        
                        # ipBytes = data[0:4]
                        # print('.'.join(f'{c}' for c in ipBytes))
                        # print("Event type = ", data[4])
                    elif messageType == 1: # Mobility handover
                        dataQueue.put(data)
                        keepServing = False
                sleep(0.005)




        

                