import socket
import queue
from time import sleep
from common.messageType import MessageType


"""
EXPLAINATION :

    Constructor : Recieves the IP & Port Number and then binds to it in self.sock
    Process :  
        Synchronously accepts the sockets in the forever loop, and from the connection it waits for the type 
        of the mobility i.e hint or handover is recieved. Once handover, function process is ready to listen
        tp new connections.    

"""


class MobilityHandler:

    def __init__(self, serverIP, serverPort):
        self.ip = serverIP
        self.port = serverPort
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((serverIP, serverPort))
        self.sock.listen()
        # print("MEC Mobility handler started ...")

    def Process(self, dataQueue : queue, mobilityHadlerExpectedDataLen):
        while True:
            conn, addr = self.sock.accept()
            print("MEC mobility handler connected by: ", addr)

            keepServing = True

            while keepServing:
                data = conn.recv(mobilityHadlerExpectedDataLen)
                # print("Recieved Data ! \n")
                if len(data) >= mobilityHadlerExpectedDataLen:
                    messageType = data[4]
                        
                    print(f"mobility related message recieved : {messageType}")
                    if messageType == MessageType.MobilityHint.value : # Mobility hint
                        print("Mobility Hint Recieved! \n")
                        dataQueue.put(data)
                        
                        # ipBytes = data[0:4]
                        # print('.'.join(f'{c}' for c in ipBytes))
                        # print("Event type = ", data[4])
                    elif messageType == MessageType.MobilityHandover.value: # Mobility handover
                        print("Mobility Handover Recieved! \n")
                        dataQueue.put(data)
                        keepServing = False
                else :
                    print()
                sleep(0.005)
            
            conn.close()
            self.sock.close()
            break




        

                