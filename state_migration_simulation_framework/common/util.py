import socket
import struct
import time
from datetime import datetime

class UtilityFunctions:

    IsMigrationCompleted = False

    def ip2int(addr):
        return struct.unpack("!I", socket.inet_aton(addr))[0]

    def int2ip(addr):
        return socket.inet_ntoa(struct.pack("!I", addr))

    def PrintMessage(message):
        messageWithTime = "Time: {}:  {}.".format(datetime.now().time(), message)
        print(messageWithTime)