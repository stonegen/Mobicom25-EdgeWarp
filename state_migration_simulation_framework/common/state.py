from common.messageType import MessageType

class State:
    msg = bytearray()
    receivedFrom = MessageType.Default
    def __init__(self, id, reqType):
        self.receivedFrom = reqType
        self.id = id

    def setMessage(self, message):
        self.msg = message

    def getMessage(self):
        return self.msg