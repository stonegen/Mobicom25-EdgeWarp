import enum
class MessageType(enum.Enum):
    Default = 1 ## Dummy messages with no use 
    Client = 2
    MobilityHint = 3
    MobilityHandover = 4
    MobilityHandoverAck = 5
    Ochtestrator = 6
