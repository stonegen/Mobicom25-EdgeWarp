import enum
class MessageType(enum.Enum):
    Default = 1
    Client = 2
    MobilityHint = 3
    MobilityHandover = 4
    MobilityHandoverAck = 5
    Ochtestrator = 6
