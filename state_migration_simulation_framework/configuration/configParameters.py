class ConfigParameters:
    def __init__(self, localIp, portNumer, mobilityPort,remoteIp, remotePortNumer, storePortNumber, \
        storePasswrod, dynamicVariables, totalVariables, keySize, isDefaultMigrationEnabled, asyncType ,storeType, hintTime , handoverTime, numberOfClients, clientUpdateRate, minOldestUpdates, maxOldestUpdates,stateMethod, appName , WorstCase):
        self.localIp = localIp
        self.portNumer = portNumer
        self.mobilityPort = mobilityPort
        self.remoteIp = remoteIp
        self.remotePortNumer = remotePortNumer
        self.storePortNumber = storePortNumber
        self.storePasswrod = storePasswrod
        self.dynamicVariables = dynamicVariables
        self.totalVariables = totalVariables
        self.keySize = keySize
        self.isDefaultMigrationEnabled = isDefaultMigrationEnabled
        self.asyncType = asyncType
        self.storeType = storeType
        self.hintTime = hintTime
        self.handoverTime = handoverTime
        self.numberOfClients = numberOfClients
        self.clientRequestRate = clientUpdateRate
        self.minOldestUpdates = minOldestUpdates
        self.maxOldestUpdates = maxOldestUpdates
        self.stateMethod = stateMethod
        self.appName = appName
        self.WorstCase = WorstCase

