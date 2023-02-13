class ConfigParameters:
    def __init__(self, localIp, portNumer, remoteIp, remotePortNumer, storePortNumber, \
        storePasswrod, dynamicVariables, totalVariables, isDefaultMigrationEnabled, storeType):
        self.localIp = localIp
        self.portNumer = portNumer
        self.remoteIp = remoteIp
        self.remotePortNumer = remotePortNumer
        self.storePortNumber = storePortNumber
        self.storePasswrod = storePasswrod
        self.dynamicVariables = dynamicVariables
        self.totalVariables = totalVariables
        self.isDefaultMigrationEnabled = isDefaultMigrationEnabled
        self.storeType = storeType