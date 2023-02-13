from ipaddress import ip_address
import json
from threading import local
import os
from types import SimpleNamespace
from configuration.configParameters import ConfigParameters


class Config:

    def __init__(self, fileName) -> None:
        self.fileName = fileName
        self.remoteFileName = "config.json"
        self.localIpAddress = "localhost"

    def loadConfig(self):
        with open(self.fileName, 'r') as f:
            data = json.load(f)
            return data

    def GetConfigParameters(self):
        data = self.loadConfig()
        paramters = ConfigParameters(data["localIp"], data["portNumer"], data["remoteIp"], \
        data["remotePortNumer"], data["storePortNumber"], data["storePassword"], \
        data["dynamicVariables"], data["totalVariables"], data["isDefaultMigrationEnabled"], data["storeType"])

        return paramters

    # def PrintParameters(self):
    #     print(self.paramters.localIp, self.paramters.portNumber, self.paramters.remoteIp, self.paramters.remotePortNumer, \
    #         self.paramters.storePortNumber, self.paramters.storePassword, self.paramters.dynamicVariables, \
    #             self.paramters.totalVariables, self.paramters.isDefaultMigrationEnabled)
        