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
        self.localIpAddress = "203.135.63.29" ## ignore these details, everything now is on the configuration File

    def loadConfig(self):
        with open(self.fileName, 'r') as f:
            data = json.load(f)
            return data

    def GetConfigParameters(self):
        data = self.loadConfig()
        paramters = ConfigParameters(data["localIp"], data["portNumer"], data["mobilityPort"],data["remoteIp"], \
        data["remotePortNumer"], data["storePortNumber"], data["storePassword"], \
        data["dynamicVariables"], data["totalVariables"], data["keySize"], data["isDefaultMigrationEnabled"], data["asyncType"] , data["storeType"], data["hintTime"], data["handoverTime"], data["numberOfClients"], data["clientUpdateRate"] , data["minOldestUpdates"] , data["maxOldestUpdates"], data["stateMethod"], data["appName"], data["WorstCase"])

        return paramters
    
    def setConfigParameters(self, newParameters):
        data = self.loadConfig()
        data.update({
            "localIp": newParameters.localIp,
            "portNumer": newParameters.portNumer,
            "mobilityPort": newParameters.mobilityPort,
            "remoteIp": newParameters.remoteIp,
            "remotePortNumer": newParameters.remotePortNumer,
            "storePortNumber": newParameters.storePortNumber,
            "storePassword": newParameters.storePasswrod,
            "dynamicVariables": newParameters.dynamicVariables,
            "totalVariables": newParameters.totalVariables,
            "keySize": newParameters.keySize,
            "isDefaultMigrationEnabled": newParameters.isDefaultMigrationEnabled,
            "asyncType": newParameters.asyncType,
            "storeType": newParameters.storeType,
            "hintTime": newParameters.hintTime,
            "handoverTime": newParameters.handoverTime,
            "numberOfClients": newParameters.numberOfClients,
            "clientUpdateRate": newParameters.clientRequestRate,
            "minOldestUpdates": newParameters.minOldestUpdates,
            "maxOldestUpdates": newParameters.maxOldestUpdates,
            "stateMethod": newParameters.stateMethod,
            "appName": newParameters.appName,
            "WorstCase": newParameters.WorstCase
        })
        self.saveConfig(data)

    def saveConfig(self, data):
        with open(self.fileName, 'w') as f:
            json.dump(data, f, indent=4)