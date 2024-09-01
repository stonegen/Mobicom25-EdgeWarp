from datetime import datetime
import time
import os
from configuration.config import Config



class logger:
    def __init__(self):

        self.StartupDateTime = time.time()

        configuration = Config("configuration/config.json")
        self.configParameters = configuration.GetConfigParameters() 


        ## Map of the Results Directory (Assuming No WorstCase set Up):
        ##  results 
        ##      --- EdgeCat (default Edge Application's )
        ##      --- Car Map
        ##      --- EMP

        dirPathPrefix = ""
        if (self.configParameters.WorstCase == 1):
            dirPathPrefix = "WorstCase/results"
        else :
            dirPathPrefix = "results"


        folderPath = ""
        if self.configParameters.isDefaultMigrationEnabled:
            folderPath = f"{dirPathPrefix}/{self.configParameters.appName}/default/"
        else:
            if(self.configParameters.asyncType == 0) :
                folderPath = f"{dirPathPrefix}/{self.configParameters.appName}/async-modified-noredis/"
            else :
                folderPath = f"{dirPathPrefix}/{self.configParameters.appName}/async-modified-redis/"

        if (os.path.exists(folderPath) == False):
            os.makedirs(folderPath)

        self.DataFileName = folderPath + self.createFileName() +"RTT-microsec.csv"
        self.EventFileName = folderPath + self.createFileName()+"Events.csv"

        self.responseFile = open(self.DataFileName, "a")
        self.eventFile = open(self.EventFileName, "a")


    def createFileName(self):
        """
            Based on the configuration at the time of the Experiment we will create a unique File Name following a certain format
        """

        filenamePrefix = f"{self.configParameters.asyncType}_{self.configParameters.clientRequestRate}_{self.configParameters.handoverTime}_{self.configParameters.hintTime}_{self.configParameters.minOldestUpdates}_{self.configParameters.maxOldestUpdates}_{self.configParameters.totalVariables}_{self.configParameters.dynamicVariables}_{self.configParameters.keySize}_"
        return filenamePrefix


    def LogData(self, rttMicroSeconds):
        currentTime = time.time()

        if self.firstTimeCall == True:
            self.StartupDateTime = currentTime
            self.firstTimeCall = False

        currentTickMicroSeconds = (currentTime - self.StartupDateTime)*1000000
        message = str(currentTickMicroSeconds) + "," + str(rttMicroSeconds) + "\n"
        self.dataFile.write(message)

    def LogEvent(self, evenType):
        currentTickMicroSeconds = (time.time() - self.StartupDateTime)*1000000
        message = str(currentTickMicroSeconds) + "," + str(evenType) + "\n"
        self.eventFile.write(message)

    def LogEventTimes(self , eventType : str , numberOfKeys:str, keySize:str , t1:float , t2:float):

        """
            Here Event Time Corresponds to the timne Region at which this recording was created : \n
            1) hint (after recieving hint)
            2) handover (when blocking migration is occuring)
        """
        eventTimeInMilli = (t2-t1)*1000
        message = eventType + "," + numberOfKeys + "," + keySize + "," + str(eventTimeInMilli) + "\n"
        self.eventFile.write(message)

    def LogResponseTimes(self , eventType : str , counterNumber:int, responseTime : float):
  
        """
            Here Event Time Corresponds to the timne Region at which this recording was created : \n
            1) normal (before hint)
            2) hint (after recieving hint)
            3) handover (when handover signal is generated , not going to have any results cuz application is on the shutdown)
        """
        message = eventType + "," + str(counterNumber) + "," + str(responseTime) + "\n"
        self.responseFile.write(message)
    
