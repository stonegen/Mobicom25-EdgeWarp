from datetime import datetime
import imp
import time
import os


class logger:
    def __init__(self, t1, t2, isDefaultSchemeConfigured):
        handoverPredictionGap = t2-t1
        self.StartupDateTime = time.time()
        currentDateTime = datetime.now()

        folderPath = ""
        if isDefaultSchemeConfigured:
            folderPath = "results/default/"
        else:
            folderPath = "results/modified/"

        if (os.path.exists(folderPath) == False):
            os.makedirs(folderPath)


        self.DataFileName = folderPath + "RTT-microsec.csv"
        self.EventFileName = folderPath + "Events.csv"

        self.dataFile = open(self.DataFileName, "w")
        self.eventFile = open(self.EventFileName, "w")

        self.firstTimeCall = True

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