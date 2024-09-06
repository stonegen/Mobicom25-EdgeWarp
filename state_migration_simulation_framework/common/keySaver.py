# import imp
import redis
import string
import random
import datetime
import time
import pickle
from common.state import State
from common.redisStore import RedisStore
from common.memcacheStore import MemcacheStore
from common.messageType import MessageType
import socket
import sys
import os
import struct
from common.util import UtilityFunctions
from multiprocessing import Process, Lock
from configuration.config import Config


class KeySaver:
    def CreateKeys(self, extStore , totalVariables, keySize, numberOfClients, appName = None):
        

        keys = []

        for clientNumber in range(1, numberOfClients + 1 ):
            
            for i in range(1, totalVariables + 1):
                newKey = "user:" + str(clientNumber) + ":key" + str(i)
                keys.append(newKey)

        if(appName == "EdgeWarp_APP" or appName == None):
            self.defaultApp(keys, extStore, keySize)
        
        elif(appName == "CarMAP_APP" or appName == None):
            self.carMap(keys, extStore, keySize)
        
        elif(appName == "EMP_APP" or appName == None):
            self.mobileVr(keys, extStore, keySize)

        elif(appName == "EdgeWarp_APP2") :
            self.edgeApp2(keys , extStore , keySize)

        elif(appName == "onlineGAME_APP" or appName == None):
            self.onlineGame(keys, extStore, keySize)
        
        return keys


    def defaultApp(self, keys : list, extStore : RedisStore, K : int):
        """
            Explaination :
            This function will be generating the Keys from keys parameter of Size K. Since our evaluations
            will be mostly on Redis DataStore from simulation so we are passing RedisDataStore as external 
            store
        """
        
        print("Default app state stored.")
        counter = 1
        totalKeys = len(keys)
        for key in keys:
            
            print("Generating key {} having size = {}".format(key, K))
            value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = K))
            extStore.Set(key, value ,K)
            counter += 1


    def carMap(self, keys, extStore, K):


        ## Dynamic Variables : 2
        print("Car map state stored.")
        
        if (len(keys) == 11):

            for i in range(0,11):

                K = 1000

                if (i == len(keys) - 1 ):
                    
                    print("Generating key {} having size = {}".format(keys[i], 1800*K))
                    value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = (1800*K)))
                    extStore.Set(keys[i], value , (1800*K))       

                elif (i < 2) :

                    print("Generating key {} having size = {}".format(keys[i], 50*K))
                    value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = (50*K)))
                    extStore.Set(keys[i], value , (50*K))

                else :

                    print("Generating key {} having size = {}".format(keys[i], 50*K))
                    value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = (50*K)))
                    extStore.Set(keys[i], value , (50*K))


        ## Dynamic Variables : 4
        if (len(keys) == 6):

            for i in range(0,6):

                K = 1000

                if (i == len(keys) - 1 ):
                    
                    print("Generating key {} having size = {}".format(keys[i], 1800*K))
                    value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = (1800*K)))
                    extStore.Set(keys[i], value , (1800*K))       

                elif (i == len(keys) - 2) :

                    print("Generating key {} having size = {}".format(keys[i], 500*K))
                    value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = (500*K)))
                    extStore.Set(keys[i], value , (50*K))

                else :

                    print("Generating key {} having size = {}".format(keys[i], 500*K))
                    value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = (500*K)))
                    extStore.Set(keys[i], value , (50*K))

                
        ## Dynamic Variables : 6s
        if (len(keys) == 10):

            for i in range(0,10):

                K = 1000

                if (i == len(keys) - 1 ):
                    
                    print("Generating key {} having size = {}".format(keys[i], 1800*K))
                    value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = (1800*K)))
                    extStore.Set(keys[i], value , (1800*K))       

                elif (i < 2) :

                    print("Generating key {} having size = {}".format(keys[i], 50*K))
                    value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = (50*K)))
                    extStore.Set(keys[i], value , (50*K))

                else :

                    print("Generating key {} having size = {}".format(keys[i], 500*K))
                    value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = (500*K)))
                    extStore.Set(keys[i], value , (50*K))

      
    
    def mobileVr(self, keys, extStore, K):
        print("Mobile VR(EMP) state stored.")
       
        """
            Things to note about the EMP or Mobile VR :
            1) each image frame is stored of equal size 
            2) upon observing the Application we realised that image frames were mostly around 32 KB
            3) For the table 5 by varying the number of keys we got 15,23,31 that will be used for observing reduction in
                blocking migration Time In Sha Allah
        """
        K = 32 
        multiplier = 1024
        for key in keys :

            print("Generating key {} having size = {}".format(key, multiplier*K))
            value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = multiplier*K))
            extStore.Set(key, value , (multiplier*K))
            

    def edgeApp2(self, keys, extStore, K):

        """
            This app is created so that we can have some diversity in the key sizes dimensions for including
            randomness to observe the blocking migration across multiple dimensions
        """
        print("Default app2 state stored.")
        K = 1024
        counter = 1
        totalKeys = len(keys)

        for key in keys:
            # For first one thrid keys, keep this multiplier which makes the value 5KB
            multiplier = 5

            # For second set of 1/3rd keys, the value size is 50KB
            if counter > totalKeys/3 and counter <= 2*totalKeys/3:
                multiplier = 50
            # For third set of 1/3rd keys, the value size is 1MB
            elif counter > 2*totalKeys/3:
                # multiplier = 1024
                multiplier = 1024

            print("Generating key {} having size = {}".format(key, multiplier*K))
            value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = multiplier*K))
            extStore.Set(key, value , (multiplier*K))
            counter += 1


    def onlineGame(self, keys, extStore, K):
        print("Online game state stored.")

        counter = 1
        totalKeys = len(keys)

        for key in keys:
            # For first one thrid keys, keep this multiplier which makes the value 5KB
            multiplier = 5

            if (counter == 1):
                multiplier = 10 
            elif counter == 2:
                 multiplier = 20
            elif counter == 3:
                 multiplier = 30
            elif counter == 4:
                 multiplier = 500
            elif counter == 5:
                 multiplier = 600
            elif counter == 6:
                 multiplier = 800
            elif counter == 7:
                 multiplier = 900

            print("Generating key {} having size = {}".format(key, multiplier*K))
            value = ''.join(random.choices(string.ascii_uppercase + string.digits, k = multiplier*K))
            extStore.Set(key, value)

            counter += 1