import redis
import string
import random
import datetime
import time



"""
Explaination :

    The following class is responsible to change the keys state in the datastore after certain counter
    values via the function : Update
"""

class StateModifier:
    def __init__(self, externalStore, dynamicVariables):
        self.externalStore = externalStore 
        self.dynamicVariables = dynamicVariables
        self.counter = 1

    def isUpdateRequired(self, keyIndex , totalKeys = None):

        if ((self.counter % (keyIndex + 5)) == 0):
            return True

     

    def Update(self, keys):
        for i in range(self.dynamicVariables):
                
            # if self.isUpdateRequired(i):
                isKeyFound, state = self.externalStore.Get(keys[i])

                if isKeyFound:
                    stateList = list(state)
                    randomByte = random.randint(0, 255)
                    stateList[0] = randomByte
                    state = bytes(stateList)
                    self.externalStore.Set(keys[i], state)
                else :
                    print("NOT FOUND!!!!")
        self.counter += 1




    