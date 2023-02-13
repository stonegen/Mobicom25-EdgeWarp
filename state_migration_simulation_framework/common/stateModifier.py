import redis
import string
import random
import datetime
import time

class StateModifier:
    def __init__(self, externalStore, dynamicVariables):
        self.externalStore = externalStore
        self.dynamicVariables = dynamicVariables
        self.counter = 1

    def isUpdateRequired(self, keyIndex):
        if keyIndex == 0 and self.counter % 10 == 0:
        # if keyIndex == 0 and self.counter % 500 == 0:
            return True
        elif keyIndex == 1 and self.counter % 25 == 0:
            return True
        elif keyIndex == 2 and self.counter % 50 == 0:
            return True
        elif keyIndex == 3 and self.counter % 60 == 0:
            return True
        elif keyIndex == 4 and self.counter % 70 == 0:
            return True
        elif keyIndex == 5 and self.counter % 80 == 0:
            return True
        else:
            return False

    # def isUpdateRequired(self, keyIndex):
    #     if keyIndex == 0 or keyIndex == 1:
    #         if self.counter % 5 == 0: # Do it every 5 messages
    #             return True
    #     elif keyIndex == 2 or keyIndex == 3:
    #         if self.counter % 10 == 0: # Do it every 10 messages
    #             return True
    #     elif keyIndex == 4 or keyIndex == 5:
    #         if self.counter % 50 == 0: # Do it every 50 messages
    #             return True
    #     return False # The rest of the keys are static.

    def Update(self, keys):
        
        for i in range(self.dynamicVariables):
            if self.isUpdateRequired(i):
                isKeyFound, state = self.externalStore.Get(keys[i])

                if isKeyFound:
                    stateList = list(state)
                    randomByte = random.randint(0, 255)
                    stateList[0] = randomByte
                    state = bytes(stateList)
                    self.externalStore.Set(keys[i], state)
        self.counter += 1


    