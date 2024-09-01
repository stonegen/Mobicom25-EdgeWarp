from operator import ne
import time
import redis
from _thread import *
import threading
import sys
from threading import Lock
from common.statesManager import HashTable,DoublyLinkedList,Node
from configuration.configParameters import ConfigParameters
from configuration.config import Config

"""
    Explaination :
    This class calculates the updation frequency for each key when attached to it 
"""

   
class StateTracker:
   
    def __init__(self, lock):
        self.hashTable = HashTable()
        self.stateDict = {}
        self.lock = lock
        self.updateRateUpperLimit = 10
        
        
        configuration = Config("configuration/config.json")
        configParameters = configuration.GetConfigParameters()
        self.stateMethod = configParameters.stateMethod


    """
        Stores the History class for each key in the dictionary
    """
    def AddKeys(self, key, size):
        
        self.hashTable.add_node(key,size)


    """
        Updates the update time if key exists else creates a new History class , adds the counter regardless
    """
    def UpdateKey(self, key):
        try :
            self.hashTable.update_node(key , 1, stateMethod = self.stateMethod)
        except KeyError as e:
            print(f"{e} in UpdateKeys")
    
    
    def GetOldestUpdate(self , n : int , m : int):
        """
        Explaination : \n
            To find atleast n oldest UnSynced Keys and atmost m oldest UnSynced Keys : \n
            - From the tail of Unsync List we basically extract the keys one by one  
            - Our Unsync List based on the LFU or LRU mode will be sorted on the UnSync time
            - Oldest Updated key will have the smalled real UnSync Time (time at which key was updated)
            Returns : keys in list with single item oldest updated key 
        """    
        keys = []

        if (self.hashTable.unSyncList.length == 0 or self.hashTable.unSyncList.length < n):
            return keys

        if(self.hashTable.unSyncList.length < m):
            m = self.hashTable.unSyncList.length

        ptr = self.hashTable.unSyncList.tail
        for _ in range(0,m):

            if(ptr != None) :
                keys.append(ptr.key)           
                ptr = ptr.prev
            else :
                break            
        return keys


    def GetOutOfSyncKeys(self):

        """
            Explaination :
                Get all the keys that have been updated after their last sync time 
                This function is mainly called in the event of Redis HandOver only.
        """
        keys = []
        ptr = self.hashTable.unSyncList.tail
        ## For my personal Debugging
        # print("UNSYNC LIST SIZE IS : ", self.hashTable.unSyncList.length)
        while(ptr != None):

            if(ptr != None) :
                keys.append(ptr.key)           
                ptr = ptr.prev
            else :
                break            
        return keys 


    """
        Explaination :
            This function is mainly used in the case when we have updated the keys hence we will be shifting them in SyncList 
    """

    def moveMigratedKeys(self , keys , timeSync = None):

        try :
            for key in keys :
                self.hashTable.update_node(key,2,timeSync , stateMethod = self.stateMethod)
            return True
        except KeyError as e:
            print(f"{e} in moveMigrateKeys")
            return False
        


    def GetLatestUpdateTime(self):

        try:
            tempItem = list(self.stateDict.keys())[0]

            for key, Val in self.stateDict.items():
                if tempItem.updateTime < Val.updateTime:
                    tempItem.updateTime = Val.updateTime

        finally:
            pass
            
        return tempItem.updateTime

    
    # def Sort(self):

    def PrintKeys(self):
        
        try:
            unSyncList = self.hashTable.unSyncList
            temp = unSyncList.head
            print("__________")
            while (temp != None ):
                print(f"key : {temp.key} , Update Counter : {temp.history.updateCounter}" )
                temp = temp.next
            print("__________\n\n")
        finally:
            pass

    """
        Explaination :
            Checks the keys that whether they have been updated after their sync ?          
    """
    def AllSyncDone(self):
        try:
            for key, val in self.stateDict.items():
                if val.updateTime > val.syncTime:
                    return False
            return True

        finally:
            pass
