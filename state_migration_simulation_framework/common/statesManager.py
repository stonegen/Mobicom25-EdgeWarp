import time

"""
    The classes here are created to allow for fast state managing and extracting the relevant keys for migration in minimum time possible,
    All operations here are of time-Complexity O(1) except for the case of LFU Mode, here insertion/updation to Unsync List will cost us O(n)
"""


class History:
    def __init__(self, updateTime, key):
        self.updateTime = updateTime
        self.syncTime = 0
        self.key = key
        self.updateCounter = 0
        self.syncCounter = 0
        self.creationTime = time.time()
        self.keySize = 0
        self.updateFrequency = 0

    def Fequency(self):
        frequency = 0

        # Wait for at least three updates to calculate frequency
        if self.updateCounter >= 3:
            duration = time.time() - self.creationTime
            frequency = self.updateCounter / duration
            self.updateFrequency = frequency

        return frequency




class Node:
    def __init__(self, key):
        self.key = key
        self.history = History(time.time(), key)
        self.prev = None
        self.next = None

class DoublyLinkedList:
    def __init__(self):
        self.head = None
        self.tail = None
        self.length = 0

    def add_to_head(self, node : Node):
        node.next = self.head
        node.prev = None
        if self.head is not None:
            self.head.prev = node
        self.head = node
        if self.tail is None:
            self.tail = node
        self.length += 1

    def remove(self, node):
        if node.prev:
            node.prev.next = node.next
        else:
            self.head = node.next
        if node.next:
            node.next.prev = node.prev
        else:
            self.tail = node.prev
        node.next = node.prev = None
        self.length -= 1
        

    def move_to_head(self, node):
        self.remove(node)
        self.add_to_head(node)

    def add_based_updateCounter(self, node: Node):

        
        if self.head is None:
            self.head = self.tail = node
            self.length += 1
            return

        if node.history.updateCounter >= self.head.history.updateCounter:
            self.add_to_head(node)
        else:
            temp = self.head.next
            while temp is not None and node.history.updateCounter < temp.history.updateCounter:        
                temp = temp.next
        
            if temp is None: 
                node.prev = self.tail
                self.tail.next = node
                self.tail = node
            else:  
                node.next = temp
                node.prev = temp.prev
                if temp.prev:
                    temp.prev.next = node
                temp.prev = node

            self.length += 1
        
       
    def verify_no_cycle(self):
        # A simple method to check if our dubly list has formed a cycle
        slow, fast = self.head, self.head
        while fast and fast.next:
            slow = slow.next
            fast = fast.next.next
            if slow == fast:
                raise Exception(f"Cycle detected in the list! : {self.length}")


class HashTable:
    def __init__(self):
        self.table = {}
        self.unSyncList = DoublyLinkedList()
        self.syncList = DoublyLinkedList()

    def checkEligibility(self , node : Node , updateTime , destinationListType):

        if destinationListType == 1 :
            if(node.history.syncTime < updateTime):
                return True 
        else :
            if(node.history.updateTime > updateTime):
                return True             
        return False


    def add_node(self, key, size ,list_type=1):
        """
            key would be the string name , key type would be : \n
            1) 1 for unSynced Keys list
            2) 2 for Synced Keys
        """
        node = Node(key)
        node.history.keySize = size
        if list_type == 1:
            self.unSyncList.add_to_head(node)
        else:
            self.syncList.add_to_head(node)
        self.table[key] = (node, list_type)





    def update_node(self, key , destinationListType = None , updateTime = None , stateMethod = None):
        decsion = None
    
        if key in self.table:
            node , list_type = self.table[key]
            if updateTime != None :
                decsion = self.checkEligibility(node , updateTime , destinationListType)
            if destinationListType == 1 :

                node.history.updateCounter += 1
                if updateTime == None :
                    node.history.updateTime = time.time()    
                else :
                    if decsion :
                        node.history.updateTime = updateTime
          
            else :
                if updateTime == None :
                    node.history.syncTime = time.time()
                    node.history.syncCounter += 1
                else :
                    if decsion :
                        node.history.syncTime = updateTime
                        node.history.syncCounter += 1


            if( destinationListType != None and destinationListType == list_type and ((decsion == None or decsion == True) or list_type == 1)):

                if list_type == 1:
                    if(stateMethod == "LFU"):
                        self.unSyncList.remove(node)
                        self.unSyncList.add_based_updateCounter(node)
                        self.table[key] = (node,1)
                    else :    
                        self.unSyncList.move_to_head(node)
                else:
                    self.syncList.move_to_head(node)
            
            else :
                if list_type == 1:  
                    self.unSyncList.remove(node)
                    self.syncList.add_to_head(node)
                    self.table[key] = (node, 2)
                else:
                    self.syncList.remove(node)
                    if(stateMethod == "LFU"):
                        self.unSyncList.add_based_updateCounter(node)
                    else :
                        self.unSyncList.add_to_head(node)
                    self.table[key] = (node, 1)
            
        else:
            raise KeyError(f'Key {key} not found in hash table.')

    def move_node_to_other_list(self, key , destinationListType = None):

        if key in self.table:
            node, list_type = self.table[key]

            if( destinationListType != None and destinationListType == list_type):

                if list_type == 1 :
                    self.unSyncList.remove(node)
                    self.unSyncList.add_to_head(node)
                else :
                    self.syncList.remove(node)
                    self.syncList.add_to_head(node) 
                return                    

            if list_type == 1:
                self.unSyncList.remove(node)
                self.syncList.add_to_head(node)
                self.table[key] = (node, 2)
            else:
                self.syncList.remove(node)
                self.unSyncList.add_to_head(node)
                self.table[key] = (node, 1)
        else:
            raise KeyError(f'Key {key} not found in hash table.')