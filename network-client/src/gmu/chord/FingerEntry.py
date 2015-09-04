''' Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Feb 24, 2014

@author: dfleck
'''
import math

class FingerEntry:
    '''Represents an entry in the finger table.
       Note: Finger indexes go from 0-->m-1 which is different than the 
       Chord paper which goes from 1-->m
    '''    
    m = 128 # Number of bits in entry set
    
    
    
    def __init__(self, k, n, nodeLocation):
        '''k is the finger table entry.
           n is the node ID of the node holding this entry
        '''
        #print("DEBUG: fingerINIT: %d  %d " % (k-1,n))
        twoToTheM = math.pow(2, FingerEntry.m)
        self.start = n + math.pow(2, k-1) % twoToTheM
        self.intervalStart = self.start
        self.intervalEnd =  n + math.pow(2, k) % twoToTheM

        
        self.nodeLocation = nodeLocation # This is the succ on the tables in the Chord paper
        
        
    def __str__(self):
        if self.nodeLocation is None:
            nodeId = -999
        else:
            nodeId = self.nodeLocation.id
            
        return "Start:%d  End:%d  NodeLocation:%d" % (self.start, self.intervalEnd, nodeId)        