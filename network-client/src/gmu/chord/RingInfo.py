'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.


Created on Aug 6, 2014

@author: dfleck
'''
from FingerEntry import FingerEntry
import copy, sets
from NodeLocation import NodeLocation


class RingInfo(object):
    '''
    This class holds specific information about a Chord ring. It allows a
    Chord node to actually be part of several rings
    '''


    def __init__(self, enclaveId, enclaveName, isBootstrapNode, enableAutoDiscovery):
        '''
        Constructor
        '''
        
        self.enclaveName = enclaveName
        self.enclaveId = enclaveId
        self.isBootstrapNode = isBootstrapNode
        self.enableAutoDiscovery = enableAutoDiscovery
        
        self.predecessorLocation = None
        self.bootstrapNodeLocations = [] # Save so we can re-bootstrap if needed
        self.successorList = None
        self.nodeIndex = 0
        
        
    def addBootstrapLocations(self, bootstrapNodeList):
        '''Take the incoming (Node IP, Node Ports) and build a list of node locations.'''
        if not bootstrapNodeList:
            return
        else:
            for nodeLoc in bootstrapNodeList:
                if nodeLoc not in self.bootstrapNodeLocations:
                    self.bootstrapNodeLocations.append(nodeLoc)
                
                
        


    def destroyFingerLocation(self, nodeLoc):
        ''' The nodeLoc passed in, is dead, so we need to go through finger table and kill it.'''
        for i in range(FingerEntry.m-1):
            if self.finger[i+1].nodeLocation == nodeLoc:
                # Replace with previous finger entry
                self.finger[i+1].nodeLocation = copy.deepcopy(self.finger[i].nodeLocation)
        
    
        