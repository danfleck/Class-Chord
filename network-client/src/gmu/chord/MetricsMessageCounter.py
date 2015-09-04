'''
This module is used really by the metrics code, but it is part of the ChordNode class requirements.
It simply counts the total number of sent and recv messages and some other statistics.

Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Nov 3, 2014

@author: dfleck
'''
import glog

class MetricsMessageCounter(object):
    '''
    This module is used really by the metrics code, but it is part of the ChordNode class requirements.
    It simply counts the total number of sent and recv messages and some other statistics.
    '''


    def __init__(self, node):
        '''
        Constructor
        '''
        self.node = node
        self.resetAll()
        
    def resetAll(self):
        self.numRecv = 0 # Num Messages recv
        self.numSent = 0 # Num Messages sent
        
        self.numOutgoingConnections = 0 # How many nodes did we connect to (basically, how many times did we call
                                        # out and try to connect to another node. This counts attempts, not successful connections.
        
        # How many of each lookup type have we done
        self.longLookups = 0
        self.shortLookups = 0
        
        # Dictionary of class messages for each test.
        self.classMsgs = dict()
        
        
    def madeOutgoingConnection(self):
        self.numOutgoingConnections += 1
        
    def sentMsg(self, msg, envelope):
        self.numSent += 1
        
    def recvMsg(self, msg, envelope):
        self.numRecv += 1
        
        
    def longLookup(self):
        self.longLookups += 1

    def shortLookup(self):
        self.shortLookups += 1        
        
    def classMessageReceived(self, msg, inMyClass):
        testNum = msg['testNum']
        
        if testNum not in self.classMsgs:
            # Initialize it
            self.classMsgs[testNum] = [0,0] # Good, Wasted
            
        if inMyClass:
            self.classMsgs[testNum][0] += 1
            glog.debug("%s got msg IN CLASS" % self.node.nodeLocation, system="MetricsMessageCounter")
        else:
            self.classMsgs[testNum][1] += 1
            glog.debug("%s got msg NOT IN CLASS" % self.node.nodeLocation, system="MetricsMessageCounter")
            