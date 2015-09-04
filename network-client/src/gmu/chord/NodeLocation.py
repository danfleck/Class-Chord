'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Mar 5, 2014

@author: dfleck
'''
from twisted.spread import pb

import traceback

class NodeLocation(object):
    '''
    classdocs
    '''

    '''Defines the node identifier and how to reach it.'''
    def __init__(self, myId = None, myIp=None, myPort=None):
        
        self.id = myId
        self.ip = myIp
        self.port = myPort
        
        if myPort is not None and not isinstance(myPort, int):
            raise Exception("Port in NodeLocation must be an int!")   
        
       
    def setValues(self, myId, myIp, myPort): 
        self.id = myId
        self.ip = myIp
        self.port = myPort   
        
             
    def __str__(self):
        # tcp:127.0.0.1:port=7081 
        # This is the required format by Twisted
        return "tcp:%s:port=%s" % (self.ip, self.port)
    
    def __repr__(self):
        return "NodeLocation:"+str(self)        
    
    def convertToObject(self):
        raise Exception("ConvertToObj not implemented!")
    
    def toDict(self):
        raise Exception("toDict not implemented!")
    
    def __eq__(self, other):
        if other is None or not isinstance(other, NodeLocation):
            return False
        else:
            v = other.id == self.id and other.ip == self.ip and other.port == self.port
            return v
              
    def __ne__(self, other):
        return not self.__eq__(other)
    
    
    
# Make sure the NodeLocation is copyable.
class CopyNodeLocation(NodeLocation, pb.Copyable, pb.RemoteCopy):
    pass

pb.setUnjellyableForClass(CopyNodeLocation, CopyNodeLocation)

