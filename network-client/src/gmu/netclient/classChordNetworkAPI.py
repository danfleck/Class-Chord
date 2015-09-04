'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.


Created on Jun 2, 2014

@author: dfleck
'''

import datetime
from gmu.netclient.classChordClientAPI import classChordClientAPI

class classChordNetworkObj(object):
    '''
    This is a base class for all classChordNetworkImplementations. Concrete implementations
    need to subclass this and implement all the methods!
    '''

    def generateNodeID(self, aString, enclaveStr='localhost'):
        '''
        Generates a node ID given a random String and enclaveStr. The assumption is 'aString'
        is already globally unique (UUID) and we'll add on a prefix for whatever use we need.
        
        We'll then return total ID to the higher layer and it'll use it.
        
        enclaveStr is the enclave name that will be included in the ID.
        
        http://docs.python.org/2/library/uuid.html
        http://stackoverflow.com/questions/2461141/get-a-unique-computer-id-in-python-on-windows-and-linux

        OS Platform System:
        https://github.com/hpcugent/easybuild/wiki/OS_flavor_name_version
        '''
        raise NotImplementedError()
    
    
    def start(self, statusCallback, nodeID, enclaveStr, authenticationPayload,bootstrapNodeList, isBootstrapNode, enableAutoDiscovery=True):
        
        '''Startup the network interface.
        
           The status callback will be called once the join is complete.
           It will provide a parameter True-->join was successful, False-->join failed.
           
           nodeID is the unique ID of this node. It will be used and cannot be blank.
           enclaveStr is the enclave you want this node to join. 
               if we are the bootstrap node
                   it will default to localhost if not provided
               else
                   if enclave is blank:
                       it will ask the bootstrap node for it's enclave which WILL FAIL if the bootstrap node 
                       is part of more than 1 enclave.
                   else not blank: 
                       it will check that the enclave you want to join is also part part of the
                       bootstrap node, and fail if it's not
            
            
            bootstrapNodeList can be either
                1. a list of NodeLocation objects e.g. bsList = [ NodeLocation1, NodeLocation2, etc...]
                2. a list of (IP,Port) tuples e.g. bsList = [ (ip1, port1), (ip2, port2), ...]
            
            authenticationPayload - this is a payload that will be passed to the 
                node as you try to join it to authenticate. It gets sent to classChordClientApi.authenticateJoin(payload)                         
        '''
        raise NotImplementedError()
    
    def joinEnclave(self, bootstrapNodeList, isBootstrapNode, authenticationPayload, enclaveStr=None, statusCallback=None, enableAutoDiscovery=True):
        '''
        Join another enclave. This assumes you're currently listening for connections and have joined
            a previous Chord ring.
            
        If you are creating a Ring (so you'll be the bootstrap node) you can set 
            bootstrapNodeList to None, but then you MUST specific the enclaveStr
            and isBootstrapNode = True
            
        If you are connecting to an existing enclave you must specify the bootstrap IP/Port, but
            can leave the enclaveStr blank and we'll ask the bootstrapNode for it.
            However, if the bootstrapNode is part of multiple enclaves, then the code will return
            an error code and then you MUST specify the enclaveStr so the bootstrapNode knows which
            enclave you are trying to join.
            
        authenticationPayload - this is a payload that will be passed to the 
                node as you try to join it to authenticate. It gets sent to classChordClientApi.authenticateJoin(payload)                         

        bootstrapNodeList can be either
            1. a list of NodeLocation objects e.g. bsList = [ NodeLocation1, NodeLocation2, etc...]
            2. a list of (IP,Port) tuples e.g. bsList = [ (ip1, port1), (ip2, port2), ...]            
        '''
        raise NotImplementedError()

        
        
    def isConnected(self, enclave='ANY'):
        '''Check if the client is connected to the network.
           Note: this is an ACTIVE test which should try and 
           connect to the server. Don't call it super-often because it 
           does generate network call/response.
           
           If a specific enclave is passed in, it checks connectivity to that enclave.
           otherwise it just checks to ANY other nodes.
        '''
        raise NotImplementedError()
    
    
    def addNetworkObserver(self, methodToBeCalled):
        '''Call the observer method with network statuses'''
        '''methodToBeCalled will be called with a network status value as:
           methodToBeCalled( incomingConnectionMade, outgoingConnectionMade)
           where values are True | False
        ''' 
        raise NotImplementedError()

    def removeNetworkObserver(self, obs):
        raise NotImplementedError()
            
    
    def sendMessage(self, message, messageType, statusCallback, destination=None, ttl=None, enclave='localhost', aggregationNum=0):
        '''
        Sends a message to the given destination
        
        message = message to send. This should be an encrypted string
        messageType = valid values are "class", "flood", or "p2p" (point to point), "agg-response" (aggregate responses)
        statusCallback = method to call upon send success/failure
        destination = destination for the message (this should be a node ID)
                    p2p: nodeID of place you want this message to end up.
                    flood: un-used. leave as None
                    agg-response: nodeID of place you want this message to end up (it's FINAL destination...probably the mgmt node).
        enclave = p2p: enclave of node to send to, blank or ALL and the code will figure it out.
                  flood: None-->localhost, ALL --> all enclaves you know about, <another string> --> to the enclave specified
                  agg-response: 
        ttl = [optional] defaults to 10 mins from now
        
        aggregationNum: p2p, flood, class: Unused
                         agg-response: the number of nodes to aggregate over.
                         
        
        Note: For flooding messages, we run the callback after the initial send, thus True does NOT
                mean that the message reached every node. If you want that, lets discuss :-)

        '''
        raise NotImplementedError()    
    
    def disconnect(self):
        '''Disconnect from the network.'''
        raise NotImplementedError()
    
    def getCurrentNodeLocation(self):
        '''This is mostly for debugging or display. Used to get the current NodeLocation
           for the node being managed by ths Obj.
        '''
        raise NotImplementedError()    

    def getCurrentNodeEnclaves(self):
        '''This is mostly for debugging or display. Used to get the current Node's enclaves joined.           
        '''
        raise NotImplementedError()    


