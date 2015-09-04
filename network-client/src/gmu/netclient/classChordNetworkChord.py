'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This code is the network API subclass for the Chord network backend.

Created on Mar 24, 2014

@author: shiremag
'''

import random
import threading
import socket
from twisted.internet import defer
from twisted.python import log

from gmu.chord.ChordNode import ChordNode
from gmu.chord.GmuServerFactory import GmuServerFactory
from gmu.chord.NodeLocation import NodeLocation


from gmu.netclient.envelope import Envelope
from gmu.chord.CopyEnvelope import CopyEnvelope

from gmu.netclient.classChordNetworkAPI import classChordNetworkObj
import datetime
from gmu.chord import NetworkUtils
from gmu.chord import Utils



class classChordNetworkChord(classChordNetworkObj):
    '''
    ClassChord Node Class
    Provides one interfaces to the layer above: send()
    Calls the processMessage() function in the layer above upon receiving a Message.
    '''
    
    def __init__(self, classChordClientObj, myPort=None, myIP=None):
        '''
        myPort and IP are the port and IP I should run on. If left blank,
        they will be generated.                 
        '''
        self.myPort=myPort
        self.myIP=myIP
        
        #Reference/Handle to the layer above
        self.classChordClientObj = classChordClientObj
        
        #Reference/Handle to the P2P Layer below
        self.chordNode = None
        
        self.serverFactory = None         
    
        
    def disconnect(self):
        '''Leave the network. Stop listening and go!'''
        return self.chordNode.leave()
        
    
    @defer.inlineCallbacks
    def isConnected(self, enclaveName='ANY'):
        '''Returns True | False if the node is connected to the enclave.'''
        
        
        # Get the enclaveID for the name
        if enclaveName == 'ANY':
            # Loop through all
            rc = yield self.chordNode.isConnectedToAnyEnclave()
            defer.returnValue(rc)
        else:
            # Get the enclaveID
            enclaveId = Utils.getEnclaveIDFromString(enclaveName)  
            (madeOutgoingConnection, madeIncomingConnection) =  yield self.chordNode.remote_isConnected(None, enclaveId, True)                             
            defer.returnValue(madeOutgoingConnection and madeIncomingConnection)
    

    def sendMessage(self, message, messageType, statusCallback, destination=None, ttl=None, enclave='localhost', aggregationNum=0, classQuery='0000'):
        '''
        Sends a message from the Client layer above to the P2P layer for forwarding
        message to next hop/destination
        '''
        # TODO: Encrypt the messages and envelope BEFORE sending
        
        if ttl is None:
            ttl = datetime.datetime.now() + datetime.timedelta(minutes=10)
           
        # Build the envelope
        env = CopyEnvelope()
        env['ttl'] = ttl
        env['source'] = self.chordNode.nodeLocation
        env['type'] = messageType
        env['destination'] = destination
        env['msgID'] = random.randint(1, 99999999) # TODO: Something better here!
        env['enclave'] = enclave # Make the node find it!        
        env['aggregationNumber'] = aggregationNum
        env['classQuery'] = classQuery # TODO: default for now 
        
        if messageType == 'flood':
            if destination != None:
                log.msg("WARNING: flooding message destination is ignored. Don't set that.")
            retStatus = self.chordNode.sendFloodingMessage(message, env)
            retStatus.addBoth(statusCallback)
        elif messageType == 'classType':
            retStatus = self.chordNode.sendClassMessage(message, env)
            retStatus.addBoth(statusCallback)            
        elif messageType == 'p2p':
            retStatus = self.chordNode.sendSyncMessage(message, env)
            retStatus.addBoth(statusCallback)
        elif messageType == 'agg-response':
            # Do a bit of validation
            if aggregationNum == 0:
                raise Exception("Cannot send agg-response with 0 nodes to aggregate over.")
            retStatus = self.chordNode.sendAggregationResponseMessageWithStatus(message, env)
            retStatus.addBoth(statusCallback)            
        else:
            raise Exception("sendMessage: Unknown type %s"  % messageType)
        
                
    def processMessage(self, currentMessage, envelope):
        '''
        This function processes an individual message
        
        Process individual Message -- by the time we get here, it's already 
        been validated and decrypted and sent on if required.
        
        '''

        #Is this message for me?
        if envelope['type'] == 'flood' or envelope['type'] == 'classType' or envelope['destination'] == self.chordNode.nodeLocation.id:
            self.classChordClientObj.receiveMessage(currentMessage, envelope)
            return

        
        
             
    def start(self, statusCallback, nodeID, enclaveToJoin, authenticationPayload, bootstrapNodeList, isBootstrapNode, enableAutoDiscovery=True):
        '''
        Start ClassChord Network Services
        The main driver of the Network API
        
        bootstrapNodeList is a list of (IP, Port) pairs for bootstrap nodes [ (ip,port), (ip, port), ...]
        isBootstrapNode = True | False if this node should be considered a bootstrap node in the given enclave
        enableAutoDiscovery = True | False. Try to use autoDiscovery if no one on the BootstrapNodeList is reachable.
        
        myPort and IP are the port and IP I should run on. If left blank,
        they will be generated. 
        
        '''
        log.msg("ClassChord API| Starting ClassChord Network Services")

        # Just in case, convert to node locations
        bootstrapNodeList = self.convertToNodeLocations(bootstrapNodeList)

        # Startup Chord once the reactor is running
        from twisted.internet import reactor
        
        log.msg("ClassChord API| Calling reactor")
        reactor.callWhenRunning(self.chordStart,  bootstrapNodeList, isBootstrapNode, enableAutoDiscovery, 
                                 self.myPort, self.myIP, statusCallback, enclaveToJoin, nodeID, authenticationPayload)




    def joinEnclave(self, bootstrapNodeList, isBootstrapNode, authenticationPayload, enclaveStr=None, statusCallback=None,  enableAutoDiscovery=True):
        '''
        Join another enclave. This assumes you're currently listening for connections and have joined
            a previous Chord ring.
            
        If you are creating a Ring (so you'll be the bootstrap node) you can set 
            bootstrapNodeList to None, but then you MUST specific the enclaveStr
            
        If you are connecting to an existing enclave you must specify the bootstrapNodeList, but
            can leave the enclaveStr blank (None) and we'll ask the bootstrapNode for it.
            However, if the bootstrapNode is part of multiple enclaves, then the code will return
            an error code and then you MUST specify the enclaveStr so the bootstrapNode knows which
            enclave you are trying to join.
        '''

        # Check that we have a ChordNode already
        if self.chordNode is None:
            raise Exception("Coding error in joinEnclave: ChordNode is None.")

        # Just in case, convert to node locations
        bootstrapNodeList = self.convertToNodeLocations(bootstrapNodeList)
        
        d = self.chordNode.joinEnclave(bootstrapNodeList,enableAutoDiscovery, authenticationPayload, isBootstrapNode, enclaveStr)
        if statusCallback is not None:
            d.addBoth(self.joinComplete, statusCallback)

            
    def convertToNodeLocations(self, origList):
        '''Convert a list of (ip, port) tuples into node locations.'''
        
        if origList is None:
            return origList
        else:
            newList = []
            for val in origList:
                if isinstance(val, NodeLocation):
                    # It already a NodeLocation
                    newList.append(val)
                else:
                    # It wasn't a NodeLocation -- fix it!
                    (ip, port) = val
                    newList.append(NodeLocation(None, ip, port))
            return newList
        
    
    @defer.inlineCallbacks
    def getBootstrapEnclaves(self,bootstrapNodeIP, bootstrapNodePort):
        '''Ask the passed in bootstrap node what it's enclave is.
           If it has multiple, this will fail since we don't know the right one then..
           in that case it must be specified.
        '''
        
        bsNodeLocation = NodeLocation(None, bootstrapNodeIP,bootstrapNodePort )
        # Get the remote reference
        (factory, conn) = Utils.getRemoteConnection(bsNodeLocation)
        bootstrapNodeRef = yield factory.getRootObject()
        
        enclaveNames = yield bootstrapNodeRef.callRemote("getEnclaveNames")
        
        # Close the connection to closerNodeLoc
        Utils.disconnect(None, conn)
        
        defer.returnValue(enclaveNames)
        
        
        
    @defer.inlineCallbacks        
    def findEnclave(self, bootstrapNodeIP, bootstrapNodePort):
        '''Ask the passed in bootstrap node what it's enclave is.
           If it has multiple, this will fail since we don't know the right one then..
           in that case it must be specified.
        '''        
        enclaveNames = yield self.getBootstrapEnclaves(bootstrapNodeIP, bootstrapNodePort)
        
        if len(enclaveNames) != 1:
            raise Exception("Trying to get enclave from bootstrap, but it has multiple enclave names!")
        else:
            # Return the enclaveNames
            defer.returnValue(enclaveNames[0])        


    @defer.inlineCallbacks        
    def checkEnclave(self, bootstrapNodeIP, bootstrapNodePort, enclaveToCheck):
        '''Check if the bootstrap node is part of 'enclaveToCheck' '''
        
        enclaveNames = yield self.getBootstrapEnclaves(bootstrapNodeIP, bootstrapNodePort)
        defer.returnValue(enclaveToCheck in enclaveNames)

        
        
    @defer.inlineCallbacks
    def chordStart(self, bootstrapNodeList, isBootstrapNode, enableAutoDiscovery, myPort, myIP, statusCallback, enclaveToJoin, nodeID, authenticationPayload):
        '''
        Start Chord using the given parameters
        '''
        
        # Find a live bootstrap node
        bootstrapNodeLocation = yield Utils.findLiveBootstrapNode(bootstrapNodeList, enableAutoDiscovery)
        log.msg("DEBUG: chordStart: bootstrapNodeLocation returned: %s" % bootstrapNodeLocation, system="chordStart")
        
        if bootstrapNodeLocation is None:
            if isBootstrapNode:
                # This is okay, we are just the only node.
                bootstrapNodeIP = None
                bootstrapNodePort = None
            else:
                # Failure... we can't startup by ourselves!
                log.msg("Failed to join because could not find a live bootstrap node.")
                statusCallback(False, None)
                return
        else:
            bootstrapNodeIP = bootstrapNodeLocation.ip
            bootstrapNodePort = bootstrapNodeLocation.port
            
        # Setup the bootstrap node location list
        if bootstrapNodeList == None:
            bootstrapNodeList = []
        
        if bootstrapNodeIP is not None and bootstrapNodePort is not None:
            bootstrapNodeList.append( NodeLocation(None,bootstrapNodeIP,bootstrapNodePort))
            
        
        
        
        # Figure out the enclave we're joining
        if bootstrapNodeIP == None and bootstrapNodePort == None:
            if enclaveToJoin is None:
                enclaveToJoin = 'localhost'
        else:
            # There is another node we can get info from
            if enclaveToJoin is None or enclaveToJoin == 'fromBootstrap':
                # Figure it out from the bootstrap node
                enclaveToJoin = yield self.findEnclave(bootstrapNodeIP, bootstrapNodePort)
            else:
                # Check if the bootstrap node is consistent
                status = yield self.checkEnclave(bootstrapNodeIP, bootstrapNodePort, enclaveToJoin)
                if not status:
                    raise Exception('Bootstrap node is not part of enclave [%s]. Cannot join like that.' % enclaveToJoin)
       
        print("DEBUG: ChordStart: enclave to join is : %s : %s " % (enclaveToJoin, type(enclaveToJoin)))
        
        
        # Get the node ID for this Node/Enclave
        if nodeID == None:
            raise Exception("Programming error. NodeID cannot be None.")
        
        
        # Make sure it's an int
        if not isinstance( nodeID, ( int, long ) ):
            raise Exception("Programming error. NodeID must be an int.")

        # Set myPort and myIP
        if myPort is None:
            raise Exception("No port specified in ChordStart. No can do!")
        
        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        allowFloatingIP = myIP == None # Do we allow chord node to change the IP? True | False
        if myIP is None:
            if bootstrapNodeIP is None:
                myIP = socket.gethostbyname(socket.gethostname())
                # To use ipNode, We need bootstrapNodeLocation. 
                # Not sure how to change this code if we need external IP? 
                # Maybe this code is fine. -Fengwei   
            else: 
                # There is someone else to ask!

                # Create an IPNode
                ipNode = NetworkUtils.IPNode()
                # The IP address should get from the IPNode in case of NAT
                try:
                    myIP = yield ipNode.getRemoteIP(bootstrapNodeLocation)
                except:
                    log.err()         
        
        
        # Now startup the node
        log.msg("ClassChord API| Starting Chord Node on Port: %s:%s:%s" %(myIP, myPort, enclaveToJoin))
        self.chordNode = ChordNode(myIP, myPort, nodeID, allowFloatingIP)
        self.chordNode.setAuthenticator(self.classChordClientObj) # Who can auth messages for us?
        self.serverFactory = GmuServerFactory(self.chordNode)

        deferResult = self.chordNode.join(bootstrapNodeList,enableAutoDiscovery, enclaveToJoin, authenticationPayload, isBootstrapNode, factory=self.serverFactory)
        deferResult.addBoth(self.joinComplete, statusCallback) # This is our callback, we use it to set some things upon completion.
        

        



        
    def joinComplete(self, joinStatus, clientStatusCallback):
        '''This method gets called when the join is completed either
           successfully or failed.
        '''
        
        if joinStatus == True:
            log.msg("JOIN WAS SUCCESSFUL")
            self.chordNode.addMessageObserver(self.processMessage)
        else:
            log.msg("JOIN FAILED")
            Utils.showError(joinStatus)
                
        # Now call the client's status callback and include the auth payload from the authentication node.
        if clientStatusCallback is not None:
            clientStatusCallback(joinStatus, self.chordNode.joinAuthPayload)
        
        #return joinStatus
    
    def getCurrentNodeLocation(self):
        '''This is mostly for debugging or display. Used to get the current NodeLocation
           for the node being managed by ths Obj.
        '''
        if self.chordNode is not None:
            return self.chordNode.nodeLocation
        else:
            return None
        
    def getCurrentNodeEnclaves(self):
        '''This is mostly for debugging or display. Used to get the current NodeLocation
        '''
        if self.chordNode is not None:
            return self.chordNode.remote_getEnclaveNames()
        else:
            return "None"
        
    def generateNodeID(self, aString, enclaveStr='localhost'):
        '''Generates the node's ID from 'aString'
           The enclave is added on as the high order bits. 
           
           The final ID looks like   enclaveBits | uniq ID bits
           So, if the enclave is 0xFF11 and the uniq ID is 0x12345 theId=0xFF1112345          
        '''
        
        return Utils.generateNodeID(aString, enclaveStr)
            
    def addNetworkObserver(self, observer):
        '''Call the observer method with network statuses'''
        self.chordNode.addNetworkObserver(observer)

    def removeNetworkObserver(self, obs):
        self.chordNode.removeNetworkObserver(obs)
        
                    

        
        
        
            
           
                