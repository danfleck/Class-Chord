''' Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This is the main code implementing the Chord network node.

Created on Feb 24, 2014

@author: dfleck
'''
from twisted.internet import defer, reactor, task, error
import Config
from __builtin__ import isinstance
if Config.SSL_ON:
    from twisted.internet import ssl
from twisted.python import log, failure
from twisted.spread import pb, jelly
import twisted

from types import *

import hashlib
import copy, sys, random
from FingerEntry import FingerEntry
from NodeLocation import CopyNodeLocation
from LargeMessageProducer import LargeMessageFactory
from LargeMessageProducer import LargeMessageProducer
from LargeMessageConsumer import LargeMessageConsumer
from AggregationResponseCache import AggregationResponseCache
from MetricsMessageCounter import MetricsMessageCounter
import glog
import Utils, AutoDiscovery
from SuccessorsList import CopySuccessorsList
import traceback
from MessageCache import MessageCache
import datetime
from gmu.chord import NetworkUtils, ConnectionCache
from RingInfo import RingInfo
import cPickle
from ClassLookup import classLookup
import ClassLookup
from GmuServerFactory import GmuServerFactory

from CopyEnvelope import CopyEnvelope


class ChordNode(pb.Root):
    # what is the threshold to start using streaming protocol for messages?
    largeMsgThreshold = 1024 * 600 # 600K
    
    def __init__(self, theIp, thePort, theId, allowFloatingIP=True):
        '''        
        theID - the ID to use for this node. If not given, one will be generated based off the IP and port.
        theEnclave - the enclave location for this node, if not given we use localhost
        '''

        print("ChordNode running on twisted version: %s" % twisted.version)

        if theId is None:
            raise Exception("theID cannot be None when creating a ChordNode.")

        self.rings = dict() # All the rings we're part of
        self.notifyComplete = dict() # A dict of defered's waiting to be complete.
        
        self.msgsWaitingForAck = dict() # Messages we sent, and are waiting for ACK (typically, aggregation responses)
        
        self.nextFingerToFix = 4
        self.activeNumClients = 0 
        self.totalNumClients = 0
        
        self.messageObservers = []
        self.networkObservers = []
        
        if Config.DEBUG_METRICS:
            self.metricsMessageCounter = MetricsMessageCounter(self)
        else:
            self.metricsMessageCounter = None
        
        self.messageAuthenticator = None
        self.sentMessagesCache = MessageCache()
        self.aggResponseCache = dict() 
        self.joinAuthPayload = None
        self.listeningPort = None
        
        self.largeMsgFactory = None
            
        self.allowFloatingIP = allowFloatingIP # Do we allow this code to change the IP or not?
        self.nodeLocation = CopyNodeLocation()
        self.nodeLocation.setValues(theId, theIp, thePort)

        # Make sure they are defined
        self.numMainCallsSkipped = 0 # How many calls have we skipped? 
        self.maintRunning = False # Is it currently running?
        self.initMaintenanceCalls()
         

    @defer.inlineCallbacks
    def join(self,bootstrapNodeList,enableAutoDiscovery, enclaveName, authenticationPayload, isBootstrapNode,  factory=None):
        '''Join the network and start listening. Basically this re-initializes everything also.
           A custom factory can be passed in, but is not required.
           
           bootstrapNodeList is a list of (node IP, node Port) pairs which list all known bootstrap nodes.
           isBootstrapNode - True|False if this node is or is not a bootstrap node for this enclaveName
           enableAutoDiscovery -- should we auto-discover bootstrap nodes or not?
           
           Returns a deferred which will eventually return True (for success) and False if join failed.
        '''
        
        if not isinstance(enableAutoDiscovery, bool):
            raise Exception("Programming error in join. enableAutoDiscovery is not a bool!")
        
        # Do some checks so we quickly find programming errors.
        if bootstrapNodeList is not None and not isinstance(bootstrapNodeList, list):
            raise Exception("Programming error in join. bootstrapNodeList is not a list!")
        
        if not isinstance(isBootstrapNode, bool):
            raise Exception("Programming error in join. isBootstrapNode is not a bool!")
                    
        (enclaveId, enclaveName) = yield self.getEnclaveIDandName(enclaveName,bootstrapNodeList,isBootstrapNode)
        print("DEBUG: join enc name is:%s  ID:%s" % (enclaveName, enclaveId))
            
        self.rings[enclaveId] = RingInfo(enclaveId, enclaveName, isBootstrapNode, enableAutoDiscovery)
        self.rings[enclaveId].addBootstrapLocations(bootstrapNodeList) # Save so we can re-bootstrap if needed


        ringInfo = self.rings[enclaveId]
        
        # My list of successors        
        ringInfo.successorList = CopySuccessorsList(self.nodeLocation, self.metricsMessageCounter) # A list of succesor's in case one dies
        
        # Init the finger table
        ringInfo.finger = []
        for i in range(FingerEntry.m):
            # Add 1 to i because paper indexes start at 1 not 0
            ringInfo.finger.append(FingerEntry(i+1, self.nodeLocation.id, self.nodeLocation))
        

        # Join the network
        if factory == None:
            factory = GmuServerFactory(self, unsafeTracebacks=True)
            factory.clientConnectionMade = self.clientConnected
        
        self.factory = factory # Store for later.
        
        # During enclave joins may already be listening.
        if self.listeningPort is None:
            yield self._startListening(factory)

            
        try:
            yield self._checkIp(ringInfo)
            yield self.joinNetwork(ringInfo, authenticationPayload)
            yield self.startMaintenenaceCalls()
            if isBootstrapNode:
                # Startup an autodiscovery listener if I'm a bootstrap node
                self.startAutodiscoveryServer(self.nodeLocation)
        except Exception, theErr:
            Utils.showError(theErr)
            self.leave()            
            defer.returnValue(False)

        defer.returnValue(True)

    def startAutodiscoveryServer(self, nodeLocation):
        '''Start an autodiscovery server.
           This should only be called for BootstrapNodes.
        '''
        if not hasattr(self, "autodiscoveryServer"):
            self.autodiscoveryServer = AutoDiscovery.AutoDiscoveryServer(self)
        
        
        
    def joinEnclave(self, bootstrapNodeList,enableAutoDiscovery, authenticationPayload, isBootstrapNode, enclaveStr=None):
        '''
        Join another enclave. This assumes you're currently listening for connections and have joined
            a previous Chord ring.
            
        bootstrapNodeList -- list of node locations on the ring you are trying to join,
        isBootstrapNode -- are you trying to be a bootstrapNode ?
        enableAutoDiscovery -- should we auto-discover bootstrap nodes or not?
        
        enclaveStr -- The enclave you want to join or create if no other nodes are on it.
        
        if you are creating a ring, you'd call this like this:
                bootstrapNodeList=None,authenticationPayload=None, isBootstrapNode=True, enclaveStr=MyEnclaveName)
                
        if you are joining an existing enclave as a bootstrap, you'd call this like this:
                bootstrapNodeList=<list of bootstrap node locations>
                authenticationPayload=payload, 
                isBootstrapNode=True, enclaveStr=ExistingEnclave | None
        
        if you are joining an existing enclave NOT as a bootstrap, you'd call this like this:
                bootstrapNodeList=<list of bootstrap node locations>
                authenticationPayload=payload, 
                isBootstrapNode=False, enclaveStr=ExistingEnclave | None
        '''


        # Do some checks so we quickly find programming errors.
        if bootstrapNodeList is not None and not isinstance(bootstrapNodeList, list):
            raise Exception("Programming error in joinEnclave. bootstrapNodeList is not a list!")
        
        if not isinstance(isBootstrapNode, bool):
            raise Exception("Programming error in joinEnclave. isBootstrapNode is not a bool!")
        
        return self.join(bootstrapNodeList,enableAutoDiscovery,enclaveStr,authenticationPayload,isBootstrapNode, factory=None)
    
    @defer.inlineCallbacks
    def getEnclaveIDandName(self,enclaveName,bootstrapNodeList,isBootstrapNode):
        '''Figure out what the enclave name and ID should be, given the information.'''
        
        enclaveNames = None
        
        # Get any enclaveNames we can.
        if bootstrapNodeList is not None:
            for bsNodeLocation in bootstrapNodeList:
                if bsNodeLocation.ip != self.nodeLocation.ip or bsNodeLocation.port != self.nodeLocation.port:
                    enclaveNames = yield Utils.findEnclaveNames(bsNodeLocation)
                    if enclaveNames != None:
                        break

        
        if enclaveNames is not None and enclaveName in enclaveNames:
            # All good..our name matches something in the bootstrap node.
            enclaveId = Utils.getEnclaveIDFromString(enclaveName)
            defer.returnValue(  (enclaveId, enclaveName) )
            
        elif enclaveName is None and enclaveNames is not None:
            # The user didn't provide a name, can we get one from the bootstrap node?
            if len(enclaveNames) == 1:
                enclaveName =  enclaveNames[0]
                enclaveId = Utils.getEnclaveIDFromString(enclaveName)
                defer.returnValue(  (enclaveId, enclaveName) )
            else:
                raise Exception("BootstrapNode has multiple enclave names, cannot auto choose:%s. Error out." % enclaveNames)
            
        elif enclaveName is None and 'localhost' in enclaveNames:
            # Okay, default to localhost
            enclaveName = 'localhost'
            enclaveId = Utils.getEnclaveIDFromString(enclaveName)
            defer.returnValue(  (enclaveId, enclaveName) )
        elif enclaveNames is None and isBootstrapNode:
            # Okay, we couldn't find any other enclave names, but we're the first node... no problem.
            if enclaveName is None:
                enclaveName = 'localhost'
            enclaveId = Utils.getEnclaveIDFromString(enclaveName)
            defer.returnValue(  (enclaveId, enclaveName) )
        elif enclaveNames is None and enclaveName is not None:
            # There was an error contacting bootstrap nodes, just use the default
            enclaveId = Utils.getEnclaveIDFromString(enclaveName)
            defer.returnValue(  (enclaveId, enclaveName) )
        else:
            if enclaveNames == None:
                enclaveNames = ["None"]
            log.err("Enclave names: %s" % " ".join(enclaveNames))
            raise Exception("Trying to join enclave [%s] but not present in enclaves the bootstrap node knows." % enclaveName)
               
               
    @defer.inlineCallbacks
    def _checkIp(self, ringInfo):
        '''This method attempts to make sure that my IP is a valid external IP.
           Returns True if the IP is valid (or has been switched to a valid IP)
           Returns False otherwise.
        '''
        
        if len(ringInfo.bootstrapNodeLocations) == 0:
            glog.debug("bootstrapNodeLocation is None, so I am the only bootstrapNode")
            defer.returnValue(True)
            
        # First, we need to make sure we can find an external node we can connect OUTGOING to, if not
        # we have no idea and default to return True and hope :-)
        
        result = False 
        madeOutgoingConnection = False
        madeIncomingConnection = False
        
        for bootstrapNodeLocation in ringInfo.bootstrapNodeLocations:
            # see if the node can connect to bootstrap node back and forth
            
            # Never try to check ourselves!
            if bootstrapNodeLocation.ip == self.nodeLocation.ip and bootstrapNodeLocation.port == self.nodeLocation.port:
                continue
            
            glog.debug("_checkIp: [%s] --> [%s] " % (self.nodeLocation, bootstrapNodeLocation))
            
            (madeOutgoingConnection, madeIncomingConnection) = yield self.remote_isConnected(bootstrapNodeLocation)
            glog.debug("_checkIp: %s %s " % (madeOutgoingConnection, madeIncomingConnection))
            if madeOutgoingConnection:
                # Okay, some node is alive, lets try to have it connect incoming
                if madeIncomingConnection:
                    result = True
                    #defer.returnValue(True)
                elif self.allowFloatingIP:
                    # We need to try and change to a new external IP address
                    result1 = yield self._setExternalIp(bootstrapNodeLocation)
                    result2 = yield NetworkUtils.set_portMapping(self.nodeLocation.port)
            
                    # Test connectivity again after set to external IP and port mapping
                    (madeOutgoingConnection, madeIncomingConnection) = yield self.remote_isConnected(bootstrapNodeLocation)
                    if not madeOutgoingConnection or not madeIncomingConnection:
                        print "remote_isConnected is still False with the external IP and port mapping"
                    result = result1 and result2 and madeOutgoingConnection and madeIncomingConnection
                    
                else:
                    log.msg("Cannot connect on this IP, but requested not to change it by allowFloatingIP = False.")
                    
                # Once we made an outgoing connection, we're as good as we get, break out.
                break
                    
    
        self.callNetworkObserver(madeIncomingConnection, madeOutgoingConnection)
        defer.returnValue(result)        
    
    def callNetworkObserver(self, incomingStatus, outgoingStatus):
        '''Call the network observers with two statuses'''
        for obs in self.networkObservers:
            obs( incomingStatus, outgoingStatus )
        
    def addNetworkObserver(self, observer):
        '''Call the observer method with network statuses'''
        if observer not in self.networkObservers:
            self.networkObservers.append(observer)

    def removeNetworkObserver(self, obs):
        self.networkObservers.remove(obs)
    
    @defer.inlineCallbacks
    def _setExternalIp(self, bootstrapNodeLocation):
        ''' set the external IP in the node location'''

        # Before Join, make sure that the IP address is the external one.
        #Create an IPNode
        ipNode = NetworkUtils.IPNode()
        try:
            # Get the external IP from the network utils
            externalIp = yield ipNode.getRemoteIP(bootstrapNodeLocation)
        except:
            print "getRemoteIP raised exception..."
            log.err()
            defer.returnValue(False)
      
        print "_setExternalIp - external IP: %s" %externalIp
        if self.nodeLocation.ip != externalIp:
            print "the IP address specified by user is not the external IP"
            self.nodeLocation.ip = externalIp
            
        defer.returnValue(True)

        
    @defer.inlineCallbacks
    def isConnectedToAnyEnclave(self):
        '''Is this node connected to ANY enclave node?'''
        
        for ringInfo in self.rings.values():
            (madeOutgoingConnection, madeIncomingConnection) = yield self.remote_isConnected(None,ringInfo.enclaveId, True)
            if madeOutgoingConnection and madeIncomingConnection:
                defer.returnValue(True)
        defer.returnValue(False)
        
        
    @defer.inlineCallbacks
    def remote_isConnected(self,testNodeLocation=None,enclaveId=None, checkIncoming=True):
        '''Check if this node is connected to another live node.
           If testNodeLocation is given, we will use it, otherwise
           we'll test if we can connect to a live successor.
           
           checkIncoming=True means the other node will attempt to connect back to this 
               one. This sometimes fails because outbound connections are allowed, but 
               inbound aren't.
           
           Note: This method will not test a connection to yourself, if you're the only
           node in the network, this returns False.
           
           Returns a defered which fires and returns 
             a tuple (outgoing status, incoming status) like this (True, False), (True, True), etc...
             True=Connected, False=Not connected
        '''
        
        madeIncomingConnection = False
        madeOutgoingConnection = False
        
        
        # Do the connection
        try:
            if testNodeLocation is None:
                if enclaveId is None:
                    raise Exception("enclaveID must be specified if testNodeLocation isn't.")
                elif enclaveId not in self.rings:
                    log.msg("Enclave ID specified [%s] is not in my list of known rings." % enclaveId, system="remote_isConnected")
                else:                        
                    #This actually tests a group of successors, so it's more robust than
                    #testing simple testNodeLocation
                    (nodeLoc, nodeRef, conn) = yield self.rings[enclaveId].successorList.getLiveSuccessor()
                    if nodeLoc is not None and nodeLoc != self.nodeLocation:
                        if checkIncoming:
                            
                            (madeOut, _madeIn) = yield nodeRef.callRemote("isConnected", self.nodeLocation,None, False)
                            madeIncomingConnection = madeOut # Out from other node is in to us.
                            
                        # Close the connection
                        if conn is not None: # This can happen for bootstrap nodes
                            yield Utils.disconnect(None, conn)
        
                        madeOutgoingConnection = True
            elif testNodeLocation != self.nodeLocation:
                # Try to connect instead to the testNodeLocation
                try:
                    (factory, conn) = Utils.getRemoteConnection(testNodeLocation, self.metricsMessageCounter)
                    nodeRef = yield factory.getRootObject()
                    madeOutgoingConnection = True

                    if checkIncoming:
                        (madeOut, _madeIn) = yield nodeRef.callRemote("isConnected", self.nodeLocation, None, False)
                        madeIncomingConnection = madeOut # Out from other node is in to us.

                    yield Utils.disconnect(None, conn)
                    
                except Exception, e:
                    # Connection failed in some way
                    log.msg("remote_isConnected failed to get a connection to testLocation [%s]\ndue to error:%s" % (testNodeLocation, e), system=self.nodeLocation)
                    madeOutgoingConnection = False
                            
        except Exception, e:
            traceback.print_exc()
            log.err(e, "Error in remote_isConnected")
            
        # Return the status value     
        if checkIncoming:
            defer.returnValue( (madeOutgoingConnection, madeIncomingConnection) )
        else:
            defer.returnValue( (madeOutgoingConnection, True) )
  
        
    @defer.inlineCallbacks            
    def _startListening(self,factory):
        # Begin listening to the network port
        
        if Config.SSL_ON:
            self.listeningPort = yield reactor.listenSSL(self.nodeLocation.port, factory, ssl.DefaultOpenSSLContextFactory(Config.SSL_PRIVATE_KEY_PATH, Config.SSL_CERT_PATH))
        else:
            self.listeningPort = yield reactor.listenTCP(self.nodeLocation.port, factory)
        
        defer.returnValue(True)
                
    @defer.inlineCallbacks
    def leave(self, dummyDefResult=None):
        '''Leave the network.'''
        try:
            self.stopMaintenanceCalls()
                    
            #Stop listening for Large Messages
            if self.largeMsgFactory is not None:
                if self.largeMsgFactory.numProtocols == 0 and len(self.largeMsgFactory.messages) == 0:
                    yield self.largeConn.stopListening()
                    self.largeMsgFactory = None
                else:
                    log.err("WARNING: Not stopping large factory because transfer still in progress.")

            # Disconnect all
            ConnectionCache.disconnectAll()
            
            # Stop listening for ring connections
            try:
                yield self.listeningPort.stopListening()
                self.listeningPort = None
            except AttributeError:
                pass
            
            # Try and kill any connections made *to* us
            try:
                self.factory.disconnectAll()
                #self.protocol.transport.loseConnection()
            except Exception, e:
                log.msg("Couldn't lose the connection when leaving. Ok. %s" % e)
            
            # Stop the autodiscovery server
            try:
                yield self.autodiscoveryServer.stopListening()
                del self.autodiscoveryServer
            except AttributeError:
                pass
                
            glog.debug("Node %s stopped listening" % self.nodeLocation)
            
            defer.returnValue(True)
        except Exception,e:
            log.err(e, "Error trying to leave")
            defer.returnValue(False)
        
        
    def sendAggregationResponseMessageWithStatus(self, message, envelope):
        '''Send the aggregation response message, and return a deferred which
           fires when the message reaches the FINAL destination.. not just the next
           node in the ring.
           
           The deferred is eventually fired from the AggregationMessageCache code.
        '''
        
        returnDeferred = defer.Deferred() # The deferred we'll return which fires upon final completion
        self.msgsWaitingForAck[envelope['msgID']] = returnDeferred

        # If we fail to send, then fire the returnDefered 
        d = self.sendAggregationResponseMessage(message, envelope)
        d.addErrback(self.aggFloodToRingFailed, returnDeferred)
        
        return returnDeferred
        
    def aggFloodToRingFailed(self, theFail, theDef):
        '''Aggregate flood to ring failed during sendAggregationResponseMessage. 
           Call the defered that should fire in this case.
        '''
        theDef.errback(theFail)
        
        
    @defer.inlineCallbacks
    def sendAggregationResponseMessage(self, message, envelope):
        '''Send an aggregation response type message. 
        
           This message goes from successor to successor until it reaches an aggregation
           point. Once there, the node holds it until it's ready to send to the real dest
           node. 
           
           In this way the dest node isn't overwhelmed with lots of open connections.
           
           Returns a deferred that fires upon successful sending (when message
           gets to the next node or is cached. This is NOT when the message is 
           received at the final dest. Use sendAggregationResponseMessageWithStatus 
           for that.      
           
           This method is used more internally, external calls would probably be better
           off with sendAggregationResponseMessageWithStatus
        '''
        
        #glog.debug("sendAggregationResponseMessage [%s]->[%s]" % (self.nodeLocation.port, envelope['destination']))
        
        #envelope = dict(envelope)
        
        # Add to the message cache
        self.sentMessagesCache.append( (envelope, message) )
        
        # Get the enclave name we're destined for.
        enclaveName = envelope['enclave']
        if enclaveName == 'ALL':
            # We must actually FIND the enclave for the dest node and replace it in the envelope
            # Tricky.
            ringInfo = yield self.findNodeEnclaveId(envelope['destination'])
            if ringInfo == None:
                raise Exception("I cannot find node ID's enclave when sending aggregation response. Cannot send.")
            else:
                envelope['enclave'] = ringInfo.enclaveName
                enclaveName = ringInfo.enclaveName
            
        elif enclaveName is None or enclaveName == "":
            enclaveName = 'localhost'

            
        # Get the ring info for this message
        foundEnclave = False
        for ringInfo in self.rings.values():
            if ringInfo.enclaveName == enclaveName:
                foundEnclave = True
                break
        if not foundEnclave:
            raise Exception("I do not know about enclave [%s] when sending aggregation response. Cannot send." % enclaveName)
                
                
        dest = envelope['destination']

        # Check if I'm the aggregator for this message or not.
        if ringInfo.nodeIndex % envelope['aggregationNumber'] == 0 or \
            envelope['destination'] == self.nodeLocation.id:
            # I am the aggregator
                    
            # Cache the response into the response cache
            if dest in self.aggResponseCache:
                self.aggResponseCache[dest].addResponse(message, envelope)
            else:
                numSecondsToWait = 3
                numMessagesToWaitFor = envelope['aggregationNumber']
                self.aggResponseCache[dest] = AggregationResponseCache(numSecondsToWait, numMessagesToWaitFor, self)
                self.aggResponseCache[dest].addResponse(message, envelope)
                
            status = True

        else:
            # Fwd on to my successor, I'm not the aggregator
            status = yield self.floodToRing(ringInfo, message, envelope)

            
        defer.returnValue(status)
        

                
    @defer.inlineCallbacks
    def sendFloodingMessage(self, message, envelope):
        '''Send a flooding message if we haven't seen this msgID before.
           If we have, don't send it on.
        '''
        
        # Convert to a real dict (since Twisted messes up subclasses of dict)
        #envelopeDict = dict(envelope)
        
        # Have we seen this message?
        if envelope in self.sentMessagesCache:
            glog.debug("sendFloodingMessage: We have seen this envelope before!")
            
            # Check if anyone needs to know about 
            self.messageComplete(message)
            
            defer.returnValue(True)
        
        # Add to the message cache
        self.sentMessagesCache.append( (envelope, message) )
        
        # Figure out the enclave
        if 'enclave' in envelope:
            enclaveStr = envelope['enclave']
            if enclaveStr == 'ALL':
                ringsToSendTo = self.rings.values()
        
            else:
                enclaveId = Utils.getEnclaveIDFromString(enclaveStr)
                if enclaveId in self.rings:
                    ringsToSendTo = [ self.rings[enclaveId] ]
                else:
                    raise Exception("Cannot send from node %s to enclave %s. It's not a member." % (self, enclaveStr))
        else:
            # It's blank, use default enclaveID
            raise Exception("Enclave shouldn't be blank in sendFloodingMessage")
        
        # Send the message to all the ring's we're part of.
        rv = True
        for ring in ringsToSendTo:
            nextRv = yield self.floodToRing(ring, message, envelope)
            rv = rv and nextRv
            
        defer.returnValue(rv)
    
    @defer.inlineCallbacks
    def floodToRing(self, ringInfo, message, envelopeDict):
        '''Flood the message onto a specific ring.
           This should only EVER be called from ChordNode.sendFloodingMessage
        '''
        
        # Get the message length in mostly the same way Twisted does.
        msgLength = len(cPickle.dumps(jelly.jelly(message)))
        
        # Does the envelope have a nodeCounter, if so, use it...otherwise create it.
        #oldindex= ringInfo.nodeIndex
        if 'nodeCounter' in envelopeDict:
            ringInfo.nodeIndex = envelopeDict['nodeCounter']
            envelopeDict['nodeCounter'] = envelopeDict['nodeCounter'] + 1 # Increment
       
            
#         # DEBUG Message
#         if oldindex != ringInfo.nodeIndex:
#             glog.debug("NodeIndex for [%s] changed from %d to %d" % (self.nodeLocation.port, oldindex, ringInfo.nodeIndex))
            
        
        # Send to our successor
        connected = False
        while connected == False:
            #glog.debug("sendFloodingMessage: Trying to send!")

            try:
                (succLoc, nodeRef, conn) = yield ringInfo.successorList.getLiveSuccessor()

                if succLoc is None:
                    glog.debug("sendFloodingMessage: No successor when sending flooding message")
                    defer.returnValue(False)
                elif succLoc == self.nodeLocation:
                    glog.debug("sendFloodingMessage: I'm the only node left when sending flooding message. Done.")
                    defer.returnValue(True)
                
                # Send it a message
                if msgLength <= ChordNode.largeMsgThreshold:
                    status = yield self._sendTheMessage(nodeRef,message,envelopeDict)

                else:
                    # Need to use streaming protocol
                    
                    # Launch a producer
                    if self.largeMsgFactory is None:
                        self.largeMsgFactory = LargeMessageFactory()
                        self.largeMsgPort = self.nodeLocation.port+1000
                        self.largeMsgFactory.protocol = LargeMessageProducer
                        
                        if Config.SSL_ON:
                            self.largeConn = yield reactor.listenSSL(self.largeMsgPort, self.largeMsgFactory, ssl.DefaultOpenSSLContextFactory(Config.SSL_PRIVATE_KEY_PATH, Config.SSL_CERT_PATH))
                        else:
                            self.largeConn = yield reactor.listenTCP(self.largeMsgPort, self.largeMsgFactory)
                        
                    # Tell the Factory what the message key is
                    msgKey = self.largeMsgFactory.addMessage(message)
                    
                    # Tell it what to do
                    status = yield nodeRef.callRemote("receiveLargeMessage", msgKey, self.largeMsgPort, self.nodeLocation.ip, envelopeDict)
                    
                # Done!
                connected = True
                                
                # Close the connection to closerNodeLoc
                yield Utils.disconnect(None, conn)
                
                defer.returnValue(status)

            except Exception:
                glog.debug("sendFloodingMessage: Successor connect failed in sendFlooding. Rebuilding successorList and retrying. [From:%s to Successor:%s]" % (self.nodeLocation.port,  succLoc))
                log.err()
                worked = yield ringInfo.successorList.rebuildSuccessorList(ringInfo.enclaveId)
                glog.debug("Rebuild successors list worked???? %s" % worked)
                connected = False
            
            
    def remote_sendMessage(self, msgText, envelope):
        ''' This is just a remote enabler for sendClassMessage tests.
            However, it does notify when the message is finally dead.
            
            This happens when a node decides no longer to fwd the message, it will
            notify this node and then this node will fire d's callback.
            '''
        
        d = defer.Deferred()
        self.notifyComplete[msgText['testNum']] = d
        
        msgType = envelope['type']
        
        if msgType == 'classType':
            # Send the message --- but don't count it in the metrics, for debug only
            status = self.sendClassMessage(msgText, envelope)
        elif msgType == "flood":
            status = self.sendFloodingMessage(msgText, envelope)
        elif msgType == "p2p":
            status = self.sendSyncMessage(msgText, envelope)
            d = status
            
        else:
            raise Exception("Unknown msgType [%s] in remote_sendMessage" % msgType)
                
        return d
        
    @defer.inlineCallbacks
    def sendClassMessage(self, msgText, envelope):
        '''Send a class message to the successor in our class.
           We want it to iterate through all successors.'''
        
        glog.debug("-------------------- \n Sending...from:%s" % self.nodeLocation, system="sendClassMessage")
        
        if envelope in self.sentMessagesCache:
            glog.debug("We have seen this envelope before.", system="sendClassMessage")
            defer.returnValue(True)
                   
        glog.debug("Msg not in cache...from:%s" % self.nodeLocation, system="sendClassMessage")                  
        # Add to the message cache
        self.sentMessagesCache.append( (envelope, msgText) )        

        # Get the next valid ID within the class/enclave to send to
        nextID = ClassLookup.getNextHop(self.nodeLocation.id, envelope)
        glog.debug("NextID: %s ...from:%s" % (nextID, self.nodeLocation), system="sendClassMessage")
        
        
        
        if nextID == None:
            glog.debug("No more nodes in the class. Message stopping.",  system="sendClassMessage")
            defer.returnValue(True)
        
        glog.debug("NextID is Me? %s" % (long(nextID) == self.nodeLocation.id), system="sendClassMessage")
        
            
        enclaveId = Utils.getEnclaveID(nextID)
        
        glog.debug("ClassMessage enclaveID is: %s" % enclaveId, system="sendClassMessage")
                           
        # Figure out the successor from the envelope            
        succLoc = yield self.remote_findSuccessorLocation(nextID, enclaveId)
        glog.debug("SuccLoc: %s ...from:%s" % (succLoc, self.nodeLocation), system="sendClassMessage")

        # Connect to the successor        
        (factory, conn) = yield Utils.getRemoteConnection(succLoc)
        noderef = yield factory.getRootObject()

        if noderef is None:
            glog.debug("sendClassMessage: No Class successor found. Done.")
            defer.returnValue(False)
        elif noderef == self.nodeLocation:
            print("sendClassMessage: I'm the only node left when sending Class message. Done.")
            defer.returnValue(False)                
        
        # Send message to successor
        glog.debug("Sending message to SuccLoc: %s ...from:%s" % (succLoc, self.nodeLocation), system="sendClassMessage")
        status = yield self.sendMessageToLocation(noderef, msgText, envelope)
        
        # Close the connection
        if conn != None:
            # Close the connection to succLoc
            yield Utils.disconnect(None, conn)
        
        defer.returnValue(status)
        
            
    @defer.inlineCallbacks
    def findNodeEnclaveId(self, toID):
        '''Find the ringInfo of the node with ID toID.
           Return the enclaveName or None if we can't find it.
        '''
        for ring in self.rings.values():
            ringInfo = ring
            enclaveId = ringInfo.enclaveId
            succLoc = yield self.remote_findSuccessorLocation(toID, enclaveId)
            if succLoc is not None and succLoc.id == toID:
                defer.returnValue(ringInfo)
            
        defer.returnValue(None)
                    
    
    @defer.inlineCallbacks
    def sendSyncMessage(self,  msgText, envelope):
        '''Send a message to a single, specific destination.
           msgText is a string or probably any standard data type (supported by Twisted PB)
           envelope is a message envelope.'''
        
        # Figure out the successor            
        succLoc = yield self.getSuccLocation(envelope)
        if succLoc == False:
            glog.debug("sendSyncMessage succLoc is False", system=self.nodeLocation)
            defer.returnValue(False)
            
        # Get a nodeRef
        if succLoc.id == self.nodeLocation.id: # Messages to me!
            nodeRef = self
            conn = None
        else:
            # Open a connection to that destination
            (factory, conn) = Utils.getRemoteConnection(succLoc, self.metricsMessageCounter)
            nodeRef = yield factory.getRootObject()
            
        # Send the successor the message
        status = yield self.sendMessageToLocation(nodeRef, msgText, envelope)
        
        # Close the connection
        if conn != None:
            # Close the connection to succLoc
            yield Utils.disconnect(None, conn)
        
        defer.returnValue(status)
        
    @defer.inlineCallbacks
    def sendSyncMultipleMessage(self, listOfMessages, changeMsgTypeTo=None):
        '''This method is used by the aggregation message code to send 
           lots of messages to one dest. 
           
           listOfMessages is a list of (message, envelope) tuples. This 
           code will error out of the dest is not the same in the entire list.
           
           The method will return a dict of sendStatuses (True/False) for each message
           dict[msgID] = True/False for each in listOfMessages.
           
           if changeMsgTypeTo is set to a String, then the outgoing envelope msg type will be
           set to that type. This is so agg-response messages become p2p messages and are treated
           as such.
           
        '''
        
        allStatuses = dict()
        
        if len(listOfMessages) == 0:
            defer.returnValue(allStatuses)
        
        
        conn = None
        
        try:
        
            # Get the destination
            envelope = listOfMessages[0][1]
            mainDest = envelope['destination']
            
            # Figure out the location to send to            
            succLoc = yield self.getSuccLocation(envelope)
            if succLoc == False:
                defer.returnValue(False)
                    
                
            if succLoc.id == self.nodeLocation.id: # Messages to me!
                for (msgText, envelope) in listOfMessages:
                    #envelope = dict(envelope)
                    
                    if changeMsgTypeTo is not None:
                        envelope['type'] = changeMsgTypeTo
                        
                    s = yield self.remote_receiveMessage(msgText, envelope)
                    allStatuses[envelope['msgID']] = s
    
                    if mainDest != envelope['destination']:
                        raise Exception("Had multiple destinations in sendSyncMultiple. Error! [%s] [%s] " % (mainDest, envelope['destination']))        
            else:
                                
                # Open a connection to that destination
                (factory, conn) = Utils.getRemoteConnection(succLoc, self.metricsMessageCounter)
                nodeRef = yield factory.getRootObject()
                
                # Send a bunch of messages
                for (msgText, envelope) in listOfMessages:
                    #envelope = dict(envelope)
                    
                    if changeMsgTypeTo is not None:
                        envelope['type'] = changeMsgTypeTo
                                            
                    print("DEBUG: sendSyncMultipleMessage msg from[%s] -> [%s]" % (self.nodeLocation.port, succLoc.port))
    
                    s = yield self.sendMessageToLocation(nodeRef, msgText, envelope)
                    allStatuses[envelope['msgID']] = s
            
                # Close the connection
                if conn != None:
                    # Close the connection to succLoc
                    yield Utils.disconnect(None, conn)
                   
        except Exception, e:
            log.err(e, "Error in send sync multiple") 
            
        
        # Return the dict of statuses
        defer.returnValue(allStatuses)
                
            

    @defer.inlineCallbacks        
    def sendSingleAck(self, msgID, source, status):
        '''Send an ack for msgID to nodeLocation 'source' with status.'''
            
        try:
            if source == self.nodeLocation:
                # Ack to self
                self.remote_receiveAck(msgID, status)
            else:
                # Now send the ACK for the message to the sender
                (factory, conn) = Utils.getRemoteConnection(source, self.metricsMessageCounter)
                nodeRef = yield factory.getRootObject()
                
                yield nodeRef.callRemote("receiveAck", msgID, status)
                
                yield Utils.disconnect(None, conn) # Disconnect
        except Exception, e:
            log.err(e, "sendSingleAck failed to send to [%s][%s]" % (source, e))
            defer.returnValue(False)
            
        defer.returnValue(True)
                        
        
        
    def remote_receiveAck(self, msgID, status):
        '''This is an ack from an aggregation message with id msgID.
           So we need to now callback the original callback from 
           sendAggregationResponseMessage
        '''
        
        #glog.debug("remote_receiveAck: Node[%s] got ack for [%s]" % (self.nodeLocation.port, msgID))
        
        # Find the deferred to callback
        if msgID in self.msgsWaitingForAck:
            d = self.msgsWaitingForAck[msgID]
        else:
            ids = self.msgsWaitingForAck.keys()
            raise Exception("[%s]: receiveAck for message ID I don't know about. Error. [%s] [%s]" % (self.nodeLocation.port, msgID, ids))
        
        # Call it
        if status == True:
            d.callback(status)
        else:
            d.errback(status)
        
        # Remove from the dict
        del self.msgsWaitingForAck[msgID]
            
        

    @defer.inlineCallbacks    
    def sendMessageToLocation(self, nodeRef, msgText, envelope):
        # Get the message length in mostly the same way Twisted does.
        msgLength = len(cPickle.dumps(jelly.jelly(msgText)))

        
        if nodeRef == self: # Message to me!
            status = yield self.remote_receiveMessage(msgText, envelope)            
            defer.returnValue(status)
            
        elif msgLength > ChordNode.largeMsgThreshold: # Bigger than 600K
            # We need to use the producer/consumer protocol
            # This works like this:
            # 1. Message sender launches a factory which is a Message Producer
            # 2.     The factory stores the message to send with a random key
            # 3. The sender sends the key and the producer's node location to the receiver
            # 4. The receiver connects to the sender and sends the key
            # 5. The producer validates the key, and sends the message to the receiver

            # Launch a producer
            if self.largeMsgFactory is None:
                self.largeMsgFactory = LargeMessageFactory()
                self.largeMsgPort = self.nodeLocation.port+1000
                self.largeMsgFactory.protocol = LargeMessageProducer
                
                if Config.SSL_ON:
                    self.largeConn = yield reactor.listenSSL(self.largeMsgPort, self.largeMsgFactory, ssl.DefaultOpenSSLContextFactory(Config.SSL_PRIVATE_KEY_PATH, Config.SSL_CERT_PATH))
                else:
                    self.largeConn = yield reactor.listenTCP(self.largeMsgPort, self.largeMsgFactory)
                                
            # Tell the Factory what the message key is
            msgKey = self.largeMsgFactory.addMessage(msgText)

            # Tell the remote node to connect and get the message on the appropriate port
            # Also, send the envelope
            status = yield nodeRef.callRemote("receiveLargeMessage", msgKey, self.largeMsgPort, self.nodeLocation.ip, envelope)

            defer.returnValue(status)

        else:
            
            # Send it a message
            try:
                status = yield self._sendTheMessage(nodeRef,msgText,envelope)
    
                # Done
                defer.returnValue( status )
            except Exception, e:
                log.err(e, "Error in sendMessageToLocation: Msg was:[%s]  Env Was:[%s]" % (msgText, envelope))
                defer.returnValue( False )
                       
    def _sendTheMessage(self,nodeRef,msgText,envelope):
        '''Returns a deferred after sending the message to the nodeRef.
           This should be the only place we call receiveMessage so we can 
           accurately compute metrics.
        '''
        
        if Config.DEBUG_METRICS:
            self.metricsMessageCounter.sentMsg(msgText, envelope)
        
        return nodeRef.callRemote("receiveMessage", msgText, envelope)
    
    @defer.inlineCallbacks
    def getSuccLocation(self, envelope):
        '''Figure out where this msg should go.
           Send a message to a single, specific destination.
           msgText is a string or probably any standard data type (supported by Twisted PB)
           envelope is a message envelope.'''
        
        toID = envelope['destination']
        
        #glog.debug("getSuccLocation: toID:%s" % toID, system=self.nodeLocation)
        if envelope['type'] != 'p2p' and envelope['type'] != 'agg-response' and envelope['type'] != 'classType':
            raise Exception("Coding error... sendSyncMessage with a non-P2P/AGG message!")
        if toID == None:
            raise Exception("Coding error... sendSyncMessage with a missing destination!")
        
        # Get the enclave the node is on
        enclaveId = Utils.getEnclaveID(toID)
        if enclaveId not in self.rings:
            # The node may be in a ring with us, but it's ID
            # has an enclave we don't know. This can happen if we are trying to send to 
            # a manager node which is part of multiple enclaves, but only gets one ID
            
            for ring in self.rings.values():
                ringInfo = ring
                enclaveId = ringInfo.enclaveId
                succLoc = yield self.remote_findSuccessorLocation(toID, enclaveId)
                if succLoc is not None and succLoc.id == toID:
                    break # We found it!
        else:
            # Check the one ring (since we know it)
            ringInfo = self.rings[enclaveId]

            # Locate the node with the given ID
            succLoc = yield self.remote_findSuccessorLocation(toID, enclaveId) 

        
        if succLoc is None:
            log.err("sendSyncMessage: nodeID [%s] has no successor" % toID, system=self.nodeLocation)
            defer.returnValue(False)
            
        if succLoc.id != toID:
            # The node does not exist in the network!
            log.msg("syncMessage returning FALSE [ID is unreachable/does not exist in network]")
            defer.returnValue(False)
        else:
            defer.returnValue(succLoc)
            
                    
        
        
    
    
    @defer.inlineCallbacks
    def remote_receiveLargeMessage(self, msgKey, port, ip, envelope):
        '''The remote node wants to send a large message to me... get it from them
           on the port/ip they passed in.
        '''
        
        try:
            consumer = LargeMessageConsumer()
            (connectionOpen, finishedReceivingDefer) = consumer.openConnection(ip, port) # this starts the transfer
            
            yield connectionOpen # Wait for the connection to open
            
            yield consumer.sendLine("%s" % msgKey) # Send the key to start the transfer
            
            
            defResult = yield finishedReceivingDefer # Finish receiving the message
            
            # Process the message
            if defResult:
                status = yield self.remote_receiveMessage(consumer.totalMessage, envelope)
                defer.returnValue(status)
            else: 
                log.err("remote_receiveLargeMessage failed for some reason.", system=str(self.nodeLocation))
                defer.returnValue(False)
        except Exception, e:
            log.err("remote_receiveLargeMessage failed for some unknown reason.", system=str(self.nodeLocation))
            defer.returnValue(False)
            
        
        
             
    def setAuthenticator(self, authenticator):
        '''Sets the authenticator class. 
           In our case this is the client who can decrypt 
           and authenticate messages for us.
           
           If it's None, then the code continues without auth/decrypting
           which is possibly not the right thing to do once we get to production.
        '''
        self.messageAuthenticator = authenticator
        
  
    def decryptAndAuthenticate(self, encMsg, encEnvelope):
        '''Decrypt the message and envelope.
           Check it's all valid,
           return (True, unencryptedMsg, unencryptedEnvelope) or
           return (False, None, None)
           
           If there is no messageAuthenticator set, 
           provide a warning.
        '''
        
        if self.messageAuthenticator is None:
            if Config.WARN_NO_MESSAGE_AUTHENTICATOR:
                log.msg("Warning: No message authenticator set. Continuing without checking.")
            return (True, encMsg, encEnvelope)
        
        #Check Auth
        if self.messageAuthenticator.authMessage(encMsg, encEnvelope) == False:
            log.msg("decryptAndAuthenticate: Received Message Auth failure. Dropped...")
            return (False, None, None)
        
        # Decrypt the message and envelope
        #print("encEnvelope TYPE IN DECRYPT IS %s" % encEnvelope)
        envelope = self.messageAuthenticator.decryptEnvelope(encEnvelope)
        message = self.messageAuthenticator.decryptMessage(encMsg)
        #print("ENV TYPE IN DECRYPT IS %s" % envelope)
        # Check the TTL
        if 'ttl' not in envelope:
            raise Exception("Coding error. Envelope has no member named ttl. %s" % message)
        if datetime.datetime.now() > envelope['ttl']:
            log.msg("decryptAndAuthenticate: Message TTL expired %s" % envelope['ttl'])
            return (False, None, None)
        
        # Looks like we're okay!
        return (True, message, envelope)
        
                
    def remote_doClassMessageTest(self, msgText):
        '''Do the class message test from this node.'''
        
        # Done in somewhat stupid way by using the messageObserver framework. Hack!
        
        # Build the envelope
        theEnv = CopyEnvelope()
        theEnv['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
        theEnv['source'] = self.nodeLocation
        theEnv['type'] = 'p2p'
        theEnv['destination'] = self.nodeLocation.id # To me! 
        theEnv['msgID'] = random.getrandbits(128) # TODO: Something better here!
        
        d = defer.Deferred()
        
        
        self.notifyComplete[msgText['testNum']] = d
        
        # Send the message --- but don't count it in the metrics, for debug only        
        status = self.remote_receiveMessage(msgText, theEnv)
    
        return d
    
    def remote_notifyMessageComplete(self, testNum):
        '''Fire the defered which says this message has finally completed.'''
        
        log.msg("remote_notifyMessageComplete got num: %s" % testNum)
        if testNum in self.notifyComplete:
            self.notifyComplete[testNum].callback(True)
            del self.notifyComplete[testNum]
        else:
            raise Exception("Unknown key in notifyComplete [%s]" % testNum)
            
    @defer.inlineCallbacks
    def messageComplete(self, message):
        '''Notify nodeLoc that the message is complete (meaning you aren't 
           fwd'ing it anymore. Used for class messages to test them easily.
           Not useful in production code.
        '''
        
        if not isinstance(message, dict):
            return
        
        if "notifyWhenComplete" not in message:
            return
            
            
        nodeLoc = message["notifyWhenComplete"]            
        testNum = message["testNum"] 
        
        # Get the remote reference
        (factory, conn) = Utils.getRemoteConnection(nodeLoc, None)
        theNode = yield factory.getRootObject()
        
        # Tell it we're good
        yield theNode.callRemote("notifyMessageComplete", testNum)
        
        # Close the connection
        yield Utils.disconnect(None, conn)
         
        
                        
    @defer.inlineCallbacks        
    def remote_receiveMessage(self, incomingMessage, incomingEnvelope):
        '''A message came in from another node'''      
        
        # Decrypt, authenticate and verify the message
        # drop the message if it's invalid or expired.
        (isValid, message, envelope) = self.decryptAndAuthenticate(incomingMessage,incomingEnvelope)

           
        if not isValid:
            defer.returnValue(True) # Maybe we should have a different status here?
            glog.debug("DEBUG: I got an invalid message! %s" % self.nodeLocation, system="receiveMessage")
    
        if Config.DEBUG_METRICS:
            self.metricsMessageCounter.recvMsg(message, envelope) # Note: this counts even things in cache!
            
            if "classSpec" in message:
                message['destination'] = message['classSpec'] # Hack since it really expects an envelope
                inMyClass = ClassLookup.messageInMyClass(self.nodeLocation.id, message)
                self.metricsMessageCounter.classMessageReceived(message, inMyClass)
            
                                 
        # Tell everyone about it, since it's valid.
        if envelope['type'] == 'flood' or \
           envelope['destination'] == self.nodeLocation.id:
            status = self.callObservers(message, envelope)
            
            
        elif envelope['type'] == 'classType':
            if ClassLookup.messageInMyClass(self.nodeLocation.id, envelope):
                status = self.callObservers(message, envelope)
            else:
                # wasted message
                status = True 
        else:
            status = True
        
             
        # Did we send this message already?
        if envelope in self.sentMessagesCache:
            glog.debug("DEBUG: Message was in my cache. Goodbye %s" % self.nodeLocation, system="receiveMessage")
            
            # Check if anyone needs to know about 
            self.messageComplete(message)

            defer.returnValue(True) 
            
        # Now resend it if needed
        if status:
            if envelope['type'] == 'flood':
                status = yield self.sendFloodingMessage(message, envelope)
            elif envelope['type'] == 'agg-response':
                if envelope['destination'] != self.nodeLocation.id:
                    status = yield self.sendAggregationResponseMessage(message, envelope)
                else:
                    # ACK The message since it won't be done otherwise.
                    msgID = envelope['msgID']
                    source = envelope['source']
                    status = yield self.sendSingleAck(msgID, source, True)
                
                
            elif envelope['type'] == 'classType':
                ''' If we receive a class message,
                    send it on if we have a successor in our class.
                    # TODO: perform a more succinct check of successor
                    to make sure in falls within our query to handle
                    corner-cases that are escaping our current query
                    criteria that are failing:
                    Successful:
                    0?0? = 0201 and 0301
                    0??? = 0201 and 0301
                    0?01 = 0201 and 0301
                    ???? = 0201 and 0301
                    
                    Failed:
                    020? = 0201 and 0301 # Should not have returned 0301
                    0051 = 0201 # Should only have looked at 0051
                    0101 = 0201 # ""
                    0500 = 0505 # ""
                    '''
                
                status = yield self.sendClassMessage(message, envelope)

        defer.returnValue(status)  
    
        
    def callObservers(self, message, envelope):
        # Call the observers once the message is known to be valid.
        for o in self.messageObservers:
            if isinstance(message, dict):
                o(message.copy(), envelope.copy())
            else:
                o(message, envelope)
        
        return True   
        
    def addMessageObserver(self, observer):
        '''Call the observer method with any messages received'''
        if observer not in self.messageObservers:
            self.messageObservers.append(observer)
        
    def removeMessageObserver(self, observer):
        self.messageObservers.remove(observer)
            
    def clientConnected(self, protocol):
        '''This is a callback from GmuServerFactory
           when the client is connected.
        '''
        self.activeNumClients +=1
        self.totalNumClients += 1
        #print("DEBUG: clientWasConnected %s \n\n\n" % protocol)
        self.protocol = protocol
        
    def remote_getClientIP(self):
        '''Get the IP of the current connection.'''
        peer = self.protocol.transport.getPeer()
        return peer.host   

        
    def clientDisconnected(self):
        '''This is a callback from GmuServerFactory
           when the client is disconnected.
        '''        
        self.activeNumClients -=1
        #glog.debug("Removing client %d" % self.activeNumClients)
        
    def initMaintenanceCalls(self):
        self.doMaintenanceCall = None
        
        
    def stopMaintenanceCalls(self):
        '''Stop any maintenance calls which are running.'''
        if self.doMaintenanceCall is not None and self.doMaintenanceCall.running:
            self.doMaintenanceCall.stop()
                
                
    def startMaintenenaceCalls(self):
        ''' After joinNetwork is complete, start periodic maintenance calls'''        
        # Setup the stabilize call
        
        if self.doMaintenanceCall is None or not self.doMaintenanceCall.running: # Only start if not started!
            glog.debug("starting maintenance calls", system=self.nodeLocation)
            self.doMaintenanceCall = task.LoopingCall(self.doMaintenance)
            self.doMaintenanceCall.start(5.0, now = False)
        return True
    
    
        
    @defer.inlineCallbacks
    def doMaintenance(self):
        '''Periodically called to perform maintenance on the network.
           This does the calls in a sequence to help minimize some threading issues.
        '''

        # Don't let maint calls run over each other, but if we skip a bunch
        # let it run anyway.
        if self.maintRunning == True and self.numMainCallsSkipped < 10:
            log.err("Maintenance calls still running! Skipping this run. [%d]" % self.numMainCallsSkipped, system=self.nodeLocation)
            self.numMainCallsSkipped += 1
            defer.returnValue(True)
            
        self.maintRunning = True
        self.numMainCallsSkipped = 0 

        # Increment the fingers to fix  
        self._updateNextFingerToFix()      
        
        for (enclaveID, ringInfo) in self.rings.iteritems():
            try:
                # Run all the maintenance calls in series (unless stop was called!)
                if self.doMaintenanceCall.running:
                    yield self.stabilize(ringInfo)
                    
                if self.doMaintenanceCall.running:
                    yield self.fixFingers(ringInfo)
                    
                if self.doMaintenanceCall.running:
                    yield ringInfo.successorList.rebuildSuccessorList(ringInfo.enclaveId, ringInfo.bootstrapNodeLocations)
                    
                if self.doMaintenanceCall.running:
                    yield self.checkPredecessor(ringInfo)
                    
                if self.doMaintenanceCall.running:
                    yield self._checkForNetworkSeparation(ringInfo)
            except Exception, e:
                log.err(e, "Error doing maintenance on enclave [%s]" % enclaveID)
            
            
        self.maintRunning = False # So we can run again
        
        defer.returnValue(True)
        

    def _updateNextFingerToFix(self):
        # Increment the fingers to fix        
        self.nextFingerToFix += 1
        if (self.nextFingerToFix >= FingerEntry.m):
            self.nextFingerToFix = 1        
        
    def remote_getNodeMetricMessageCounts(self, resetAfter=False):
        '''Get all the information needed by WalkNetworkSerial into a dict and 
           return it.
           This is only used for metrics purposes, shouldn't be used in general
        '''
        if Config.DEBUG_METRICS:
            
            d = dict()
            d['numSent'] = self.metricsMessageCounter.numSent
            d['numRecv'] = self.metricsMessageCounter.numRecv
            d['nodeLoc'] = self.nodeLocation
            d['succLoc'] = self.remote_getSuccessorLocations()
            d['predLoc'] = self.remote_getPredLocations()
            d['nodeEnclaves'] = self.remote_getEnclaveNames()
            
            d['activeClientConnections'] = self.activeNumClients  # How many are currently connected to us?
            d['totalClientConnections'] = self.totalNumClients # How many ever connected to us?
            
            d['totalOutboundConnections'] = self.metricsMessageCounter.numOutgoingConnections # How many have we ever connected out to?
            
            d['shortLookups'] = self.metricsMessageCounter.shortLookups
            d['longLookups'] = self.metricsMessageCounter.longLookups
            
            d['classMsgDict'] = self.metricsMessageCounter.classMsgs
            
            if resetAfter:
                self.metricsMessageCounter.resetAll()
                self.totalNumClients = self.activeNumClients
                
            return d
        else:
            raise Exception("Coding error... tried to getNodeMetrics, without enabling them!")
        
        
        
    def remote_getEnclaveNames(self):
        '''Return the list of names this node knows about'''
        theList = []
        for ringInfo in self.rings.values():
            theList.append(ringInfo.enclaveName)
        return theList

    def remote_hasEnclaveId(self, anID):
        for ringInfo in self.rings.values():
            if ringInfo.enclaveId == anID:
                return True
        return False
        
    def remote_getSuccessorList(self, enclaveId):
        '''Return the successor's list object'''
        return self.rings[enclaveId].successorList
           
    def remote_getNodeStr(self, enclaveId):
        '''Called from the ClientWalkNetwork code.'''
        
        ringInfo = self.rings[enclaveId]
        
        return "ID:%s IP:%s PORT:%s PRED:%s SUCC:%s" % (self.nodeLocation.id, self.nodeLocation.ip, self.nodeLocation.port, ringInfo.predecessorLocation, ringInfo.successorList.getSuccessor())
        

    def remote_getFingerTableStr(self, enclaveId):
        '''Called from the ClientWalkNetwork code.'''
        theStr = ""
        ringInfo = self.rings[enclaveId]
        
        for f in ringInfo.finger:
            theStr += "\n   "+str(f)
        return theStr
            
        
    @defer.inlineCallbacks
    def remote_findSuccessorLocation(self, theId, enclaveId, countedAlready = False):
        '''Returns a the successor of the given ID.
        '''
        assert type(theId) is not StringType, "theID was type: %s" % type(theId)
        if enclaveId not in self.rings:
            for k in self.rings.iterkeys():
                print("DEBUG: ring's enclave ID: T:%s T2:%s %s" % (type(k), type(enclaveId), k))
            raise Exception("remote_findSuccessorLocation: node [%s] does not know about enclave ID given [%s]" % (self.nodeLocation, enclaveId))
        
        ringInfo = self.rings[enclaveId]
        
        mySuccessor = ringInfo.successorList.getSuccessor()
        myPredecessor = ringInfo.predecessorLocation
        
        # Is my successor the answer?
        if mySuccessor != None and self.inIntervalInclusiveRight(theId, self.nodeLocation.id, mySuccessor.id):
            # is the ID between me and my successor?
            
            # Counter here for short lookups
            if not countedAlready:
                self.metricsMessageCounter.shortLookup()
            glog.debug("Short lookup to find mySuccessor: ID:%s [%s]" % (theId, mySuccessor), system=self.nodeLocation)
            
            defer.returnValue(mySuccessor)
            
        elif myPredecessor != None and  self.inIntervalInclusiveRight(theId, myPredecessor.id, self.nodeLocation.id):
            # Is the successor for this ID me?
            
            # Counter here for short lookups
            if not countedAlready:
                self.metricsMessageCounter.shortLookup()
            glog.debug("Short lookup to find msg is for me: ID:%s [%s]" % (theId, myPredecessor), system=self.nodeLocation)
            
            defer.returnValue(self.nodeLocation)
        else:
            # Not me...Forward to other closer nodes
            closerNodeLoc = self.remote_closestPrecedingFinger(theId, enclaveId)
            #glog.debug("DEBUG: find_succ calling closest_preceding_finger with id:%s,  closer ID:%s" % (theId, closerNodeLoc.id))
            
            # If I'm the best option, go with it to avoid infinite loop
            if closerNodeLoc == self.nodeLocation:
                # Counter here for short lookups
                if not countedAlready:
                    self.metricsMessageCounter.shortLookup()
                glog.debug("Short lookup to find myself [%s]" % self.nodeLocation, system=self.nodeLocation)
            
                defer.returnValue(self.nodeLocation)
            else:
                try:
                    # Counter here for long lookups
                    if not countedAlready:
                        self.metricsMessageCounter.longLookup()
                        countedAlready = True
                    
                    # Get the remote reference
                    (factory, conn) = Utils.getRemoteConnection(closerNodeLoc, self.metricsMessageCounter)
                    closerNodeRef = yield factory.getRootObject()
                    
                    succ = yield closerNodeRef.callRemote("findSuccessorLocation", theId, enclaveId, countedAlready)
                    
                    # Close the connection to closerNodeLoc
                    yield Utils.disconnect(None, conn)
                    
                    glog.debug("Long lookup to find ID:%s [%s]" % (theId, succ), system=self.nodeLocation)
                                
                    # Return the succ
                    defer.returnValue(succ)
                    
                except Exception, e:
                    log.msg("Couldn't find successor in findSuccessorLocation. Returning None. [%s][%s]" % (e, closerNodeLoc), system=str(self.nodeLocation))
                    #log.err(e)
                    ringInfo.destroyFingerLocation(closerNodeLoc)
                    defer.returnValue(None)
               



    @defer.inlineCallbacks 
    def _checkForNetworkSeparation(self, ringInfo):
        '''In rare cases two rings can form. If that happens they'll never join unless we check 
           a common bootstrap node for connectivity. Do that occasionally!
           
           FirstCall is used internally to stop recursion.
        '''
        try: # Put in Try/Catch since we're a looping call.
            
            # Get a connection to the bootstrap node (other than me!)
            bootstrapNodeLocation = yield Utils.findLiveBootstrapNode(ringInfo.bootstrapNodeLocations, ringInfo.enableAutoDiscovery, excludeNode=self.nodeLocation, enclaveIDToFind=ringInfo.enclaveId)

            if bootstrapNodeLocation is not None:
                
                # Add it to the list of locations
                ringInfo.addBootstrapLocations( [ bootstrapNodeLocation ])

                # Connect to it
                (factory, conn) = Utils.getRemoteConnection(bootstrapNodeLocation, self.metricsMessageCounter)
                bootstrapNode = yield factory.getRootObject()
                 
                # Ask bootstrap node what it thinks my successor is
                succLoc = yield bootstrapNode.callRemote("findSuccessorLocation", self.nodeLocation.id, ringInfo.enclaveId)

                if succLoc is not None:
                    # Add the successor to my list
                    ringInfo.successorList.addValues(succLoc)
                    
                # Close the connections
                yield Utils.disconnect(None, conn)

        except Exception, e:
            log.err("Error in _checkForNetworkSeparation (continuing) [%s]" % e, system="_checkForNetworkSeparation")
                    
            
    @defer.inlineCallbacks
    def checkPredecessor(self, ringInfo):
        '''Verify if my predecessor is alive.
           If I cannot connect to my Pred, call them dead.
           
           If I can connect to my Pred, but it cannot connect back to me...
               re-verify my IP because I may have moved IP addresses!
        '''
        
        connectedToPred = False
        # Wrap in try/catch to ensure looping call continues without fail.
        try:
            if ringInfo.predecessorLocation is not None:
                
                # First we are trying to connect to the predecessor
                try: 
                    (factory, conn) = Utils.getRemoteConnection(ringInfo.predecessorLocation, self.metricsMessageCounter)
                    nodeRef = yield factory.getRootObject()
                    connectedToPred = True
                except (error.ConnectionRefusedError, error.TimeoutError, error.ConnectError) as e:
                    connectedToPred = False
                except Exception, e: 
                    connectedToPred = False
                    log.err(e, "Error in checkPredecessor(2).")
                
                # Now we will try to have them connect back to us
                if connectedToPred:
                    # Now try to have them connect back to me!
                    (predConnectedToMe, _incomingConnection) = yield nodeRef.callRemote("isConnected", self.nodeLocation, None, False)
                    
                    if not predConnectedToMe:
                        # Re-check my own IP...Pred is alive, but couldn't connect to me :-(
                        self._revalidateIP(ringInfo) 
                else:
                    # Connect to Pred failed.
                    glog.debug("Pred location for node[%s] changed from [%s] to None" % (self.nodeLocation.port, ringInfo.predecessorLocation), system='checkPredecessor')
                    ringInfo.predecessorLocation = None        
                                
                # Close the connection
                Utils.disconnect(None, conn)
        except Exception, e:
            log.err(e, "Error in checkPredecessor. Continuing anyway.")
        
    def _revalidateIP(self, ringInfo):
        '''This means our IP seems to no longer be working. We should try and get another one.'''
        
        if self.allowFloatingIP:
            # First store the old one
            oldIP = self.nodeLocation.ip
            
            # Now get a new IP
            self.nodeLocation.ip = NetworkUtils.getNonLoopbackIP()
            
            isWorking = self._checkIp(ringInfo)
            
            if not isWorking:
                log.err("Cannot find an IP where people can connect to me. Trying again later.", system="_revalidateIP")
                
                # Go back to the old one just in case.
                self.nodeLocation.ip = oldIP
            elif self.nodeLocation.ip != oldIP:
                glog.debug("IP changed from [%s] to [%s]" % (oldIP, self.nodeLocation.ip), system="_revalidateIP")
                
        else:
            # We aren't allowed to change the IP, just check the current one for informational purposes 
            self._checkIp(ringInfo)
            
        
        
    def remote_ping(self):
        return "PONG"
    
    def remote_getSuccessorLocation(self, enclaveId):
        
        return self.rings[enclaveId].successorList.getSuccessor()
    
    def remote_getSuccessorLocations(self):
        '''Return a list of all my successors in all enclaves I'm part of.'''
        succLocs = []
        for ring in self.rings.values():
            succLocs.append(ring.successorList.getSuccessor())
        return succLocs
    
    
        
        
       
    def remote_closestPrecedingFinger(self, nodeId, enclaveId):
        '''Find the finger that is closest to the given node ID. Return it's node location
        '''
        
        ringInfo = self.rings[enclaveId]
        for currentFinger in reversed(ringInfo.finger[1:]):
            
            if currentFinger.nodeLocation is None:
                continue
            
            if self.inInterval(currentFinger.nodeLocation.id, self.nodeLocation.id, nodeId):
                return currentFinger.nodeLocation

        # Check successor
        mySuccessor = ringInfo.successorList.getSuccessor()
        if mySuccessor != None and self.inInterval(mySuccessor.id, self.nodeLocation.id, nodeId):
            return mySuccessor
        
        return self.nodeLocation

        
        
    @defer.inlineCallbacks
    def partOfEnclave(self, nodeInNetwork, enclaveId):
        '''Determine if the nodeInNetwork knows about the passed in enclaveId. If it doesn't, throw an error'''

        hasEnclaveId = yield nodeInNetwork.callRemote('hasEnclaveId', enclaveId)
        
        if not hasEnclaveId:
            log.err("Bootstrap node does not have enclave ID, so cannot join it under that enclave name")
            raise Exception("Bootstrap node does not have enclave ID, so cannot join it under that enclave name. Fail out!")
            
        # Return ok
        defer.returnValue(nodeInNetwork)
                
    
    @defer.inlineCallbacks
    def checkIfExist(self, nodeInNetwork, enclaveId):
        '''Determine if my Node ID already exists. If it does, throw an error'''
        
        succLoc = yield nodeInNetwork.callRemote("findSuccessorLocation", self.nodeLocation.id, enclaveId)
        
        if succLoc == None:
            log.err("Could not find a successor. This means we are dead but could recover. Fail out! [ID:%s]" % self.nodeLocation.id)
            raise Exception("Could not find a successor. This means we are dead but could recover. Fail out!")
            
        elif succLoc.id == self.nodeLocation.id:
            # Note: We may want to do two other things here..
            # 1. Check is the ID is actually dead, maybe this is the node re-joining?
            # 2. If it really is an ID collision, make a new ID?
            log.err("Trying to join network with ID that already exists. Fail out! [ID:%s]" % self.nodeLocation.id)
            raise Exception("Trying to join network with ID that already exists. Fail out!")
            
        # Return ok
        defer.returnValue(nodeInNetwork)
        
        
    def printCallback(self, val, aStr):
        '''Debug method used to print during callbacks'''
        log.msg(aStr)
        return val
    
    @defer.inlineCallbacks
    def authenticateJoin(self, nodeRef, authenticationPayload):
        '''Check if the node trying to join is allowed to join.'''
        
        if self.messageAuthenticator is None and Config.ALLOW_NO_AUTHENTICATOR:
            if Config.WARN_NO_MESSAGE_AUTHENTICATOR:
                log.msg("Warning: No message authenticator set. Joining without checking.")
            
            #return nodeRef
            defer.returnValue(nodeRef)
        
        (retVal, retPayload) = yield nodeRef.callRemote('authJoin', authenticationPayload)
        self.joinAuthPayload = retPayload # Save it to return later. This feels kludgy!
        
        
        if not retVal:
            log.msg("failed to authenticate. Node cannot join!", system="authenticateJoin")
            
            # Stop listening.
            self.leave()
            
            raise Exception("Node could not authenticate to network.")
        
        defer.returnValue(nodeRef)
    
        
    def remote_authJoin(self, authenticationPayload):
        # This should return a tuple (True, returnPayload)
        return self.messageAuthenticator.authenticateJoin(authenticationPayload)
        
        
    @defer.inlineCallbacks
    def joinNetwork(self, ringInfo, authenticationPayload):
        '''Join the chord network.
        
            Three cases are present, if 
            - I'm the only node and a bootstrap node -- okay
            - I'm the only node and not a bootstrap node -- fail, can't authenticate
            
            - Other nodes exists - authenticate and join.
        '''           
        
        '''Get the answer from another node, if any are present.'''
        (bootstrapNode, factory, conn, nodeLocation) = yield Utils.getRemoteConnectionFromList(ringInfo.bootstrapNodeLocations, self.nodeLocation, metricsMessageCounter=self.metricsMessageCounter)
        
        glog.debug("Tried to connect to bootstrap. [%s][%s][%s][%s]"  % (bootstrapNode, factory, conn, ringInfo.bootstrapNodeLocations))
        
        if bootstrapNode is not False: # We made a connection!
            try:
                yield self.authenticateJoin(bootstrapNode, authenticationPayload)
                yield self.partOfEnclave(bootstrapNode, ringInfo.enclaveId)
                yield self.checkIfExist(bootstrapNode, ringInfo.enclaveId)
                yield self.init_finger_table(bootstrapNode, ringInfo)
                yield Utils.disconnect(None, conn) # Disconnect when we're all done
                
                # Don't wait for this to be done... it's okay to do async.
                #self.update_others(None, ringInfo.enclaveId) # Don't update others until init is done
                  
            except Exception, e:
                log.err("There was a caught error in joinNewtork [%s]" % e)
                Utils.disconnect(None, conn)
                raise
                
            defer.returnValue(True)
        else:
            # No connection to another bootstrap node.        
            if ringInfo.isBootstrapNode:
                # We're okay, just couldn't talk to another bootstrap node.
                log.msg("No other bootstrap nodes to authenticate me during join.")

                # We're the only node (bootstrap node)
                for i in range(FingerEntry.m):
                    ringInfo.finger[i].nodeLocation = self.nodeLocation
                    ringInfo.finger[i].predecessorLocation = self.nodeLocation
                    ringInfo.successorList.addValues(self.nodeLocation)
                
                ringInfo.predecessorLocation = self.nodeLocation
                
                # Make sure we return a defered
                defer.returnValue(True)
            else:
                # We couldn't connect to a bootstrap node, and we arren't one!
                log.err("Could not connect to a bootstrap node, and we are not one. Fail to join.")
                defer.returnValue(False)
        
        
    @defer.inlineCallbacks                        
    def update_others(self, _=None, enclaveId=None):
        '''Look through all the finger entries that should point to me.
           Check that they do.
        '''
        if enclaveId is None:
            raise Exception("update_others cannot have an empty enclaveId!")
        
        completedIDs = []
        
        #glog.debug("DEBUG: update_others NodeId:%s" % self.nodeLocation.id)
        for ind in range(FingerEntry.m):
            try:
                i = ind+1
                
                nodeId = (self.nodeLocation.id - (2**i) ) % 2**FingerEntry.m
                glog.debug("update_others: call findPred..NodeID:%s" % nodeId, system=self.nodeLocation)
                
                (predNodeRef, predNodeLoc, conn) = yield self._findPredecessor(nodeId, self.rings[enclaveId])
                
                glog.debug("update_others: call findPred..DONE", system=self.nodeLocation)
                if predNodeRef is None or predNodeLoc.id in completedIDs:
                    glog.debug("update_others: SKIPPING!", system=self.nodeLocation)

                    pass
                elif predNodeRef == self:
                    # I am my own predecessor --- empty chord ring?
                    glog.debug("update_others: call updateFinger local..", system=self.nodeLocation)
                    yield self.remote_updateFingerTable(self.nodeLocation, ind, enclaveId)
                else:
                    #glog.debug("DEBUG: [nID:%s] pred of nodeId[%d] is [%s] [i=%d]" % (self.nodeLocation.id, nodeId, predNodeRef.id, i))
                    glog.debug("update_others: call updateFinger remote..", system=self.nodeLocation)
                    yield predNodeRef.callRemote("updateFingerTable", self.nodeLocation, ind, enclaveId)            
                
                # So we don't process it again
                completedIDs.append(predNodeLoc.id)
                
                glog.debug("update_others: call updateFinger .. DONE", system=self.nodeLocation)
                # Close the connection before continuing
                if conn is not None:
                    glog.debug("update_others: call disconnect ..", system=self.nodeLocation)
                    yield Utils.disconnect(None, conn)
            except Exception, e:
                log.err(e, "Error in update_others. Continuing though.", system=self.nodeLocation)
                
        #glog.debug("DEBUG: update_others NodeId:%s END" % self.nodeLocation.id)

            
    @defer.inlineCallbacks                        
    def remote_updateFingerTable(self, sLocation, startIndex, enclaveId):
        '''There is a new successor node at location sLocation. Does it fit in finger table slot i ? 
        '''
        #glog.debug("DEBUG: updateFingerTable I[%d] MyID[%d] NewID[%d] interval(%d, %d)" % (i, self.nodeLocation.id, sLocation.id, self.finger[i].start, self.finger[i].nodeLocation.id))

        calledPred = False # Have we called the predecessor yet?
        
        try:
            ringInfo = self.rings[enclaveId]

            # Loop through all the indexes to try them all and update when needed.
            for i in range(startIndex, len(ringInfo.finger)):
                if self.inIntervalInclusive(sLocation.id, ringInfo.finger[i].start,ringInfo.finger[i].nodeLocation.id ):
                    #glog.debug("DEBUG: updateFingerTable for %s: Fin[%d](start:%d) --> Node[%d]" % (str(self), i, self.finger[i].start, sLocation.id))
        
                    # Yes... update my finger table index i to point to the new node
                    ringInfo.finger[i].nodeLocation = copy.deepcopy(sLocation)
                    
                    # Should my pred also point to it?
                    if ringInfo.predecessorLocation is not None and not calledPred:
                        (factory, conn) = Utils.getRemoteConnection(ringInfo.predecessorLocation, self.metricsMessageCounter)
                        predRef = yield factory.getRootObject()
                            
                        yield predRef.callRemote("updateFingerTable", sLocation, i, enclaveId)
                    
                        # Close connection to predRef
                        yield Utils.disconnect(None, conn)
                        
                        calledPred = True # Don't do this again!
                
        except error.ConnectionRefusedError:
            log.err("Connection refused in remote_updateFingerTable [%s]. Continuing." % str(ringInfo.predecessorLocation))
        except Exception, e:
            log.err(e, "Could not complete updateFingerTable.")
        # Nothing to return
        defer.returnValue(None)

                
              
    @defer.inlineCallbacks  
    def stabilize(self, ringInfo):
        '''called periodically. n asks the successor about its predecessor, verifies if n's immediate 
           successor is consistent, and tells the successor about n
        '''
        conn = None
        conn2 = None
        
        # Ask my successor for it's pred
        try:
            # Get a ref to my successor
            (currentSuccessor, succ, conn) = yield ringInfo.successorList.getLiveSuccessor()

            if currentSuccessor is None:
                log.err("Node has no successors. Hopefully rebuild will fix. [Node:%s]" % str(self.nodeLocation))
                return
            elif currentSuccessor == self.nodeLocation:
                successorsPred = self.remote_getPredLocation(ringInfo.enclaveId)
            else:
                # Get who my successor thinks is it's predecessor
                successorsPred = yield succ.callRemote("getPredLocation", ringInfo.enclaveId)
            
            # If I'm the successor, then we're all consistent -- get out!
            if successorsPred is None or successorsPred.id != self.nodeLocation.id:
                # if the result *is* my successor, set it
                if successorsPred is not None and \
                   self.inInterval(successorsPred.id, self.nodeLocation.id, ringInfo.successorList.getSuccessor().id):
                    yield ringInfo.successorList.addValues(successorsPred)
                
                # Now notify my successor that I'm it's pred. 
                (_, newSuccessor, conn2) = yield ringInfo.successorList.getLiveSuccessor()
                if newSuccessor != None:
                    yield newSuccessor.callRemote("notify", self.nodeLocation, ringInfo.enclaveId)
        except error.ConnectionRefusedError, e:
            #log.msg("Connection refused in stabilize. Continuing.[%s]" % e)
            log.err(e,"Connection refused in stabilize. Continuing" )
        except error.TimeoutError:
            log.msg("Timeout in stabilize. Continuing.")
        except error.ConnectError:
            log.msg("ConnectError in stabilize. Continuing.")
        except Exception,  e:
            log.err("Error in stabilize! [%s]" % e)
            traceback.print_exc()
        finally:
            # Close the connection
            if conn is not None:
                Utils.disconnect(None, conn)
            if conn2 is not None:
                Utils.disconnect(None, conn2)
                
    def remote_stabilize(self, enclaveID):
        '''My successor's pred changed, so maybe I should to?'''
        
        if enclaveID in self.rings:
            self.stabilize(self.rings[enclaveID])
        
        
        
    def remote_notify(self, nPrimeLocation, enclaveId):
        '''nPrime thinks it might be our predecessor'''
        
        #glog.debug("%s: %s thinks its my PRED" % (self.nodeLocation, nPrimeLocation))
        #glog.debug("%s notified that pred could be:ID: %s " % (self, nPrimeLocation.id))
        ring = self.rings[enclaveId]
        if ring.predecessorLocation is None or self.inInterval(nPrimeLocation.id, ring.predecessorLocation.id, self.nodeLocation.id):
            origPredLoc = ring.predecessorLocation

            ring.predecessorLocation = nPrimeLocation
            glog.debug("Pred location for node[%s] changed from %s to %s" % (self.nodeLocation.port,origPredLoc, ring.predecessorLocation.port), system='remote_notify')
    
            self.callStabilize(origPredLoc, enclaveId)
            
    @defer.inlineCallbacks
    def callStabilize(self, origPred, enclaveId):
        # Now, tell me orig predecessor, they may want to check and see if they should update their successor now.
        if origPred is not None:
            try:
                # Connect to my pred
                (factory, conn) = Utils.getRemoteConnection(origPred, self.metricsMessageCounter)
                origPrefRef = yield factory.getRootObject()
    
                yield origPrefRef.callRemote("stabilize", enclaveId)
                if conn is not None:
                    # Close the connection to nPrimeLocation
                    Utils.disconnect(None, conn)
                
            except Exception, e:
                log.msg("Attempted to tell stabilize my old predecessor, but cannot.[%s][%s] Okay. [%s]" % (origPred, enclaveId, e))
                log.err()
        

    
    @defer.inlineCallbacks
    def fixFingers(self, ringInfo):
        '''Periodically called to fix up all the fingers. 
           Runs through each sequentially.
        '''
        
        changed = True # Did we update a finger loc?
        
        # Wrap in Try/Except so any errors won't stop processing the repeating calls.
        try:
            
            origLoc = ringInfo.finger[self.nextFingerToFix].nodeLocation

            #glog.debug("fixFingers Finger:%d  For node:%s" % (self.nextFingerToFix, self))
            # Now fix the finger (or at least check it!)
            newLoc = yield self.remote_findSuccessorLocation(ringInfo.finger[self.nextFingerToFix].start, ringInfo.enclaveId)
            
            if newLoc != origLoc:
                ringInfo.finger[self.nextFingerToFix].nodeLocation = newLoc
                changed = True
            
            # Now see if the new finger should be applied to all the other finger locations.
            while changed:
                # Keep checking if the next fingers should also be changed
                self._updateNextFingerToFix()  
                
                # Change the finger if it's better.
                if self._isBetterFinger(newLoc, self.nextFingerToFix, ringInfo):
                    ringInfo.finger[self.nextFingerToFix].nodeLocation = newLoc
                else:
                    changed = False
           
            
            
        except Exception, e:
            log.err(e, "Exception in fixFingers")
        
    def _isBetterFinger(self, newLoc, fingerIndex, ringInfo):
        '''Is the newLoc a better fit for finger location fingerIndex?'''
        
        if ringInfo.finger[fingerIndex].nodeLocation is None:
            return True
        elif newLoc is None:
            return False
        
        
        return self.inIntervalInclusive(newLoc.id, ringInfo.finger[fingerIndex].start,ringInfo.finger[fingerIndex].nodeLocation.id )
                    
                    
        
        
            
    @defer.inlineCallbacks                        
    def _findPredecessor(self, nodeId, ringInfo):
        '''Find what I think is the predecessor of the passed in NodeID.
           This method also returns an open connection which should be closed
           after the pred reference is used.
        '''
        conn = None
        nPrimeRef = self
        nPrimeLocation = self.nodeLocation
        nPrimeSuccessorLocation = ringInfo.successorList.getSuccessor()
        
        # Check if self is the pred        
        found=self.inIntervalInclusiveRight(nodeId, nPrimeLocation.id, nPrimeSuccessorLocation.id)

        # While the nodeId is not between me and my successor
        while not found:
            if nPrimeRef == self:
                nPrimeLocation = self.remote_closestPrecedingFinger(nodeId, ringInfo.enclaveId)
            else:
                nPrimeLocation = yield nPrimeRef.callRemote("closestPrecedingFinger", nodeId, ringInfo.enclaveId)
                
                # Close the previous ref to nPrimeRef since we don't need it anymore
                if conn is not None:
                    # Close the connection to nPrimeLocation
                    Utils.disconnect(None, conn)
                    
            
            if nPrimeLocation == self.nodeLocation:
                nPrimeSuccessorLocation = ringInfo.successorList.getSuccessor()
            else:
                (factory, conn) = Utils.getRemoteConnection(nPrimeLocation, self.metricsMessageCounter)
                nPrimeRef = yield factory.getRootObject()
    
                nPrimeSuccessorLocation = yield nPrimeRef.callRemote("getSuccessorLocation", ringInfo.enclaveId)
                if nPrimeSuccessorLocation is None:
                    glog.debug("_findPredecessor no successor for %s" % str(nPrimeLocation)) # Network not stable enough right now.
                    # Close the previous ref to nPrimeRef since we don't need it anymore
                    if conn is not None:
                        # Close the connection to nPrimeLocation
                        Utils.disconnect(None, conn)
                    defer.returnValue( (None, None, None) )
                
            found=self.inIntervalInclusiveRight(nodeId, nPrimeLocation.id, nPrimeSuccessorLocation.id)
            
                
        #glog.debug("Closest finger to node [%s] is [%s] who has successor [%s]" % (nodeId, nPrimeLocation.id, nPrimeSuccessorLocation.id))    
    
        defer.returnValue( (nPrimeRef, nPrimeLocation, conn) )
        
    def remote_getNodeLocation(self):
        '''Get my location... this isn't really needed, but helpful.'''
        return self.nodeLocation
    
    def remote_getPredLocation(self, enclaveId):
        '''Get my predecessor'''
        return self.rings[enclaveId].predecessorLocation
    
    
    def remote_getPredLocations(self):
        '''Return a list of all my predecessors in all enclaves I'm part of.'''
        predLocs = []
        for ring in self.rings.values():
            predLocs.append(ring.predecessorLocation)
        return predLocs
        
    
        
    def remote_setPredLocation(self, predLocation, enclaveId):
        '''Set my predecessor'''
        
        ringInfo = self.rings[enclaveId]
        orig = ringInfo.predecessorLocation
        ringInfo.predecessorLocation = predLocation
        glog.debug("Pred location for node[%s] changed from [%s] to [%s] " % (self.nodeLocation.port, orig, ringInfo.predecessorLocation.port), system='setPredLocation')
        
        # If it changed, do a quick stabilize call to set my successor up right
        # This helps make the tests run faster... not officially required though.
        if orig != ringInfo.predecessorLocation:
            self.stabilize(ringInfo)
            self.callStabilize(orig, enclaveId)


    @defer.inlineCallbacks
    def setupOtherFingerNodes(self, nPrimeRef, ringInfo):
        # Now make sure to set all the other nodes correctly
        for i in range(FingerEntry.m-1):

            if ringInfo.finger[i+1].start >= self.nodeLocation.id and ringInfo.finger[i+1].start < ringInfo.finger[i].nodeLocation.id:
                ringInfo.finger[i+1].nodeLocation = copy.deepcopy(ringInfo.finger[i].nodeLocation)
            else:
                succLoc = yield nPrimeRef.callRemote("findSuccessorLocation", ringInfo.finger[i+1].start, ringInfo.enclaveId)
                if succLoc is None:
                    # We couldn't connect to the successor for some reason... so don't use it!
                    ringInfo.finger[i+1].nodeLocation = copy.deepcopy(ringInfo.finger[i].nodeLocation)
                else:
                    ringInfo.finger[i+1].nodeLocation = succLoc
                    
             
            
    @defer.inlineCallbacks                        
    def init_finger_table(self, nPrimeRef, ringInfo):
        '''Initialize the finger table for me using nPrimeRef to help.'''
        
        try:
            # Find my successor, and set it in the finger table
            successorLocation = yield nPrimeRef.callRemote("findSuccessorLocation", ringInfo.finger[0].start, ringInfo.enclaveId)
            ringInfo.successorList.addValues(successorLocation)
            
            
            # Now get the predecessor location from my successor and use it as my pred
            (factory, conn) = Utils.getRemoteConnection(successorLocation, self.metricsMessageCounter)
            successorNodeRef = yield factory.getRootObject()
            
            predLocation = yield successorNodeRef.callRemote("getPredLocation", ringInfo.enclaveId)
            ringInfo.predecessorLocation = predLocation
            
            # Now setup me as the pred of my successor
            _ = yield successorNodeRef.callRemote("setPredLocation", self.nodeLocation, ringInfo.enclaveId)
             
            # Now make sure to set all the other nodes correctly
            yield self.setupOtherFingerNodes(nPrimeRef, ringInfo)
            
            # Now close out the connection to successorNode
            Utils.disconnect(None, conn)
        except Exception, e:
            log.err("Error in init_finger_table: [%s]" % e)
                        
    def inInterval(self, val, left, right):
        '''Decide if the given val is in the interval from left to right
           val E (left, right)
        '''
        
        # Everything in the circle, except bounds
        if left == right:
            return val != left
        
        if left < right: # Normal case
            return val > left and val < right
        
        # Final case, we crossed the zero boundary
        return  val > left or val < right
        
    def inIntervalInclusive(self, val, left, right):
        '''Decide if the given val is in the interval from left to right including == left
           val E [left, right)
        '''
        
        # Everything in the circle, except bounds
        if left == right:
            return True
        
        if left < right: # Normal case
            return  left <= val < right
        
        # Final case, we crossed the zero boundary
        return  val >= left or val < right

    def inIntervalInclusiveRight(self, val, left, right):
        '''Decide if the given val is in the interval from left to right including == right
           val E (left, right]
        '''
        
        # Everything in the circle, except bounds
        if left == right:
            return True
        
        if left < right: # Normal case
            return  left < val <= right
        
        # Final case, we crossed the zero boundary
        return  val > left or val <= right
                
    def printFingerTable(self, _=None):
        log.msg("------------------------")
        log.msg("FINGER TABLE FOR NODE:%s" % str(self))
        for f in self.finger:
            log.msg(f)
        log.msg("------------------------")    
        
    def __str__(self):
        theStr = ''
        
        for ring in self.rings.values():
            
            if ring.predecessorLocation is None:
                pred = None
            else:
                pred = ring.predecessorLocation.id
                
                
            if ring.successorList.getSuccessor() is None:
                succ = None
            else:
                succ = ring.successorList.getSuccessor().id
            
            theStr += "Enclave: %s ID:%s   PRED:%s   SUCC:%s  " % (ring.enclaveId, self.nodeLocation.id, pred, succ)
        return theStr
    def __repr__(self):
        return self.__str__()  
    
