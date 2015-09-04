'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Tests the APIs we provide and the client api they provide to us.

Created on Sept 5, 2014

@author: dfleck
'''

from twisted.trial import unittest
from twisted.internet import task, defer
from twisted.python import log, failure


from gmu.chord import NetworkUtils, Config
from gmu.chord.NodeLocation import NodeLocation
from gmu.chord.MetricsMessageObserver import MetricsMessageObserver

from gmu.netclient.classChordNetworkChord import classChordNetworkChord

# Testing Modules
from ConnectivityCounter import ConnectivityCounter
from SampleClient import SampleClient
import TestUtils
import sys, random
from TestMessageObserver import TestMessageObserver


numNodes = 5 # Number of nodes per enclave
numMessages=5   # Total number of messages to send to each node
startingPort = 12350


class APITest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(APITest, cls).setUpClass()
        APITest.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(APITest, cls).tearDownClass()
        if APITest.logObs is not None:
            APITest.logObs.stop()
        
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''

        # Turn ON warning for this test!
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = True
        Config.ALLOW_NO_AUTHENTICATOR = False

        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        log.msg('Got IP: %s:%s' % (self.myIP, type(self.myIP)))

        return
        
    
    def buildNetwork(self):
        global startingPort

        d = defer.Deferred()
        
        # Create Bootstrap
        port = 12345
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []

        # Build the client and network objects
        enclaveStr = 'localhost'
        self.bsClient = SampleClient(self.myIP, port, None)
        self.bsNetwork = classChordNetworkChord(self.bsClient, port, self.myIP)
        bsID = self.bsNetwork.generateNodeID(str(port), enclaveStr) # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests

        
        # Join the network
        callFunc = lambda x, payload: self.shouldSucceedCallback(x, payload, d, self.bsNetwork)
        self.bsNetwork.start(callFunc, bsID, enclaveStr, "authenticate:succeed", None, True, True)
        
        for i in range(numNodes):
            # Start a client node
            d.addCallback(self.startClientNode, self.myIP, 12350+i, bootstrapNodeLocation, enclaveStr)
    

        # Wait for flooding to reach all the nodes
        d.addCallback(self.waitForConnectivity, numNodes+1) # Does not count bsNode itself.

        return d
    

    @defer.inlineCallbacks
    def testJoinEnclaveAPI(self):
        '''Create a node, then have it join another enclave... do this for both a bootstrap node, and 
           a client node.
        '''
        global startingPort
        
        # Create Bootstrap
        port = 12345
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        bootstrapNodeList = [ bootstrapNodeLocation ]
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []

        # Build the client and network objects
        enclaveStr1 = 'enclave1'
        enclaveStr2 = 'enclave2'
        self.bsClient = SampleClient(self.myIP, port, None)
        self.bsNetwork = classChordNetworkChord(self.bsClient, port, self.myIP)
        bsID = self.bsNetwork.generateNodeID(str(port), enclaveStr1) # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests

        
        # Join the network
        log.msg("---- Bootstrap Join 1 ---- ", system="testJoinEnclaveAPI")
        d = defer.Deferred()
        callFunc = lambda x, payload: self.shouldSucceedCallback(x, payload, d, self.bsNetwork)
        self.bsNetwork.start(callFunc, bsID, enclaveStr1, "authenticate:succeed", None, True, True)
        yield d  # Wait for join to succeed
        
        # Now have the bootstrap node join another enclave
        log.msg("---- Bootstrap Join 2 ---- ", system="testJoinEnclaveAPI")
        d = defer.Deferred()
        callFunc2 = lambda x, payload: self.shouldSucceedCallback(x, payload, d, self.bsNetwork)
        self.bsNetwork.joinEnclave([ (bootstrapNodeLocation.ip, bootstrapNodeLocation.port) ] , True, "authenticate:succeed", enclaveStr2, callFunc2, True)
        yield d
        
        # Now have a client try to join one of the enclaves
        log.msg("---- Start Client 1 ---- ", system="testJoinEnclaveAPI")
        yield self.startClientNode(None, self.myIP, 12350+1, bootstrapNodeLocation, enclaveStr1)
        
        # Have another client join the other enclave
        log.msg("---- Start Client 2 ---- ", system="testJoinEnclaveAPI")
        yield self.startClientNode(None, self.myIP, 12350+2, bootstrapNodeLocation, enclaveStr2)
        
        # Do a bit of verification
        rc = yield self.bsNetwork.isConnected("ANY")
        self.assertTrue(rc != False, "isConnected returned a False value instead of enclave ID")

        rc = yield self.bsNetwork.isConnected(enclaveStr1)
        self.assertTrue(rc != False, "isConnected returned a False value instead of enclave ID")

        rc = yield self.bsNetwork.isConnected(enclaveStr2)
        self.assertTrue(rc != False, "isConnected returned a False value instead of enclave ID")

        rc = yield self.bsNetwork.isConnected("NOTREAL")
        self.assertTrue(rc == False, "isConnected returned True value instead of False!")

        # Now shut everything down
        for (_clientAPI, networkAPI) in self.allNodes:
            yield networkAPI.disconnect()
            
        yield self.bsNetwork.disconnect()
        
        # Now wait for the network cache disconnect timeout
        yield TestUtils.wait(Config.CONNECTION_CACHE_DELAY + 1) 
        
        defer.returnValue(True)
                
    @defer.inlineCallbacks
    def testDefaultEnclaveAPI(self):
        '''Create a node which should default to localhost as the enclave name.  '''
        global startingPort
        
        # Create Bootstrap
        port = 12345
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []

        # Build the client and network objects
        enclaveStr1 = None
        self.bsClient = SampleClient(self.myIP, port, None)
        self.bsNetwork = classChordNetworkChord(self.bsClient, port, self.myIP)
        bsID = self.bsNetwork.generateNodeID(str(port), "WHO CARES") # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests

        
        # Join the network
        log.msg("---- Bootstrap Join 1 ---- ", system="testDefaultEnclaveAPI")
        d = defer.Deferred()
        callFunc = lambda x, payload: self.shouldSucceedCallback(x, payload, d, self.bsNetwork)
        self.bsNetwork.start(callFunc, bsID, enclaveStr1, "authenticate:succeed", None, True, True)
        yield d  # Wait for join to succeed


        # Now check that the node is part of localhost
        enclave = yield self.bsNetwork.findEnclave(self.myIP, port)
        self.assertEqual(enclave, "localhost", "testDefaultEnclaveAPI did not get localhost as enclave!")
        
        # Now create a client node and have it ask the bootstrap node for the enclave
        log.msg("---- Start Client 1 ---- ", system="testIPFindingAPI")
        yield self.startClientNode(None, None, 12350+1, bootstrapNodeLocation, None)
        
        # Check that it got enclave localhost
        enclave = yield self.allNodes[0][1].findEnclave(self.myIP, 12350+1)
        self.assertEqual(enclave, "localhost", "testDefaultEnclaveAPI: client did not get localhost as enclave!")

        
        # Now shut everything down
        for (_clientAPI, networkAPI) in self.allNodes:
            yield networkAPI.disconnect() 
        
        yield self.bsNetwork.disconnect()
        
        # Now wait for the network cache disconnect timeout
        yield TestUtils.wait(Config.CONNECTION_CACHE_DELAY + 1) 
        
        defer.returnValue(True)
                
                            
    @defer.inlineCallbacks
    def testIPFindingAPI(self):
        '''Create a node and ask the system to figure out it's IP from a bootstrap node.  '''
        global startingPort
        
        # Create Bootstrap
        port = 12345
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []

        # Build the client and network objects
        enclaveStr1 = "AnEnclave"
        myIP = None # self.myIP
        self.bsClient = SampleClient(myIP, port, None)
        self.bsNetwork = classChordNetworkChord(self.bsClient, port, myIP)
        bsID = self.bsNetwork.generateNodeID(str(port), "WHO CARES") # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests

        
        # Join the network
        log.msg("---- Bootstrap Join 1 ---- ", system="testDefaultEnclaveAPI")
        d = defer.Deferred()
        callFunc = lambda x, payload: self.shouldSucceedCallback(x, payload, d, self.bsNetwork)
        self.bsNetwork.start(callFunc, bsID, enclaveStr1, "authenticate:succeed", None, True, False)
        yield d  # Wait for join to succeed

        # Now create a client node which will get it's IP from the bootstrap node
        log.msg("---- Start Client 1 ---- ", system="testIPFindingAPI")
        yield self.startClientNode(None, None, 12350+1, bootstrapNodeLocation, enclaveStr1)
        
        # Do a bit of verification
        # Now shut everything down
        for (_clientAPI, networkAPI) in self.allNodes:
            rc = yield networkAPI.isConnected("ANY")
            self.assertTrue(rc != False, "isConnected returned a False value in testIPFindingAPI")


        # Now shut everything down
        for (_clientAPI, networkAPI) in self.allNodes:
            yield networkAPI.disconnect() 
        yield self.bsNetwork.disconnect()
        
        # Now wait for the network cache disconnect timeout
        yield TestUtils.wait(Config.CONNECTION_CACHE_DELAY + 1) 
        
        defer.returnValue(True)
                                    
        
        
    def waitForConnectivity(self, _, numNodes):
        ''' Wait for numNodes to be connectable from the Bootstrap Node passed in.
            Returns a deferred which fires upon all nodes connecting.
        '''
        waiter = ConnectivityCounter()
        return waiter.waitForConnectivity(numNodes, self.bsNetwork.chordNode)

        
        
    def startClientNode(self, _, ip, port, bootstrapNodeLocation, enclaveStr):
        '''Return a deferred which should fire after join succeeds. 
           
           Order of events is:
              After the callback from networkAPI.start happens, then we do a few things and then
              this code fires d.callback() (using the "d" which is returned from this method.
        '''
        d = defer.Deferred()
        
        clientAPI = SampleClient(ip, port, None)
        networkAPI = classChordNetworkChord(clientAPI, port, ip)

        nodeID = networkAPI.generateNodeID(str(port), enclaveStr) # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests
        
        # Join the network
        bootstrapNodeList = [ NodeLocation(None, bootstrapNodeLocation.ip, bootstrapNodeLocation.port ) ]
        callFunc = lambda result, payload: self.shouldSucceedCallback(result, payload, d, networkAPI)
        networkAPI.start(callFunc, nodeID, enclaveStr, "authenticate:succeed", bootstrapNodeList, False, True)
        
        # Store a tuple for each node
        self.allNodes.append(  (clientAPI, networkAPI) )
        
        return d

    def shouldSucceedCallback(self, result, payload,  deferToFire, networkAPI):
        '''Status callback from the networkAPI.start call. Should be true.
           Uses a lambda function so we can sneak in a few more parameters :-)
        '''
        self.failUnless(result == True, 'Should succeed callback got : %s ' % result)
        
        self.allMetricsObservers.append(MetricsMessageObserver(networkAPI.chordNode)) # Register an observer now
        self.allTestObservers.append(TestMessageObserver(networkAPI.chordNode, networkAPI))


        deferToFire.callback(True) # We are join complete.. fire our own callback.
        
        
    @defer.inlineCallbacks
    def testAggregationMessageAPI(self):
        '''
        By the time this function is called, we should have a full network built and connected.
        
        Tests to perform now:
            1. Send P2P messages from BS Node to all nodes
            2. Send P2P messages from all Nodes to BSNode
            3. Send flooding from BSNode to all
            4. Send query and return aggregation responses of different levels

        Finally, do cleanup.
        '''
        try:
    
            # Build a network
            yield self.buildNetwork()
         
            # 1. Send P2P messages from BS Node to all nodes
            yield self.p2pFromBStoAll()
            
            
            # 2. Send P2P messages from all Nodes to BSNode
            yield self.p2pFromAlltoBS() 
            
            # 3. Send flooding from BSNode to all
            yield self.floodFromBStoAll()
    
            # 4. Send query and return aggregation responses of different levels
            yield self.getAggResponse()
         
            # Do the cleanup
            yield TestUtils.wait(5)
            
            # Shut it all down
            for (_clientAPI, networkAPI) in self.allNodes:
                yield networkAPI.disconnect()
                
            yield self.bsNetwork.disconnect()
            
            # Now wait for the network cache disconnect timeout
            yield TestUtils.wait(Config.CONNECTION_CACHE_DELAY + 5) 
        except Exception, e:
            log.err(e, "An error occurred in testAggregationMessageAPI")
        
        defer.returnValue(True)
        
    @defer.inlineCallbacks
    def getAggResponse(self):
        '''Send a query and get an agg response from all the nodes.'''

        bsNode = self.bsNetwork.chordNode
        
        
        for aggNumber in range(1, numNodes + 5):
            # Reset the message counter
            obs = self.getObserver(bsNode, self.allTestObservers)
            obs.resetMessageCount()
                                           
            # Send the query asking for a response        
            messageNum = aggNumber # Just to keep it incrementing
            yield TestUtils.sendFlood(bsNode,messageNum,'localhost', data="SEND_AGG_RESPONSE:%d" % aggNumber)
            
            
            # Now check how many messages the bootstrap node got in return
            for _ in range(10): # 10 seconds
                recvCount = obs.getMessageCount()
    
                if recvCount >= numNodes+1:
                    break
                else:
                    yield TestUtils.wait(1)
            
            self.failUnless(recvCount == numNodes+1, "getAggResponse didn't get the correct number of messages back.Expected[%d] Got[%d]" % (numNodes+1, recvCount))
        
        defer.returnValue(True)
        
    @defer.inlineCallbacks        
    def floodFromBStoAll(self):
        '''Send flood messages from BSNode to all other nodes. Verify receipt.'''
        
        # Reset the pingback counter
        obs = self.getObserver(self.bsNetwork.chordNode, self.allMetricsObservers)
        obs.resetPingbackCount()
        
        # Send a flooding message
        d = defer.Deferred()
        message = { "type" : "PING", "loc" : self.bsNetwork.chordNode.nodeLocation, "msgNum" : 99 }

        fu = lambda x: self.sendResult(x, d, True, 'Flood message from %s send failed' %  self.bsNetwork.chordNode.nodeLocation)
        self.bsNetwork.sendMessage(message, 'flood', fu)
     
        # Wait for it... this just waits for the send to be complete!
        yield d   

        
        # Now check that all the nodes actually got it.
        status = yield self.waitForMessageCount(obs, numNodes)
        self.failUnless(status, 'Bootstrap did not receive pingback messages from all nodes. Expected[%d]  Actual[%d]' % (numNodes, obs.getPingbackCount()))
        
        # Done            
        defer.returnValue(True)
             
    @defer.inlineCallbacks 
    def waitForMessageCount(self, obs, numMessagesToWaitFor):
        
        for _ in range(10):
            if obs.getPingbackCount() >= numMessagesToWaitFor:
                defer.returnValue(True)
            else:
                yield TestUtils.wait(1)
                
        defer.returnValue(False)
            
        
          
    def getObserver(self, node, allObservers):
        '''Find the metrics message observer for this node.'''
        for obs in allObservers:
            if node == obs.chordNode:
                return obs
        
        return None
        
        
    @defer.inlineCallbacks        
    def p2pFromBStoAll(self):
        '''Send P2P messages from BSNode to all other nodes. Verify receipt.'''
        
        
        for (_clientAPI, networkAPI) in self.allNodes:
            destNodeLoc = networkAPI.getCurrentNodeLocation()
            
            d = defer.Deferred()
            message = "Test Message"
            fu = lambda x: self.sendResult(x, d, True, 'p2p message from %s -> %s failed.[%s]' %  
                                            (self.bsNetwork.chordNode.nodeLocation, destNodeLoc, x))
            self.bsNetwork.sendMessage(message, 'p2p', fu, destNodeLoc.id)
         
            # Wait for it...
            yield d   

        # Done            
        defer.returnValue(True)
    
    
    @defer.inlineCallbacks        
    def p2pFromAlltoBS(self):
        '''Send P2P messages from all nodes to BSNode nodes. Verify receipt.'''
        
        
        destNodeLoc = self.bsNetwork.getCurrentNodeLocation()
        for (_clientAPI, networkAPI) in self.allNodes:
            
            d = defer.Deferred()
            message = "Test Message"
            fu = lambda x: self.sendResult(x, d, True, 'p2p message from %s -> %s failed.[%s]' %  
                                            (networkAPI.chordNode.nodeLocation, destNodeLoc, x))
            networkAPI.sendMessage(message, 'p2p', fu, destNodeLoc.id)
         
            # Wait for it...
            yield d   

        # Done            
        defer.returnValue(True)
        
            
        
        
    def sendResult(self, status, deferToFire, expectedStatus, failureMessage):
        if isinstance(status, failure.Failure):
            log.err(status) 

        self.failUnless(status == expectedStatus, failureMessage)
               
        deferToFire.callback(status)
        
        
        
        
        
        
    