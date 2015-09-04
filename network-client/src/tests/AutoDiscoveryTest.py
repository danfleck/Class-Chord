'''
Tests for AutoDiscovery code

Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Oct 6, 2014

@author: dfleck
'''

from twisted.trial import unittest
from twisted.internet import task, defer, error
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


class AutoDiscoveryTest(unittest.TestCase):
    
    
    @classmethod
    def setUpClass(cls):
        super(AutoDiscoveryTest, cls).setUpClass()
        AutoDiscoveryTest.logObs = log.startLogging(sys.stdout)
        
        # Turn ON warning for this test!
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = True
        Config.ALLOW_NO_AUTHENTICATOR = True
        
    @classmethod
    def tearDownClass(cls):
        super(AutoDiscoveryTest, cls).tearDownClass()
        if AutoDiscoveryTest.logObs is not None:
            AutoDiscoveryTest.logObs.stop()

    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''
        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        log.msg('Got IP: %s:%s' % (self.myIP, type(self.myIP)))



        
        
    @defer.inlineCallbacks
    def testStartAutoDiscoveryClient(self):
        '''Try to start a bootstrap node with autodiscovery on.
        '''
        
        log.msg("---------------------- BEGIN testStartAutoDiscoveryClient -------------- ")
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []

        # Create Bootstrap
        port = 12345
        enclaveStr = 'testclave'
        bsNodeLocation = NodeLocation(None, self.myIP, port)
        bootstrapNodeList = [ bsNodeLocation ]
        (self.bsClient, self.bsNetwork, d) = self.startBootstrapNode(enclaveStr, self.myIP, port, "authenticate:succeed", bootstrapNodeList)
        yield d
        
        # Now try and join with autodiscovery
        yield self.startClientNodeAutoDiscovery(None, self.myIP, port+1, enclaveStr, bootstrapNodeList)

        # Shut it all down
        for (clientAPI, networkAPI) in self.allNodes:
            yield networkAPI.disconnect()
            
        yield self.bsNetwork.disconnect()     
        
        # Try and wait for connection cache to finish disconnects (just in case).
        yield TestUtils.waitForConnectionCache()        
                
                
        
    @defer.inlineCallbacks
    def testAutoDiscoveryRebootstrap(self):
        '''Try to start a bootstrap node and client with autodiscovery on.
           Then start another bootstrap node.
           Then kill first bootstrap node and have client try to re-bootstrap to
           new bootstrap node.            
        '''
        
        log.msg("---------------------- BEGIN testAutoDiscoveryRebootstrap -------------- ")
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []

        # Create Bootstrap
        port = 12345
        enclaveStr = 'testclave'
        bsNodeLocation = NodeLocation(None, self.myIP, port)
        bootstrapNodeList = [ bsNodeLocation ]

        log.msg("Starting bootstrap... \n\n")
        (self.bsClient, self.bsNetwork, d) = self.startBootstrapNode(enclaveStr, self.myIP, port, "authenticate:succeed", bootstrapNodeList)
        yield d
        
        # Now try and join with autodiscovery
        yield TestUtils.wait(1)
        log.msg("Starting client... \n\n")
        yield self.startClientNodeAutoDiscovery(None, self.myIP, port+1, enclaveStr, bootstrapNodeList)

        # Now start a second bootstrap node
        #NOT DONE YET
        log.msg("Shutting down client... \n\n")
        
        # Shut it all down
        for (clientAPI, networkAPI) in self.allNodes:
            yield networkAPI.disconnect()
            
        yield self.bsNetwork.disconnect()  
                        
        # Try and wait for connection cache to finish disconnects (just in case).
        yield TestUtils.waitForConnectionCache()        
                        
         
    @defer.inlineCallbacks
    def testStartAutoDiscoverySameNodeError(self):
        '''Try to start a bootstrap node and then two clients on the same port with autodiscovery on.        
           Should log an error when the second node attempts to join.
        '''
        
        log.msg("---------------------- BEGIN testStartAutoDiscoverySameNodeError -------------- ")
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []
        
        # Create Bootstrap
        port = 12345
        enclaveStr = 'testclave'
        bsNodeLocation = NodeLocation(None, self.myIP, port)
        bootstrapNodeList = [ bsNodeLocation ]
                
        (self.bsClient, self.bsNetwork, d) = self.startBootstrapNode(enclaveStr, self.myIP, port, "authenticate:succeed", bootstrapNodeList)
        yield d
        
        # Now startup one client which will succeed
        d2 = defer.Deferred()
        port = port + 1
        
        clientAPI = SampleClient(self.myIP, port, None)
        networkAPI = classChordNetworkChord(clientAPI, port, self.myIP)
        nodeID = networkAPI.generateNodeID(str(port), enclaveStr) # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests
        
        # Join the network
        callFunc = lambda result, payload: self.shouldSucceedCallback(result, payload, d2, networkAPI)
        networkAPI.start(callFunc, nodeID, enclaveStr, "authenticate:succeed", bootstrapNodeList, False, True)
        
        yield d2
                
        # Now startup one which should fail due to duplicate port
        d3 = defer.Deferred()
        
        clientAPI2 = SampleClient(self.myIP, port, None)
        networkAPI2 = classChordNetworkChord(clientAPI2, port, self.myIP)
        nodeID = networkAPI.generateNodeID(str(port), enclaveStr) # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests
        
        # Join the network
        callFunc2 = lambda result, payload: self.shouldFailureCallback(result, payload, d3)
        networkAPI2.start(callFunc2, nodeID, enclaveStr, "authenticate:succeed",bootstrapNodeList, False, True)
        
        yield d3
        
        yield networkAPI.disconnect()
        #yield networkAPI2.disconnect()
        yield self.bsNetwork.disconnect()
        
        # Try and wait for connection cache to finish disconnects (just in case).
        yield TestUtils.waitForConnectionCache()        
        
                                
    @defer.inlineCallbacks
    def testStartAutoDiscoveryMultipleClients(self):
        '''Try to start a bootstrap node and the many many clients all at once with autodiscovery on.        '''
        
        log.msg("---------------------- BEGIN testStartAutoDiscoveryMultipleClients -------------- ")
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []
        
        numNodes = 15

        # Create Bootstrap
        port = 12345
        enclaveStr = 'testclave'
        bootstrapNodeList = [ NodeLocation(None, self.myIP, port) ]
        (self.bsClient, self.bsNetwork, d) = self.startBootstrapNode(enclaveStr, self.myIP, port, "authenticate:succeed", bootstrapNodeList)
        yield d
        
        # Now try and join a bunch of nodes with autodiscovery
        allDefs = [] # A list of deferreds
        for counter in range(numNodes):
            allDefs.append(self.startClientNodeAutoDiscovery(None, self.myIP, port+1+counter, enclaveStr, bootstrapNodeList))
            
        dl = defer.DeferredList(allDefs)
        yield dl # Wait for all deferred joins to complete
        
        # Shut it all down
        for (clientAPI, networkAPI) in self.allNodes:
            yield networkAPI.disconnect()
            
        yield self.bsNetwork.disconnect()    

        if Config.USE_CONNECTION_CACHE:
            # Try and wait for connection cache to finish disconnects (just in case).
            yield TestUtils.waitForConnectionCache()        
        else:
            # Wait a few seconds for network timeouts
            yield TestUtils.wait(5)
        

                        
                
    @defer.inlineCallbacks
    def testMultipleBSNodesAutoDiscoveryClient(self):
        '''Try to start multiple bootstrap nodes then a client and make sure it connects to one of them.
        '''
        
        log.msg("---------------------- BEGIN testMultipleBSNodesAutoDiscoveryClient -------------- ")
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []


        
        # Create Bootstrap
        port = 12345
        enclaveStr = 'testclave'
        bootstrapNodeList = [ NodeLocation(None, self.myIP, port), NodeLocation(None, self.myIP, port+1)]
        
        (self.bsClient1, self.bsNetwork1, d) = self.startBootstrapNode(enclaveStr, self.myIP, port, "authenticate:succeed", bootstrapNodeList)
        yield d

        (self.bsClient2, self.bsNetwork2, d) = self.startBootstrapNode(enclaveStr, self.myIP, port+1, "authenticate:succeed", None)
        yield d
                
        # Now try and join with autodiscovery
        yield self.startClientNodeAutoDiscovery(None, self.myIP, port+2, enclaveStr, bootstrapNodeList)

        # Shut it all down
        for (clientAPI, networkAPI) in self.allNodes:
            yield networkAPI.disconnect()
            
        yield self.bsNetwork1.disconnect()
        yield self.bsNetwork2.disconnect()  
                        
        yield TestUtils.waitForConnectionCache()        
                        
    
        
    def startBootstrapNode(self, enclaveStr, ip, port, authPayload, bootstrapNodeList):
        # Build the client and network objects
        bsClient = SampleClient(ip, port, None)
        bsNetwork = classChordNetworkChord(bsClient, port, ip)
        bsID = bsNetwork.generateNodeID(str(port), enclaveStr) # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests


        # Join the network
        d = defer.Deferred()
        callFunc = lambda result, payload: self.shouldSucceedCallback(result, payload, d, bsNetwork)
        bsNetwork.start(callFunc, bsID, enclaveStr, authPayload, bootstrapNodeList, True, True)
        
        # d will fire upon completion.
        return (bsClient, bsNetwork, d)
        
        
    @defer.inlineCallbacks
    def testStartAutoDiscoveryClientTimeout(self):
        '''Try to start a bootstrap node with autodiscovery on.
           This should show a failure notice.
        '''
        
        log.msg("---------------------- BEGIN testStartAutoDiscoveryClientTimeout -------------- ")
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []

        # Create Bootstrap
        port = 12345
        enclaveStr = 'testclave'
        
        # Now try and join with autodiscovery
        d = defer.Deferred()
        
        clientAPI = SampleClient(self.myIP, port, None)
        networkAPI = classChordNetworkChord(clientAPI, port, self.myIP)
        nodeID = networkAPI.generateNodeID(str(port), enclaveStr) # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests
        
        # Join the network
        callFunc = lambda result, payload: self.shouldFailCallback(result, payload, d)
        networkAPI.start(callFunc, nodeID, enclaveStr, "authenticate:succeed", None, False, True)
        
        yield d
        
        yield TestUtils.waitForConnectionCache()        

        
                           
    def startClientNodeAutoDiscovery(self, _, ip, port, enclaveStr, bootstrapNodeList):
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
        callFunc = lambda result, payload: self.shouldSucceedCallback(result, payload, d, networkAPI)

        networkAPI.start(callFunc, nodeID, enclaveStr, "authenticate:succeed", bootstrapNodeList, False, True)
        
        # Store a tuple for each node
        self.allNodes.append(  (clientAPI, networkAPI) )
        
        return d
        

    def shouldTimeoutCallback(self, result, payload,  deferToFire, networkAPI):
        '''Status callback from the networkAPI.start call. Should be "TIMEOUT".
           Uses a lambda function so we can sneak in a few more parameters :-)
        '''
        self.failUnless(result == "TIMEOUT", 'shouldTimeoutCallback callback got : %s ' % result)
        
        deferToFire.callback(True) # We are join complete.. fire our own callback.
        
    def shouldSucceedCallback(self, result, payload,  deferToFire, networkAPI):
        '''Status callback from the networkAPI.start call. Should be true.
           Uses a lambda function so we can sneak in a few more parameters :-)
        '''
        self.failUnless(result == True, 'Should succeed callback got : %s ' % result)
        
        self.allMetricsObservers.append(MetricsMessageObserver(networkAPI.chordNode)) # Register an observer now
        self.allTestObservers.append(TestMessageObserver(networkAPI.chordNode))


        deferToFire.callback(True) # We are join complete.. fire our own callback.
        
        
    def shouldFailCallback(self, result, payload,  deferToFire):
        '''Status callback from the networkAPI.start call. Should be False.
           Uses a lambda function so we can sneak in a few more parameters :-)
        '''
        self.assertEqual(result, False, 'shouldFailCallback callback got : %s ' % result)
        
        deferToFire.callback(False) # We are join complete.. fire our own callback.
                

    def shouldFailureCallback(self, result, payload,  deferToFire):
        '''Status callback from the networkAPI.start call.
           The result should be the CannotListenError. 
           Uses a lambda function so we can sneak in a few more parameters :-)
        '''
        deferToFire.callback(False) # We are join complete.. fire our own callback.
        
        self.assertTrue(isinstance(result.value, error.CannotListenError), 'shouldFailureCallback callback got : %s ' % result)


        
                        
        
        
    