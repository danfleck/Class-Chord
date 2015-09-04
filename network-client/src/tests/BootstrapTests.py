'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Tests some different ways to Bootstrap nodes. 
(Note: The APITest does some of this type also!)

Created on Oct 17, 2014

@author: dfleck
'''

from twisted.trial import unittest
from twisted.internet import task, defer, reactor
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


class BootstrapTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(BootstrapTests, cls).setUpClass()
        BootstrapTests.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(BootstrapTests, cls).tearDownClass()
        if BootstrapTests.logObs is not None:
            BootstrapTests.logObs.stop()
        
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
        
    
  


                
                            
    @defer.inlineCallbacks
    def testReBootstrap(self):
        '''Create a BS node, then a client node, then kill the BS Node, wait, restart a BS node and 
           check for re-Bootstrap
        '''
        global startingPort
        
        # Create Bootstrap
        port = 12345
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []

        # Build the BS node
        log.msg("building BS node...")
        (status, bsClientAPI, bsNetworkAPI) = yield TestUtils.startNodeUsingAPI(self.myIP, port, None, 'theEnclave', False, True)
        self.assertTrue(status, 'Could not build bootstrap node')
                
        # Build the client node
        log.msg("building client node...")
        (status, clClientAPI, clNetworkAPI) = yield TestUtils.startNodeUsingAPI(self.myIP, port+1, bootstrapNodeLocation, 'theEnclave', False, False)
        self.assertTrue(status, 'Could not build client node')
        
        # Check that the client is connected to something
        connected = yield clNetworkAPI.isConnected()
        self.assertTrue(connected, "Client did not connect to the bootstrap node in testReBootstrap!")
        
        # Now kill the BS node
        yield bsNetworkAPI.disconnect()
        bsNetworkAPI = None
        bsClientAPI = None
        
        # Gotta wait for disconnect to really finish
        yield TestUtils.waitForConnectionCache()
        
        # Check that the client is connected to something
        connected = yield clNetworkAPI.isConnected()
        self.assertTrue(not connected, "Client remains connected to the bootstrap node in testReBootstrap after killing BS node!")
        
        # Now startup another bootstrap node
        log.msg("building BS node...")
        (status, bsClientAPI, bsNetworkAPI) = yield TestUtils.startNodeUsingAPI(self.myIP, port, None, 'theEnclave', False, True)
        self.assertTrue(status, 'Could not build second bootstrap node')
         
        # Wait for it to connect or fail -- basically waiting for the
        # maintenance call to run correctly.
        for _ in range(10):
            # Check that the client is connected to something
            connected = yield clNetworkAPI.isConnected()
            if connected:
                break
            yield TestUtils.wait(1)
   
        self.assertTrue(connected, "Client could not re-bootstrap!")

        
        # Now shut everything down
        
        log.msg("\n\ntestReBootstrap: Shutting down now...\n\n")
        yield clNetworkAPI.disconnect()
        yield bsNetworkAPI.disconnect()

        if Config.USE_CONNECTION_CACHE:
            yield TestUtils.waitForConnectionCache()
        else:        
            yield TestUtils.wait(5)

        defer.returnValue(True)
                                    
        
    @defer.inlineCallbacks
    def testReBootstrapNewNodeLoc(self):
        '''Create a BS node, then a client node, then kill the BS Node, wait, restart a BS node at a different location and 
           check for re-Bootstrap
        '''
        global startingPort
        
        # Create Bootstrap
        port = 12345
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []

        # Build the BS node
        log.msg("building BS node...")
        (status, bsClientAPI, bsNetworkAPI) = yield TestUtils.startNodeUsingAPI(self.myIP, port, None, 'theEnclave', False, True)
        self.assertTrue(status, 'Could not build bootstrap node')
                
        # Build the client node
        log.msg("building client node...")
        (status, clClientAPI, clNetworkAPI) = yield TestUtils.startNodeUsingAPI(self.myIP, port+1, bootstrapNodeLocation, 'theEnclave', True, False)
        self.assertTrue(status, 'Could not build client node')
        
        # Check that the client is connected to something
        connected = yield clNetworkAPI.isConnected()
        self.assertTrue(connected, "Client did not connect to the bootstrap node in testReBootstrap!")
        
        # Now kill the BS node
        yield bsNetworkAPI.disconnect()
        bsNetworkAPI = None
        bsClientAPI = None
        
        
        # Gotta wait for network timeout disconnect to really finish.
        yield TestUtils.wait(30)
                
        
        # Check that the client is connected to something
        connected = yield clNetworkAPI.isConnected()
        self.assertTrue(not connected, "Client remains connected to the bootstrap node in testReBootstrap after killing BS node!")
        
        # Now startup another bootstrap node
        log.msg("building BS node...")
        (status, bsClientAPI, bsNetworkAPI) = yield TestUtils.startNodeUsingAPI(self.myIP, port+3, None, 'theEnclave', False, True)
        self.assertTrue(status, 'Could not build second bootstrap node')
         
        # Wait for it to connect or fail -- basically waiting for the
        # maintenance call to run correctly.
        for _ in range(30):
            # Check that the client is connected to something
            connected = yield clNetworkAPI.isConnected()
            if connected:
                break
            yield TestUtils.wait(1)
   

        
        # Now shut everything down
        
        log.msg("\n\ntestReBootstrap: Shutting down now...\n\n")
        yield clNetworkAPI.disconnect()
        yield bsNetworkAPI.disconnect()
        
        if Config.USE_CONNECTION_CACHE:
            yield TestUtils.waitForConnectionCache()
        else:        
            yield TestUtils.wait(5)
        
        
        # This is the final question
        self.assertTrue(connected, "Client could not re-bootstrap!")

        defer.returnValue(True)
                
                
    @defer.inlineCallbacks
    def testDisconnectedBootstraps(self):
        '''Create a BS node and some clients. Create another bootstrap node and some clients (so we essentially have two rings). 
           Verify, that the bootstrap nodes autodiscover each other and connect together            
        '''
        global startingPort
        
        # Create Bootstrap
        port = 12345
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        bootstrapNodeLocation2 = NodeLocation(None, self.myIP, port+1)
        self.allNodes = []
        self.allMetricsObservers = []
        self.allTestObservers = []

        # Build the BS node
        (status, bsClientAPI, bsNetworkAPI) = yield TestUtils.startNodeUsingAPI(bootstrapNodeLocation.ip, bootstrapNodeLocation.port, None, 'theEnclave', True, True)
        self.allMetricsObservers.append(MetricsMessageObserver(bsNetworkAPI.chordNode))
        self.assertTrue(status, 'Could not build bootstrap node')

        # Build second BS node
        (status, bsClientAPI2, bsNetworkAPI2) = yield TestUtils.startNodeUsingAPI(bootstrapNodeLocation2.ip, bootstrapNodeLocation2.port, None, 'theEnclave', True, True)
        self.allMetricsObservers.append(MetricsMessageObserver(bsNetworkAPI2.chordNode))
        self.assertTrue(status, 'Could not build bootstrap node 2')

                
        # Build the client node
        (status, clClientAPI, clNetworkAPI) = yield TestUtils.startNodeUsingAPI(self.myIP, port+2, bootstrapNodeLocation, 'theEnclave', False, False)
        self.allMetricsObservers.append(MetricsMessageObserver(clNetworkAPI.chordNode))        
        self.assertTrue(status, 'Could not build client node')
                
        # Build the client node
        (status, clClientAPI2, clNetworkAPI2) = yield TestUtils.startNodeUsingAPI(self.myIP, port+3, bootstrapNodeLocation2, 'theEnclave', False, False)
        self.allMetricsObservers.append(MetricsMessageObserver(clNetworkAPI2.chordNode))
        self.assertTrue(status, 'Could not build client node')


        # Wait for flooding to reach all the nodes
        waiter = ConnectivityCounter()
        yield waiter.waitForConnectivity(3, clNetworkAPI.chordNode) # Does not count clNode itself.
        
                
        # Now shut everything down
        yield clNetworkAPI.disconnect()
        yield clNetworkAPI2.disconnect()
        yield bsNetworkAPI.disconnect()
        yield bsNetworkAPI2.disconnect()
        
        if Config.USE_CONNECTION_CACHE:
            yield TestUtils.waitForConnectionCache()
        else:        
            yield TestUtils.wait(5)
            
        defer.returnValue(True)
                        
        
        
        
        
        
    