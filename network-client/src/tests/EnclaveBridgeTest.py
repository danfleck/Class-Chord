'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Sept 8, 2014

Test creating two rings, and then bridge them using a client node (not a bootstrap node). This case is not
covered by other tests.

@author: dfleck
'''


from twisted.trial import unittest
from twisted.internet import task, defer
from twisted.python import log


from gmu.chord import NetworkUtils, Config
from gmu.chord.NodeLocation import NodeLocation

# Testing Modules
from ConnectivityCounter import ConnectivityCounter
import TestUtils
import sys, random

numNodes = 3 # Number of nodes per enclave
numMessages=10    # Total number of messages to send
startingPort = 12350
allEnclaves = ['enc1', 'enc2']


class EnclaveBridgeTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(EnclaveBridgeTest, cls).setUpClass()
        EnclaveBridgeTest.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(EnclaveBridgeTest, cls).tearDownClass()
        if EnclaveBridgeTest.logObs is not None:
            EnclaveBridgeTest.logObs.stop()
        
    
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''
       
        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True

        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        log.msg('Got IP: %s:%s' % (self.myIP, type(self.myIP)))

        # Get the clock        
        self.clock = task.Clock()

        d = self.buildNetwork()
        return d
        
    @defer.inlineCallbacks        
    def buildNetwork(self):
        global startingPort

        
        # Create two Bootstrap nodes
        port = 12345
        self.allNodeObservers = []
        self.allBootstrapObservers = []
        self.allNodes = []
        
        # Start a bootstrap node
        (status, self.bsNode1, observer) = yield TestUtils.startupBootstrapNode(self.myIP, port, allEnclaves[0])
        self.assertTrue(status, 'Could not build bootstrap node')
        self.allBootstrapObservers.append(observer)
        
        # Start another bootstrap node
        (status, self.bsNode2, observer) = yield TestUtils.startupBootstrapNode(self.myIP, port+1, allEnclaves[1])
        self.assertTrue(status, 'Could not build bootstrap node')
        self.allBootstrapObservers.append(observer)
        
        # Add some nodes to each bootstrap node
        for _ in range(numNodes):
            # Add a node to the enclave
            (status, node, observer) = yield TestUtils.startupClientNode(self.myIP, startingPort, allEnclaves[0], self.bsNode1.nodeLocation)
            self.assertTrue(status, 'Could not startupClientNode')
            startingPort += 1
            self.allNodes.append(node)
            self.allNodeObservers.append(observer)

        for _ in range(numNodes):
            # Add a node to the enclave
            (status, node, observer) = yield TestUtils.startupClientNode(self.myIP, startingPort, allEnclaves[1], self.bsNode2.nodeLocation)
            self.assertTrue(status, 'Could not startupClientNode')
            startingPort += 1
            self.allNodes.append(node)
            self.allNodeObservers.append(observer)

    
        # Wait for flooding to reach all the nodes
        waiter = ConnectivityCounter()
        yield waiter.waitForConnectivity(numNodes, self.bsNode1) # Does not count bsNode itself.
        yield waiter.waitForConnectivity(numNodes, self.bsNode2) # Does not count bsNode itself.

        defer.returnValue(True)

        
    def tearDown(self):
        '''Tear down the network created during setup.'''
        
        log.msg("tearDown begins...")
        d = defer.Deferred()
        
        # Stop everything    
        for node in self.allNodes:
            d.addCallback(node.leave)
            
        d.addCallback(self.bridgeNode.leave)
        d.addCallback(self.bsNode1.leave)
        d.addCallback(self.bsNode2.leave)
        
        # Wait for all network timeouts to finish
        d.addCallback(TestUtils.defWait, 7)
        
        d.callback(True)
        
        return d
        
        
        
        
    @defer.inlineCallbacks
    def testEnclaveBridge(self):
        '''Create a node that can bridge between the enclaves
        
           Send a flooding message from it to 1 enclave and verify only nodes in that one got it
           
        '''
        authenticationPayload = "This is a test"
        
  
        # Build the bridge node
        (status, bridgeNode, observer) = yield TestUtils.startupClientNode(self.myIP, 12500, allEnclaves[0], self.bsNode1.nodeLocation)
        self.failUnless(status, 'testEnclave bridge could not join first bootstrap [%s]' % status)
        
        # Join the other enclave also.
        enableAutoDiscovery = True
        bootstrapNodeList = [ self.bsNode2.nodeLocation ]
        status = yield bridgeNode.joinEnclave(bootstrapNodeList, enableAutoDiscovery,  authenticationPayload, False, None)
        self.failUnless(status, 'testEnclave bridge could not join second bootstrap [%s]' % status)

        self.bridgeNode = bridgeNode # Store a reference
        
        
        # Send a flooding message to one enclave and verify only one got it.
        messageNum = random.randint(1,9999999)
        status = yield TestUtils.sendFlood(bridgeNode,messageNum, allEnclaves[0] )
        self.assertTrue(status,"sendFlood failed!")

        yield TestUtils.wait(3) # Wait for the messages to send
        
        # Check message counts
        status = TestUtils.didReceive(self.allNodeObservers, allEnclaves[0], messageNum, numNodes)
        self.assertTrue(status,"Nodes in enclave %s didn't all recv message")

        status = TestUtils.didNotReceive(self.allNodeObservers, allEnclaves[1], messageNum, numNodes)
        self.assertTrue(status,"Some Nodes in enclave %s did recv message")
        
                