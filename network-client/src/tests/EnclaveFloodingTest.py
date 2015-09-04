'''
Created on Aug 15, 2014

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

numNodes = 7 # Number of nodes per enclave
numMessages=10    # Total number of messages to send
startingPort = 12350
allEnclaves = ['enc1', 'enc2']


class EnclaveFloodingTest(unittest.TestCase):


    @classmethod
    def setUpClass(cls):
        super(EnclaveFloodingTest, cls).setUpClass()
        EnclaveFloodingTest.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(EnclaveFloodingTest, cls).tearDownClass()
        if EnclaveFloodingTest.logObs is not None:
            EnclaveFloodingTest.logObs.stop()
        
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''

        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True
        
        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        log.msg('Got IP: %s:%s' % (self.myIP, type(self.myIP)))

        self.timeout = 600  # How many seconds to try before erroring out

        #import twisted
        #twisted.internet.base.DelayedCall.debug = True
        
    def tearDown(self):
        pass
        
        
        
        
    @defer.inlineCallbacks
    def testEnclaveFlooding(self):
        '''Build 2 enclaves and test flooding to each and all.'''
        global startingPort

        # Create Bootstrap
        port = 12345
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        allNodes = []
        allNodeObservers = []
        allBootstrapObservers = []
        self.allNodes = allNodes
        
        # Start a bootstrap node
        (status, bsNode, observer) = yield TestUtils.startupBootstrapNode(self.myIP, port, allEnclaves[0])
        self.assertTrue(status, 'Could not build bootstrap node')
        allBootstrapObservers.append(observer)

        self.bsNode = bsNode
        
        # Join another enclave
        bootstrapNodeList = None
        enableAutoDiscovery = True
        status = yield self.bsNode.joinEnclave(bootstrapNodeList, enableAutoDiscovery, None, isBootstrapNode=True, enclaveStr=allEnclaves[1])
        
                # Add X nodes to each enclave
        for enclave in allEnclaves:
            for _ in range(numNodes):
                # Add a node to the enclave
                (status, node, observer) = yield TestUtils.startupClientNode(self.myIP, startingPort, enclave, bootstrapNodeLocation)
                self.assertTrue(status, 'Could not startupClientNode')
                startingPort += 1
                allNodes.append(node)
                allNodeObservers.append(observer)
        
        # Wait for flooding to reach all the nodes
        waiter = ConnectivityCounter()
        numberOfNodes = (len(allEnclaves) * numNodes)
        yield waiter.waitForConnectivity(numberOfNodes, bsNode) # Does not count bsNode itself.
        
        # Flood from bootstrap to all
        messageNum = random.randint(1,9999999)
        status = yield TestUtils.sendFlood(bsNode,messageNum, 'ALL' )
        self.assertTrue(status,"sendFlood failed!")

        yield TestUtils.wait(3) # Wait for the messages to send
        
        # Check message counts
        status = TestUtils.didReceive(allNodeObservers, 'ALL', messageNum, numberOfNodes)
        self.assertTrue(status,"All nodes did not receive message")
        
        # Flood from bootstrap to single enclave 
        messageNum = random.randint(1,9999999)
        status = yield TestUtils.sendFlood(bsNode,messageNum, allEnclaves[0] )
        self.assertTrue(status,"sendFlood failed!")

        yield TestUtils.wait(3) # Wait for the messages to send
        
        # Check message counts
        status = TestUtils.didReceive(allNodeObservers, allEnclaves[0], messageNum, numNodes)
        self.assertTrue(status,"Nodes in enclave %s didn't all recv message")

        status = TestUtils.didNotReceive(allNodeObservers, allEnclaves[1], messageNum, numNodes)
        self.assertTrue(status,"Some Nodes in enclave %s did recv message")
        
        log.msg("\n\n\n TEST ARE ALL DONE \n\n\n")
        yield TestUtils.wait(60)

        # Stop everything    
        for node in self.allNodes:
            yield node.leave()
        yield self.bsNode.leave()
        
        # Wait for all network timeouts to finish
        yield TestUtils.wait(Config.CONNECTION_CACHE_DELAY + 3)
        #self.flushLoggedErrors()
        
         
        defer.returnValue(True)

        
