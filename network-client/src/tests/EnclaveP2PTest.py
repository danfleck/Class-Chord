'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Aug 19, 2014

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


class EnclaveP2PTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(EnclaveP2PTest, cls).setUpClass()
        EnclaveP2PTest.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(EnclaveP2PTest, cls).tearDownClass()
        if EnclaveP2PTest.logObs is not None:
            EnclaveP2PTest.logObs.stop()
    
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

        
        # Create Bootstrap
        port = 12345
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        self.allNodeObservers = []
        self.allBootstrapObservers = []
        self.allNodes = []

        # Start a bootstrap node
        log.msg("---> Startup Bootstrap Node", system="EnclaveP2P Test")
        (status, self.bsNode, observer) = yield TestUtils.startupBootstrapNode(self.myIP, port, allEnclaves[0])
        self.assertTrue(status, 'Could not build bootstrap node')
        self.allBootstrapObservers.append(observer)
        
        # Join another enclave
        enableAutoDiscovery = True
        bootstrapNodeList = [ NodeLocation(None, self.myIP, port) ]
        status = yield self.bsNode.joinEnclave(bootstrapNodeList, enableAutoDiscovery, None, isBootstrapNode=True, enclaveStr=allEnclaves[1])

        # Add X nodes to each enclave
        for enclave in allEnclaves:
            for _ in range(numNodes):
                log.msg("---> Add node to enclave", system="EnclaveP2P Test")
                
                # Add a node to the enclave
                (status, node, observer) = yield TestUtils.startupClientNode(self.myIP, startingPort, enclave, bootstrapNodeLocation)
                self.assertTrue(status, 'Could not startupClientNode')
                startingPort += 1
                self.allNodes.append(node)
                self.allNodeObservers.append(observer)
        
        # Wait for flooding to reach all the nodes
        waiter = ConnectivityCounter()
        numberOfNodes = (len(allEnclaves) * numNodes)
        yield waiter.waitForConnectivity(numberOfNodes+1, self.bsNode) # Does not count bsNode itself.

        defer.returnValue(True)

        
    def tearDown(self):
        '''Tear down the network created during setup.'''
        
        log.msg("tearDown begins...")
        d = defer.Deferred()
        
        # Stop everything    
        for node in self.allNodes:
            d.addCallback(node.leave)
        d.addCallback(self.bsNode.leave)
        
        # Wait for all network timeouts to finish
        d.addCallback(TestUtils.defWait, 7)
        
        d.callback(True)
        
        return d
        
        
        
        
    @defer.inlineCallbacks
    def testEnclaveP2PBootstrap(self):
        '''Does 4 tests... bundled so I don't have to 
           rebuild network 4 times.
           
           1. Send from Bootstrap to all nodes
           2. Send from all nodes to Bootstrap
           3. Send among all P2P nodes in same enclaves
           4. Send P2P among nodes in different enclaves (fail)
        '''
  
         
        # P2P from bootstrap to all nodes
        for node in self.allNodes:
            yield self.sendP2PMsg(self.bsNode, node, self.allNodeObservers)
  
  
        # P2P from all nodes to bootstrap
        for node in self.allNodes:
            yield self.sendP2PMsg(node, self.bsNode, self.allBootstrapObservers)
  
        # P2P among nodes in same enclave
        for node1 in self.allNodes:
            for node2 in self.allNodes:
                if node1 != node2:
                    encs1 = node1.remote_getEnclaveNames()
                    encs2 = node2.remote_getEnclaveNames()
                      
                    # Can only send within enclaves
                    if encs1[0] == encs2[0]:
                        yield self.sendP2PMsg(node1, node2, self.allNodeObservers)
          
 
        # P2P among nodes in different enclaves --- should fail
        for node1 in self.allNodes:
            for node2 in self.allNodes:
                if node1 != node2:
                    encs1 = node1.remote_getEnclaveNames()
                    encs2 = node2.remote_getEnclaveNames()
                     
                    # Not within enclave
                    if encs1[0] != encs2[0]:
                        yield self.sendFailingP2PMsg(node1, node2)

                    
        defer.returnValue(True)


    @defer.inlineCallbacks
    def sendP2PMsg(self, src, dst, allNodeObservers):
        '''Send a P2P message from src to dst and verify it got there.
           Also check that no other nodes got it.
        '''
        
        goodCounter = 0
        
        for _ in range(numMessages):
            messageNum = random.randint(1,9999999)
            
            # Send the message
            status = yield TestUtils.sendP2P(src, dst, messageNum )
            self.assertTrue(status,"sendP2PMsg failed! [%s] [%s] to [%s]" % (status, src, dst))
    
            #yield TestUtils.wait(1) # Wait for the messages to send
            
            # Check receipt (both that one did and others did not)
            status = TestUtils.didNodeReceive(allNodeObservers,dst, messageNum)
            self.assertTrue(status,"Expected node did not receive the message!")
    
            status = TestUtils.didNotReceive(allNodeObservers, 'ALL', messageNum, len(allNodeObservers) - 1)
            self.assertTrue(status,"Extra nodes received message!") 
            
            goodCounter += 1
        
        self.assertTrue(goodCounter == numMessages, "sendP2P message did not finish all messages [%d][%d]" % (goodCounter, numMessages))
        defer.returnValue(True)       
                
    @defer.inlineCallbacks
    def sendFailingP2PMsg(self, src, dst):
        '''Send a P2P message from src to dst that should fail to get there
           because they are in different enclaves.
           Also check that no other nodes got it.
        '''
        for _ in range(numMessages):
            messageNum = random.randint(1,9999999)
            
            # Send the message
            status = yield TestUtils.sendP2P(src, dst, messageNum )
            self.assertFalse(status,"sendP2PMsg  succeeded but should have failed! [%s] to [%s]" % (src, dst))
        
        defer.returnValue(True)  
                        

        
