'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Have a node disconnect from the network and then wait for ring re-formation

Created on Aug 19, 2014

@author: dfleck
'''


from twisted.trial import unittest
from twisted.internet import task, defer, error
from twisted.python import log


from gmu.chord import NetworkUtils, Config, ConnectionCache
from gmu.chord.NodeLocation import NodeLocation

# Testing Modules
from ConnectivityCounter import ConnectivityCounter
import TestUtils
import sys, random

numNodes = 5 # Number of nodes per enclave
numMessages=10    # Total number of messages to send
startingPort = 12350


class NodeDisconnectTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(NodeDisconnectTest, cls).setUpClass()
        NodeDisconnectTest.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(NodeDisconnectTest, cls).tearDownClass()
        if NodeDisconnectTest.logObs is not None:
            NodeDisconnectTest.logObs.stop()
    
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''
        

        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True

        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        log.msg('Got IP: %s:%s' % (self.myIP, type(self.myIP)))

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
        enclave = 'theEnc'

        # Start a bootstrap node
        (status, self.bsNode, observer) = yield TestUtils.startupBootstrapNode(self.myIP, port, enclave)
        self.assertTrue(status, 'Could not build bootstrap node')
        self.allBootstrapObservers.append(observer)
        

        # Add X nodes to each enclave
        for _ in range(numNodes):
            # Add a node to the enclave
            (status, node, observer) = yield TestUtils.startupClientNode(self.myIP, startingPort, enclave, bootstrapNodeLocation)
            self.assertTrue(status, 'Could not startupClientNode')
            startingPort += 1
            self.allNodes.append(node)
            self.allNodeObservers.append(observer)
            
        # Wait for flooding to reach all the nodes
        waiter = ConnectivityCounter()
        yield waiter.waitForConnectivity(numNodes+1, self.bsNode) # Does not bsNode .

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
        #d.addCallback(TestUtils.defWait, 7)
        #d.addCallback(self.flushLoggedErrors)
        
        # Now wait for the disconnect to fully take hold
        d.addCallback(TestUtils.waitForConnectionCache)
        
        d.callback(True)
        
        return d
        
        
        
        
    @defer.inlineCallbacks
    def testNodeDisconnection(self):
        '''Disconnect a node from the ring and do a few things.
        
            - Send a message to that node (verify error condition)
            - Send a flooding message -- wait until connectivity happens for all other nodes.

        '''
  
        # Disconnect a node
        nodeIndex = random.randint(0, numNodes-1) # Node to disconnect
        
        nodeLoc = self.allNodes[nodeIndex].nodeLocation
        log.msg("testNodeDisconnection: Node is leaving [%s]...\n" % nodeLoc)
        ConnectionCache._printCache()
        yield self.allNodes[nodeIndex].leave()
        log.msg("testNodeDisconnection: Node left...\n")
      
        try:
            # Now wait for full connectivity to come back
            waiter = ConnectivityCounter()
            yield waiter.waitForConnectivity(numNodes, self.bsNode) # Does  count bsNode.
        except error.ConnectionRefusedError:
            log.err("NodeDisconnect test got a connection refused error [2]")
            log.err()
            
        # Now wait for the disconnect to fully take hold
        yield TestUtils.wait(30) # Network timeout time
        
        print("DEBUG testNodeDisc \n\n\n\n")
        ConnectionCache._printCache()

        try:            
            # P2P from bootstrap to node which left
            yield self.sendFailingP2PMsg(self.bsNode, self.allNodes[nodeIndex])
        except error.ConnectionRefusedError:
            log.msg("\n\nNodeDisconnect test got a connection refused error [3], which is okay!\n\n")
                    
   
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
            self.assertFalse(status,"sendP2PMsg  succeeded but should have failed! [%s] [%s] to [%s]" % (status, src, dst))
        
        defer.returnValue(True)  
                        

        
