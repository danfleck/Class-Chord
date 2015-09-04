'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This module creates two nodes, attempts to talk between them.

Created on Jun 18, 2014

@author: dfleck
'''
from twisted.trial import unittest
from twisted.internet import  defer, base
from twisted.python import log


from gmu.chord import NetworkUtils, Config
from gmu.chord.NodeLocation import NodeLocation

import TestUtils
import sys

class NetObserver(object):
    
    inc = 0 # Not set
    out = 0 # Not set
    def setStatus(self, inc, out):
        self.inc = inc
        self.out = out

# Add chord t
class NodeCreationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(NodeCreationTest, cls).setUpClass()
        NodeCreationTest.logObs = log.startLogging(sys.stdout)
        
        base.DelayedCall.debug = False
        
        
    @classmethod
    def tearDownClass(cls):
        super(NodeCreationTest, cls).tearDownClass()
        if NodeCreationTest.logObs is not None:
            NodeCreationTest.logObs.stop()
        
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''

        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True


        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        log.msg('Got IP: %s:%s' % (self.myIP, type(self.myIP)))
        
        self.timeout = 120 # How many seconds to try before erroring out
        
    def tearDown(self):
        pass
        #print("Tearing down...")
            
    @defer.inlineCallbacks
    def testNodeStartup(self):
        
        port = 12345
        enclave = 'localhost'
        
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        
        # Start a bootstrap node
        (status, bsNode, _) = yield TestUtils.startupBootstrapNode(self.myIP, port, enclave)
        self.assertTrue(status, 'Could not build bootstrap node')
        
        # Start a client node
        (status, node, _) = yield TestUtils.startupClientNode(self.myIP, 12346, enclave, bootstrapNodeLocation)
        self.assertTrue(status, 'Could not startupClientNode')
        
        #yield TestUtils.wait(3)
        # Are they connected together?
        status = yield self.doConnectionTest(node)
        self.assertTrue(status, 'doConnectionTest')
        
        # Stop the nodes
        yield bsNode.leave()
        yield node.leave()
        
        yield TestUtils.wait(Config.CONNECTION_CACHE_DELAY + 3)
        
        defer.returnValue(True)
        
    @defer.inlineCallbacks
    def testNodeIPChanging(self):
        '''Try to create a node which needs an IP change, but try it with and without allowing
           IP changes. This should test the allowFloatingIP capability.
        '''
        
        # Update this to speed up this test
        Config.NETWORK_CONNECTION_TIMEOUT = 3
        
        port = 12345
        enclave = 'localhost'
        
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        
        # Start a bootstrap node
        (status, bsNode, _) = yield TestUtils.startupBootstrapNode(self.myIP, port, enclave)
        self.assertTrue(status, 'Could not build bootstrap node')
        
        # Start a client node with a bad IP, and wait for it to change
        allowFloatingIP = True
        log.msg("\n\n Starting Client Node1 \n\n", system="testNodeIPChanging")
        (status, node, _) = yield TestUtils.startupClientNode(self.myIP, 12346, enclave, bootstrapNodeLocation, allowFloatingIP)
        self.assertTrue(status, 'Could not startupClientNode')
        
        netObs1 = NetObserver()
        node.addNetworkObserver(netObs1.setStatus)
        
        # Are they connected together?
        status = yield self.doConnectionTest(node)
        self.assertTrue(status, 'doConnectionTest')
        
        allowFloatingIP = False
        log.msg("\n\n Starting Client Node2 \n\n", system="testNodeIPChanging")
        (status, node2, _) = yield TestUtils.startupClientNode(self.myIP, 12347, enclave, bootstrapNodeLocation, allowFloatingIP)
        self.assertTrue(status, 'Could not startupClientNode')
        
        netObs2 = NetObserver()
        node2.addNetworkObserver(netObs2.setStatus)

        # Are they connected together?
        status = yield self.doConnectionTest(node2)
        self.assertTrue(status, 'doConnectionTest')

                
        # Now, change the IP to a "bad IP" and wait.
        badIP = "192.168.9.10"
        node.nodeLocation.ip=badIP
        node2.nodeLocation.ip=badIP
        
        # Now we are waiting for the maint calls to detect the change, and 
        # Issue a new IP
        yield TestUtils.wait(20)
        
        # Check the current IP and verify it's not the bad ip
        self.assertFalse(node.nodeLocation.ip == badIP, "Chord node did not update to a new IP, but should have!")
        self.assertTrue(node2.nodeLocation.ip == badIP, "Chord node changed the IP, but wasn't allowed to float.")

        # Verify the statusus
        self.assertTrue(netObs1.inc == True, "network observer 1 failed incoming!")
        self.assertTrue(netObs1.out == True, "network observer 1 failed outgoing!")
        self.assertTrue(netObs2.inc == False, "network observer 2 failed incoming!")
        self.assertTrue(netObs2.out == True, "network observer 2 failed outgoing!")

        # Stop the nodes
        yield bsNode.leave()
        yield node.leave()
        yield node2.leave()
        
        yield TestUtils.wait(15)

        # Update this to speed up this test
        Config.NETWORK_CONNECTION_TIMEOUT = 30

        
        defer.returnValue(True)
                
                
    def networkObserver1(self, incoming, outgoing):
        log.msg("\n\nNetwork Observer 1: %s  %s " % (incoming, outgoing))
        
    def networkObserver2(self, incoming, outgoing):
        log.msg("\n\nNetwork Observer 2: %s  %s " % (incoming, outgoing))
        

    def doConnectionTest(self, theNode):
        '''Do a full test to send a message between nodes.''' 
        return theNode.isConnectedToAnyEnclave()
       
