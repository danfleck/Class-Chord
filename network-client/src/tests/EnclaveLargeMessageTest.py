'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Test large messages using an enclave network.

Created on Aug 21, 2014

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
msgSize = 790 * 1024 # 790K


class EnclaveLargeMessageTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(EnclaveLargeMessageTest, cls).setUpClass()
        EnclaveLargeMessageTest.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(EnclaveLargeMessageTest, cls).tearDownClass()
        if EnclaveLargeMessageTest.logObs is not None:
            EnclaveLargeMessageTest.logObs.stop()
        
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''

        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True

        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        log.msg('Got IP: %s:%s' % (self.myIP, type(self.myIP)))
        
        self.timeout = 120  # How many seconds to try before erroring out


        
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
        (status, self.bsNode, observer) = yield TestUtils.startupBootstrapNode(self.myIP, port, allEnclaves[0])
        self.assertTrue(status, 'Could not build bootstrap node')
        self.allBootstrapObservers.append(observer)
        
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
                self.allNodes.append(node)
                self.allNodeObservers.append(observer)
        
        # Wait for flooding to reach all the nodes
        waiter = ConnectivityCounter()
        numberOfNodes = (len(allEnclaves) * numNodes)
        yield waiter.waitForConnectivity(numberOfNodes, self.bsNode) # Does not count bsNode itself.

        defer.returnValue(True)

        
    def tearDownNetwork(self):
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
    def testEnclaveP2PLargeMessage(self):
        '''Does 2 tests... bundled so I don't have to 
           rebuild network 2 times.
           
           1. Send a large message from Bootstrap to all nodes
           2. Send a large message from all nodes to Bootstrap
        '''
  
        # Build it
        log.msg("testEnclaveP2PLargeMessage: Network building...")
        yield self.buildNetwork()
        
        log.msg("testEnclaveP2PLargeMessage: Network Built")
         
        # P2P from bootstrap to all nodes
        for node in self.allNodes:
            yield self.sendLargeP2PMsg(self.bsNode, node, self.allNodeObservers)
    
        log.msg("testEnclaveP2PLargeMessage: Network 1")
    
        # P2P from all nodes to bootstrap
        for node in self.allNodes:
            yield self.sendLargeP2PMsg(node, self.bsNode, self.allBootstrapObservers)
            
        log.msg("testEnclaveP2PLargeMessage: Network 2")
   
        # Flood from bootstrap to all nodes
        for node in self.allNodes:
            yield self.sendLargeFloodingMsg(self.bsNode, allEnclaves[0], allEnclaves[1], self.allNodeObservers, numNodes, numNodes)

        log.msg("testEnclaveP2PLargeMessage: Network 3")
        
        # Flood from all nodes to bootstrap
        for node in self.allNodes:
            nodesEnclave = node.remote_getEnclaveNames() # Returns a list
            if nodesEnclave[0] == allEnclaves[0]:
                otherEnclave = allEnclaves[1]
            else:
                otherEnclave = allEnclaves[0]
                
            yield self.sendLargeFloodingMsg(node, nodesEnclave[0], otherEnclave, self.allNodeObservers, numNodes, numNodes)
   
        yield TestUtils.wait(10)
        log.msg("testEnclaveP2PLargeMessage: Network 4")


        #Tear it down
        yield self.tearDownNetwork()
        
        log.msg("testEnclaveP2PLargeMessage: Network 5")        

        
        
        defer.returnValue(True)


    @defer.inlineCallbacks
    def sendLargeP2PMsg(self, src, dst, allNodeObservers):
        '''Send a P2P message from src to dst and verify it got there.
           Also check that no other nodes got it.
        '''
        
        msgData = "M" * msgSize
        for _ in range(numMessages):
            messageNum = random.randint(1,9999999)
            
            # Send the message
            status = yield TestUtils.sendP2P(src, dst, messageNum, msgData )
            self.assertTrue(status,"sendP2PMsg failed! [%s] to [%s]" % (src, dst))
    
            
            # Check receipt (both that one did and others did not)
            status = TestUtils.didNodeReceive(allNodeObservers,dst, messageNum)
            self.assertTrue(status,"Expected node did not receive the message!")
    
            status = TestUtils.didNotReceive(allNodeObservers, 'ALL', messageNum, len(allNodeObservers) - 1)
            self.assertTrue(status,"Extra nodes received message!") 

        yield TestUtils.wait(5) # Wait for the messages to send
        
        defer.returnValue(True)   
        
        
    @defer.inlineCallbacks
    def sendLargeFloodingMsg(self, src, dstEnclave, otherEnclave, allNodeObservers, expectedNumRecv, expectedNumNotRecv):
        '''Send a P2P message from src to dst and verify it got there.
           Also check that no other nodes got it.
        '''
        
        print("DEBUG: sendLargeFloodingMsg [%s][%s][%s]" % ( dstEnclave, otherEnclave, src))
        msgData = "M" * msgSize
        messageNum = random.randint(1,9999999)
        
        # Send the message
        try:
            status = yield TestUtils.sendFlood(src,messageNum, dstEnclave, msgData )
            self.assertTrue(status,"sendLargeFlood message failed! [%s] " % src)
        except Exception, e:
            log.err(e)
            log.err("Error was in sendLargeFloodingMsg")
        

        # Not sure if I need this or not... seems like I should need it?!?
        #yield TestUtils.wait(3) # Wait for the messages to send
        
        # Check message counts
        status = TestUtils.didReceive(allNodeObservers, dstEnclave, messageNum, expectedNumRecv)
        self.assertTrue(status,"Nodes in enclave %s didn't all recv message. \n[%s]\n[%s][%s]" % (dstEnclave, src, dstEnclave, otherEnclave))

        status = TestUtils.didNotReceive(allNodeObservers, otherEnclave, messageNum, expectedNumNotRecv)
        self.assertTrue(status,"Some Nodes in enclave %s did recv message" % otherEnclave)
        
                     
        
        defer.returnValue(True)
                
    
        
        
        
