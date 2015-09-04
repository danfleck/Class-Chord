'''
This module creates N nodes and sends X messages randomly between nodes and records
any failures. The messages are sent serially (one waits for the next).

Created on Jul 7, 2014

@author: dfleck
'''
from twisted.trial import unittest
from twisted.internet import reactor, defer
from twisted.python import log

from gmu.chord import NetworkUtils, Config
from gmu.chord.CopyEnvelope import CopyEnvelope

import TestUtils
import datetime
import random
import sys


from ConnectivityCounter import ConnectivityCounter

numNodes = 2
numMessages=100

class SerialStressTest(unittest.TestCase):
        
    @classmethod
    def setUpClass(cls):
        super(SerialStressTest, cls).setUpClass()
        SerialStressTest.logObs = log.startLogging(sys.stdout)
        
        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True
        
    @classmethod
    def tearDownClass(cls):
        super(SerialStressTest, cls).tearDownClass()
        if SerialStressTest.logObs is not None:
            SerialStressTest.logObs.stop()
    
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''
        global numNodes
        
        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        
        self.allNodes = []
        self.timeout = (numNodes * 5) + numMessages # How many seconds to try before erroring out
        self.connectedNodeList = [] # How many are currently connected?
        
        self.testCounter = -1 
        
    def tearDown(self):
        # Stop the nodes
#         self.leave(None, self.bsNode)
#         self.leave(None, self.normalNode)
#         #print("Tearing down...")
        pass
            
            
    @defer.inlineCallbacks
    def testSerialP2PSending(self):
        
        # Start a bootstrap node
        (status, self.bsNode, _observer) = yield TestUtils.startupBootstrapNode(self.myIP, 12345, 'localhost')
        self.assertTrue(status, 'Could not build bootstrap node')
        self.allNodes.append(self.bsNode)
        self.bsNode.addMessageObserver(self.messageReceived)
        
        # Start client nodes
        log.msg("Building nodes...")
        for i in range(numNodes):
            (status, node, observer) = yield TestUtils.startupClientNode(self.myIP, 12346+i, 'localhost', self.bsNode.nodeLocation)
            self.assertTrue(status, 'Could not startupClientNode')
            self.allNodes.append(node)

        # Wait for flooding to reach all the nodes
        waiter = ConnectivityCounter()
        yield waiter.waitForConnectivity(numNodes, self.bsNode) # Does not count bsNode itself.
        
        # Do the real test
        status = yield self.doStressTest()
        
        # Now close it all down!
        yield self.allLeave()
        
        # Wait a second or two
        yield TestUtils.wait(3+Config.CONNECTION_CACHE_DELAY)
        
        defer.returnValue(True)


    @defer.inlineCallbacks
    def doStressTest(self):
        '''Randomly pick two nodes and send a message between them. Verify that it goes.'''
        
        print("Running serial stress test: %d p2p messages" % numMessages)
        for i in range(numMessages):
            if True: #i % 100 == 0:
                print("Running test %d of %d" % (i, numMessages))
                
            (srcNode, dstNode) = random.sample(self.allNodes, 2)
            

            # Build the envelope
            env = CopyEnvelope()
            env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
            env['source'] = srcNode.nodeLocation
            env['type'] = 'p2p'
            env['destination'] = dstNode.nodeLocation.id
            env['msgID'] = random.getrandbits(128) # TODO: Something better here!
            
            msgText = "Test number %d " % i
            
            status = yield srcNode.sendSyncMessage(msgText, env)
                
            self.assertTrue(status, "Message [%s] -> [%s] returned False!" % (srcNode, dstNode))
            
        
        defer.returnValue(True)
        
        

        

    def messageReceived(self, msg, dummy_envelope):
        '''This is a receiver for the bootstrap node only!
        
           We got a message. For flooding pingbacks the message format is:
           type:PINGBACK
           loc:sender
           msgNum:number
        '''
        if not isinstance(msg, dict):
            return
        
        if 'type' in msg:
            theType= msg['type']
            if theType == "PINGBACK":
                
                if msg['msgNum'] == 0:  # Setup message only
                    # Add the sender to the list of nodes we know of
                    self.addNode(msg['loc'])
                    #print("Metrics NetworkConnect addNode: %s" % str(msg['loc']))
                elif msg['msgNum'] == self.testCounter:
                    # We have a message from a current PING, count it!
                    self.connectedNodeList.append(msg['loc'])
                else:
                    # Typically this means a message came in late
                    log.msg("SerialStressTest got an unknown message:%s" % msg)
                            
        
        
    def printError(self, theErr):
        print("Errback was called! %s" % theErr)
    
 
    def allLeave(self):
        '''Tell the node to leave the network.'''
        for node in self.allNodes:
            node.leave()
        
        return True
