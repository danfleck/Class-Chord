'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This module creates N nodes and sends X messages randomly between nodes and records
any failures. The messages are sent serially (one waits for the next).

It started as a copy of SerialStressTest but lasts for one hour.

Created on Oct 29, 2014

@author: dfleck
'''

from twisted.trial import unittest
from twisted.internet import reactor, defer
from twisted.python import log

from gmu.chord import NetworkUtils, Config
from gmu.chord.NodeLocation import NodeLocation
from gmu.chord.ChordNode import ChordNode
from gmu.chord.MetricsMessageObserver import MetricsMessageObserver
from gmu.chord.CopyEnvelope import CopyEnvelope

import TestUtils
import datetime
import random
import sys


from ConnectivityCounter import ConnectivityCounter

numNodes = 5 # 15
timeLimit = 1 # Minutes
#timeLimit = 0.2 # Minutes

class LongSerialStressTest(unittest.TestCase):
        
    @classmethod
    def setUpClass(cls):
        super(LongSerialStressTest, cls).setUpClass()
        LongSerialStressTest.logObs = log.startLogging(sys.stdout)
        
        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True
        
    @classmethod
    def tearDownClass(cls):
        super(LongSerialStressTest, cls).tearDownClass()
        if LongSerialStressTest.logObs is not None:
            LongSerialStressTest.logObs.stop()
    
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''
        global numNodes
        
        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        
        self.allNodes = []
        self.timeout = (timeLimit + 3) * 60  # How many seconds to try before erroring out
        self.connectedNodeList = [] # How many are currently connected?
        self.msgTracker = dict() # Stores tuples (expectedNumMessages, msgObserver)
        self.testCounter = -1 
        
    @defer.inlineCallbacks
    def testLongSerialP2PSending(self):
        
        
        # Start a bootstrap node
        (status, self.bsNode, observer) = yield TestUtils.startupBootstrapNode(self.myIP, 12345, 'localhost')
        self.assertTrue(status, 'Could not build bootstrap node')
        self.allNodes.append(self.bsNode)
        self.bsNode.addMessageObserver(self.messageReceived)
        self.msgTracker[self.bsNode] = [0, observer] # Number of messages, observer
        
        
        # Start client nodes
        log.msg("Building nodes...")
        for i in range(numNodes):
            (status, node, observer) = yield TestUtils.startupClientNode(self.myIP, 12346+i, 'localhost', self.bsNode.nodeLocation)
            self.assertTrue(status, 'Could not startupClientNode')
            self.allNodes.append(node)
            self.msgTracker[node] = [0, observer] # Number of messages, observer
            

        # Wait for flooding to reach all the nodes
        waiter = ConnectivityCounter()
        yield waiter.waitForConnectivity(numNodes+1, self.bsNode) # Does not count bsNode itself.
        
        # Do the real test
        reactor.callLater(60 * timeLimit, self.stopTest)
        self.keepRunning = True
        status = yield self.doStressTest()
        
        # Now close it all down!
        yield self.allLeave()
        
        # Wait a second or two
        yield TestUtils.wait(Config.CONNECTION_CACHE_DELAY + 3)
        
        defer.returnValue(True)

    def stopTest(self):
        self.keepRunning = False

    @defer.inlineCallbacks
    def doStressTest(self):
        '''Randomly pick two nodes and send a message between them. Verify that it goes.'''
        
        
        endTime = datetime.datetime.now() + datetime.timedelta(minutes=timeLimit)
        
        numMessages = 0
        msgText = dict()
        msgText['type'] = 'COUNT'
        #"Test number %d " % numMessages

        while self.keepRunning:
            
            numMessages += 1
            if numMessages % 100 == 0:
                remainingTime = endTime - datetime.datetime.now()
                log.msg("Running test message %d . Time remaining: %s" % (numMessages, remainingTime))
                #yield TestUtils.wait(1)
            
            if numMessages % 1000 == 0:                
                TestUtils.showOpenConnections()
            
            
            (srcNode, dstNode) = random.sample(self.allNodes, 2)

            # ------------------
            # P2P Test ----
            # ------------------

            # Build the envelope
            env = CopyEnvelope()
            env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
            env['source'] = srcNode.nodeLocation
            env['type'] = 'p2p'
            env['destination'] = dstNode.nodeLocation.id
            env['msgID'] = random.getrandbits(128) # TODO: Something better here!
             
             
            status = yield srcNode.sendSyncMessage(msgText, env)                
            self.assertTrue(status, "Message [%s] -> [%s] returned False!" % (srcNode, dstNode))
         
            # Increment the number of messages dst node should have
            self.msgTracker[dstNode][0] = self.msgTracker[dstNode][0]  + 1
            
            # ------------------
            # Flooding Test ----
            # ------------------
            
            # Now also send a flooding message from the same node to all others
            env = CopyEnvelope()
            env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
            env['source'] = srcNode.nodeLocation
            env['type'] = 'flood'
            env['destination'] = None
            env['msgID'] = random.getrandbits(128)
            env['enclave'] = 'localhost'
            
            status = yield srcNode.sendFloodingMessage(msgText, env)                
            self.assertTrue(status, "Flooding Message [%s] returned False!" % srcNode)
            
            # Increment the number of messages all nodes should get
            for (_node, valList) in self.msgTracker.iteritems():
                #if _node != srcNode:
                valList[0] = valList[0]  + 1

        
            
        self.checkResults()
            
        defer.returnValue(True)
        
        
    def checkResults(self):
        for node, value in self.msgTracker.iteritems():
            got = value[1].getMessageCount()
            expected = value[0]
            log.msg(" Node:%s   Expected:%s   Got:%s " % (node.nodeLocation, expected, got))
            self.assertEqual(got, expected, "Node %s did not recv the expected number of messages. Got:%s  Expected:%s " % (node.nodeLocation, got, expected))

        

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
    
 
    def allLeave(self):
        '''Tell the node to leave the network.'''
        for node in self.allNodes:
            node.leave()
        
        return True
