'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This module creates N nodes and sends X messages randomly between nodes and records
any failures. The messages are sent in blocks of 100 and then it waits and does it again.

Created on Jul 7, 2014

@author: dfleck
'''
from twisted.trial import unittest
from twisted.internet import reactor, defer
from twisted.python import log

from gmu.chord import NetworkUtils, Config
from gmu.chord.CopyEnvelope import CopyEnvelope

import TestUtils
from ConnectivityCounter import ConnectivityCounter
import datetime
import random, sys


numNodes = 5
numMessages=5000    # Total number of messages to send
numMessagesInBlock=100 # Size of blocks to send them in

class ParallelStressTest(unittest.TestCase):


    @classmethod
    def setUpClass(cls):
        super(ParallelStressTest, cls).setUpClass()
        ParallelStressTest.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(ParallelStressTest, cls).tearDownClass()
        if ParallelStressTest.logObs is not None:
            ParallelStressTest.logObs.stop()
        
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''
        global numNodes
        
        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True

        #log.startLogging(open('parallelStressTest.log', 'w'))
        
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
    def testParallelP2PSending(self):
        
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
        yield waiter.waitForConnectivity(numNodes+1, self.bsNode) # Does not count bsNode itself.

        # Now do the stress test
        status = yield self.doStressTest()
        
        # Now close it all down!
        yield self.allLeave()
        
        # Wait a second or two
        yield TestUtils.wait(3)
        
        defer.returnValue(True)



    @defer.inlineCallbacks
    def doStressTest(self):
        '''Randomly pick two nodes and send a message between them. Verify that it goes.'''
        
        print("Running parallel stress test: %d p2p messages" % numMessages)
        messageCounter = 0
        while messageCounter < numMessages:
            if messageCounter % 100 == 0:
                print("Running test %d of %d" % (messageCounter, numMessages))
            
            statusList = []
            for _ in range(numMessagesInBlock):     
                messageCounter += 1
                (srcNode, dstNode) = random.sample(self.allNodes, 2)
                
    
                # Build the envelope
                env = CopyEnvelope()
                env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
                env['source'] = srcNode.nodeLocation
                env['type'] = 'p2p'
                env['destination'] = dstNode.nodeLocation.id
                env['msgID'] = random.getrandbits(128) # TODO: Something better here!
                
                        
                msgText = "Test number %d " % messageCounter
                
                statusList.append(srcNode.sendSyncMessage(msgText, env))
                    
            
            # Now wait for all of them to complete
            dl = defer.DeferredList(statusList)
            results = yield dl # Wait for it
            
            # Now check all the return codes
            for (success, _) in results:
                #print("DEBUG: doStressTest Result is %s" % success)
                self.assertTrue(success, "doStressTest Message returned False!" )
                
            
            # Wait a bit... just to ease up a smidge.
            yield TestUtils.wait(0.1)
        
        defer.returnValue(True)
        


    def messageReceived(self, msg, dummy_Envelope):
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
                    log.msg("ParallelStressTest got an unknown message:%s" % msg)
                            
        
       
    def allLeave(self):
        '''Tell the node to leave the network.'''
        for node in self.allNodes:
            node.leave()
        
        return True