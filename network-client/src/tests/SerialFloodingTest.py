'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This module creates N nodes and then sends X flooding messages from each node.

The code then verifies that every message made it to every node using counts... it doesn't really check the 
content of every message.

Created on Jul 9, 2014

@author: dfleck
'''
from twisted.trial import unittest
from twisted.internet import reactor, defer
from twisted.python import log

from gmu.chord import NetworkUtils, Config
from gmu.chord.CopyEnvelope import CopyEnvelope

import datetime
import random, sys
import TestUtils
from ConnectivityCounter import ConnectivityCounter


import gmu.chord

numNodes = 5   # How many nodes do I build?
numMessages=100 # How many flooding messages will each send?

class MyMessageObserver():
    '''Simple class for each node to hold it's messages.'''
    def __init__(self):
        self.messagesReceived = 0
        self.captureOn = False
        
    def setCaptureOn(self, on=True):
        self.captureOn = on
        
    def messageReceived(self, dummy_msg, dummy_env):
        if self.captureOn:
            self.messagesReceived += 1# .append(msg)
        
    def getNumReceived(self):
        return self.messagesReceived
    
    def printMessages(self):
        print("\n\n\n")
#         for m in self.messagesReceived:
#             print(m)
        
class SerialFloodingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(SerialFloodingTest, cls).setUpClass()
        SerialFloodingTest.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(SerialFloodingTest, cls).tearDownClass()
        if SerialFloodingTest.logObs is not None:
            SerialFloodingTest.logObs.stop()
        
    
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''
        global numNodes
        
        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True
        
        log.msg("Chord node from: %s" % gmu.chord.__file__)
        #log.startLogging(open('SerialFloodingTest.log', 'w'))
        
        #print("Setting up...")
        
        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        
        self.allObservers = []
        self.allNodes = []
        self.timeout = (numNodes * 5) + (numMessages* numNodes) # How many seconds to try before erroring out
        self.connectedNodeList = [] # How many are currently connected?
        self.testCounter = -1
                
    def tearDown(self):
        # Stop the nodes
#         self.leave(None, self.bsNode)
#         self.leave(None, self.normalNode)
#         #print("Tearing down...")
        pass
            
            
    @defer.inlineCallbacks
    def testSerialFlooding(self):
        
        # Start a bootstrap node
        (status, self.bsNode, _observer) = yield TestUtils.startupBootstrapNode(self.myIP, 12345, 'localhost')
        self.assertTrue(status, 'Could not build bootstrap node')
        self.allNodes.append(self.bsNode)
        self.bsNode.addMessageObserver(self.messageReceived)

        # Start client nodes
        for i in range(numNodes):
            (status, node, observer) = yield TestUtils.startupClientNode(self.myIP, 12346+i, 'localhost', self.bsNode.nodeLocation)
            self.assertTrue(status, 'Could not startupClientNode')
            self.allNodes.append(node)
            observer = MyMessageObserver()
            node.addMessageObserver(observer.messageReceived)
            observer.node = node
            self.allObservers.append(observer)
        
        # Wait for flooding to reach all the nodes
        waiter = ConnectivityCounter()
        yield waiter.waitForConnectivity(numNodes, self.bsNode) # Does not count bsNode itself.

        # Now do the stress test
        status = yield self.doStressTest()
        
        # Now close it all down!
        yield self.allLeave()
        
        # Wait a second or two for network timeouts
        yield TestUtils.wait(9)
        
        defer.returnValue(True)


    @defer.inlineCallbacks
    def doStressTest(self):
        '''Iterate through all the nodes and send a bunch of flooding messages from each.'''
        
        print("Running serial stress test: %d p2p messages" % numMessages)
        
        # Start capturing
        for sendingNodeObserver in self.allObservers:
            sendingNodeObserver.setCaptureOn()
        
        for sendingNodeObserver in self.allObservers:

            
            for i in range(numMessages):        
                # Build the envelope
                env = CopyEnvelope()
                env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
                env['source'] = sendingNodeObserver.node.nodeLocation
                env['type'] = 'flood'
                env['enclave'] = 'ALL' # Flooding
                env['msgID'] = random.getrandbits(128) # TODO: Something better here!
                
                        
                msgText = "Test number %d " % i
                
                status = yield sendingNodeObserver.node.sendFloodingMessage(msgText, env)
                
                self.assertTrue(status, "Message [%s] returned False!" % sendingNodeObserver.node)
        
        # At this point all the messages are sent... check that they all got there!
        status = yield self.waitForAllReceived()
        
        defer.returnValue(status)
        
        
    @defer.inlineCallbacks
    def waitForAllReceived(self):
        '''Wait until all messages have been received by all the nodes.'''
        
        
        numTries = 10
        
        expectedNumMsg = numNodes*numMessages
        for _ in range(numTries):
            
            completedObservers = 0
            incompleteObservers = 0

            # Wait a sec
            yield TestUtils.wait(1)


            # Count them
            for obs in self.allObservers:
                numRx = obs.getNumReceived()
                
                if numRx == expectedNumMsg:
                    completedObservers += 1
                elif numRx > expectedNumMsg:
                    obs.printMessages()
                    raise Exception("Programming error... received more messages than possible! Got[%d] Expected[%d]" % (numRx,expectedNumMsg ))
                else:
                    incompleteObservers += 1
                    
            print("waitForAllReceived: Complete:%d   Incomplete:%d" % (completedObservers, incompleteObservers))
            
            if incompleteObservers == 0:
                defer.returnValue(True)
            
                
        defer.returnValue(False)
    
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
                            
        
        
#     def printError(self, theErr):
#         print("Errback was called! %s" % theErr)
#     
#    
    
#     def checkTrue(self, result, label="None"):
#         print("%s: RESULT IS : %s" % (label, result))
#         self.assertTrue(result)
#         return result
#  
    def allLeave(self):
        '''Tell the node to leave the network.'''
        for node in self.allNodes:
            node.leave()
        
        return True
