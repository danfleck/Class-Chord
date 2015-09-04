'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Tests the new AggregationMessage functionality. Sends a flooding message to the network and 
asks for an aggregation response. Verifies the response was successfully received.

Created on Aug 25, 2014

@author: dfleck
'''

from twisted.trial import unittest
from twisted.internet import task, defer
from twisted.python import log


from gmu.chord import NetworkUtils, Config
from gmu.chord.NodeLocation import NodeLocation

# Testing Modules
from ConnectivityCounter import ConnectivityCounter
from TestMessageObserver import TestMessageObserver
from gmu.chord.MetricsMessageObserver import MetricsMessageObserver
import TestUtils
import sys, random

numNodes = 5 #25 # Number of nodes per enclave
numMessages=5   # Total number of messages to send to each node
startingPort = 12350


class AggregationMessageTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(AggregationMessageTest, cls).setUpClass()
        AggregationMessageTest.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(AggregationMessageTest, cls).tearDownClass()
        if AggregationMessageTest.logObs is not None:
            AggregationMessageTest.logObs.stop()
        
        
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''

        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True

        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        log.msg('Got IP: %s:%s' % (self.myIP, type(self.myIP)))


        # Set the timeout relative to total messages we're going to send.
        self.timeout = max(60,(numNodes * numMessages * numNodes) / 3) # Seconds

        
        d = self.buildNetwork()
        return d
        
    @defer.inlineCallbacks        
    def buildNetwork(self):
        global startingPort

        
        # Create Bootstrap
        port = 12345
        bootstrapNodeLocation = NodeLocation(None, self.myIP, port)
        self.allNodeObservers = []
        self.allNetworkAPIs = []
        self.allClientAPIs = []

        # Start a bootstrap node
        log.msg("building BS node...")
        (status, clientAPI, networkAPI) = yield TestUtils.startNodeUsingAPI(self.myIP, port, None, 'theEnclave', False, True)
        self.assertTrue(status, 'Could not build bootstrap node')
        
        node = networkAPI.chordNode # This is very highly coupled -- bad
        MetricsMessageObserver(node)
        self.observer = TestMessageObserver(node, networkAPI)
        self.bsNode = node

        
        log.msg("building client nodes...")
        
        # Add X nodes to each enclave
        for _ in range(numNodes):
            # Add a node to the enclave
            (status, clientAPI, networkAPI) = yield TestUtils.startNodeUsingAPI(self.myIP, startingPort, bootstrapNodeLocation, 'theEnclave', False, False)
            self.assertTrue(status, 'Could not startupClientNode')
            
            node = networkAPI.chordNode # This is very highly coupled -- bad
            MetricsMessageObserver(node)
            observer = TestMessageObserver(node, networkAPI)
            
            startingPort += 1
            self.allNetworkAPIs.append(networkAPI)
            self.allNodeObservers.append(observer)
            self.allClientAPIs.append(clientAPI)
                        
        # Wait for flooding to reach all the nodes
        waiter = ConnectivityCounter()
        yield waiter.waitForConnectivity(numNodes+1, self.bsNode) # Does count bsNode itself.

        defer.returnValue(True)      
        
    @defer.inlineCallbacks
    def doTearDown(self):
        '''Tear down the network created during setup.'''
        
        log.msg("tearDown begins...")
        
        # Stop everything    
        for networkAPI in self.allNetworkAPIs:
            yield networkAPI.disconnect()
            
        yield self.bsNode.leave()
        
        # Wait for all network timeouts to finish
        #self.flushLoggedErrors()
        if Config.USE_CONNECTION_CACHE:
            yield TestUtils.wait(Config.CONNECTION_CACHE_DELAY + 2)
        else:
            yield TestUtils.defWait(3)
            
        defer.returnValue(True)
        
        
    @defer.inlineCallbacks
    def testAggregationMessage(self):
        '''
        Send flooding message and ask for aggregation response.
        
        '''
        
        print("testAggregationMessage method starting...")
        counter = 1
        try:
            d = defer.Deferred()
    
            messageNum = random.randint(1,9999999)
    
            # Reset message count
            self.observer.resetMessageCount()
            self.observer.printDebug = True
                
            aggMax = numNodes + 2
            
            #aggNumber = 3
            for aggNumber in range(1, aggMax+1):
            #if aggNumber == aggNumber:
    
                # Flood from bootstrap to all nodes
                for i in range(numMessages):
                    #d.addCallback(self.sendFlood, self.bsNode,messageNum+i,'theEnclave', data="SEND_AGG_RESPONSE:%d" % aggNumber)
                    yield self.sendFlood(None, self.bsNode,messageNum+i,'theEnclave', data="SEND_AGG_RESPONSE:%d" % aggNumber)
                    print("sending message %d", counter)
                    counter += 1

    
            d.callback(True)
            yield d
            
            # Wait a few seconds
            yield TestUtils.wait(5)
            
            print("testAggregationMessage check results...")

            # Now check the results
            yield self.checkResults(aggMax)
            
        except Exception, e:
            self.anError(e)
        finally:
            yield self.doTearDown()
        

            
        print("testAggregationMessage method returning...")

        defer.returnValue(True) 
    
    def sendFlood(self, _, chordNode,messageNum,enclave, data=""):
        return TestUtils.sendFlood(chordNode,messageNum,enclave, data)
        
        
    def anError(self, e):
        log.err(e, "An error occurred in testAggregationMessage")
    
    def checkResults(self, aggMax):
        # Now check how many messages the bootstrap node got in return
        recvCount = self.observer.getMessageCount()
        self.assertTrue(recvCount == (numNodes*numMessages*aggMax)+(numMessages*aggMax), "recv count[%d] != numNodes[%d]*numMessages[%d]*aggMax[%d] +(numMessages*aggMax) = [%d]" % (recvCount, numNodes, numMessages, aggMax, (numNodes*numMessages*aggMax)+(numMessages*aggMax)))
        
        
        didFail = False
        for o in self.allNodeObservers:
            # How many acks did each node get?
            acks = o.getAcks()
            o.clearAcks()
            
            print("acks[%d] ? numMessages[%d] * aggMax[%d]" % (acks, numMessages, aggMax))
            if acks != numMessages*aggMax:
                didFail = True
                print("Failing node was: %s" % o.chordNode.nodeLocation)
                    
                    
        self.assertFalse(didFail, "acks[%d] != numMessages*aggMax[%d]" % (acks, numMessages*aggMax))            

        return not didFail
   