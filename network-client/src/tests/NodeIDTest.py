'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Tests the APIs to set a Node ID 

Created on Sept 5, 2014

@author: dfleck
'''

from twisted.trial import unittest
from twisted.internet import task, defer
from twisted.python import log, failure


from gmu.chord import NetworkUtils, Config
from gmu.chord.NodeLocation import NodeLocation
from gmu.chord.MetricsMessageObserver import MetricsMessageObserver

from gmu.netclient.classChordNetworkChord import classChordNetworkChord

# Testing Modules
from ConnectivityCounter import ConnectivityCounter
from SampleClient import SampleClient
import TestUtils
import sys, random, hashlib
from TestMessageObserver import TestMessageObserver


numNodes = 5 # Number of nodes per enclave
numMessages=5   # Total number of messages to send to each node
startingPort = 12350


class NodeIDTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(NodeIDTest, cls).setUpClass()
        NodeIDTest.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(NodeIDTest, cls).tearDownClass()
        if NodeIDTest.logObs is not None:
            NodeIDTest.logObs.stop()
        
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''

        # Turn ON warning for this test!
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = True
        Config.ALLOW_NO_AUTHENTICATOR = False

        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        log.msg('Got IP: %s:%s' % (self.myIP, type(self.myIP)))

        return
        
    
    def testNodeIDGeneration(self):
        '''Try to generate the IDs for multiple nodes and enclaves, and verify they are consistent.'''
        
        
        # Create Bootstrap
        ip = '127.0.0.1'
        port = 12345

        # Build the client and network objects
        enclaveStr = 'localhost'
        self.bsClient = SampleClient(ip, port, None)
        self.bsNetwork = classChordNetworkChord(self.bsClient, port, ip)
        
        # Do the real test now
        
        enclaveStr = self.randomStringGenerator(10)
        
        firstNodeName = self.randomStringGenerator(15)
        firstID = self.bsNetwork.generateNodeID(firstNodeName, enclaveStr)
        bits = self.getBits(firstID, Config.ENCLAVE_ID_BITS)

        for _ in range(100000):
            # Generate a 100 node IDs
        
            randomNodeName = self.randomStringGenerator(15)

            theID = self.bsNetwork.generateNodeID(randomNodeName, enclaveStr)
            
            # Now verify that all the IDs have the same first Config.ENCLAVE_ID_BITS
            if bits != self.getBits(theID, Config.ENCLAVE_ID_BITS):
                print("enclaveStr: %s " % enclaveStr)
                print("firstNodeName: %s " % firstNodeName)
                print("otherNodeName: %s " % randomNodeName)

                print("bits0: %s " % bin(firstID))
                print("bits1: %s " % bin(theID))
                
                self.failUnless(bits == self.getBits(theID, Config.ENCLAVE_ID_BITS), 
                            'testNodeIDGenerator made IDs with different enclave bits!\n[%s]\n[%s]\n[%s]\n[%s]' % 
                            (firstID, theID, self.getBits(theID, Config.ENCLAVE_ID_BITS), bits))
        
            
    def getBits(self, aLong, numBits):
        '''Grab the first numBits from aString (high order bits)'''
    
        # Grab the bits from the ip/port hash
        return bin(aLong)[2:numBits+2]
        
        
        
        
        
    def randomStringGenerator(self, size):
        '''Generate a random string of "size" chars. This is largely from:
        http://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python
        '''
        import string
        chars=string.ascii_uppercase + string.digits
        return ''.join(random.choice(chars) for _ in range(size))


    def testCreateNodeWithID(self):
        '''Create a node with a given ID and verify the ID stuck.'''
        
        d = defer.Deferred()
        # Create Bootstrap
        ip = '127.0.0.1'
        port = 12345

        # Build the client and network objects
        enclaveStr = 'localhost'
        self.bsClient = SampleClient(ip, port, None)
        
        self.bsNetwork = classChordNetworkChord(self.bsClient, port, ip)
        
        # Do the real test now
        bsID = 12345111
        
        # Join the network
        callFunc = lambda x, payload: self.shouldSucceedCallback(x, payload, d, self.bsNetwork, bsID)
        self.bsNetwork.start(callFunc, bsID, enclaveStr, "authenticate:succeed", None, True, False)
        
        return d
    
    def shouldSucceedCallback(self, _result, payload, defered, networkAPI, nodeID):
        
        print("DEBUG: NodeID: %s " % networkAPI.chordNode.nodeLocation.id)
        if nodeID != networkAPI.chordNode.nodeLocation.id:
            self.fail("nodeID != bsID [%s][%s]" % (nodeID, networkAPI.chordNode.nodeLocation.id))
            
        networkAPI.chordNode.leave()
        
        defered.callback(True)
        
        
        
        
    