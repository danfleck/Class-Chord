'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Tests creating nodes and sending out class messages to them.

Created on Dec 12, 2014

@author: dfleck
'''

from twisted.trial import unittest
from twisted.internet import reactor, defer
from twisted.python import log


from gmu.chord import NetworkUtils, Config, ClassIDFactory
from gmu.netclient.envelope import Envelope

import TestUtils
import datetime
import random, sys
import logging
from ConnectivityCounter import ConnectivityCounter

from GmuLogObserver import GmuLogObserver
numNodes = 5 # Number of nodes per enclave
numClassNodes = 10 # How many nodes are in the class?

# Add chord t
class ClassMessageTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(ClassMessageTests, cls).setUpClass()
        
        ClassMessageTests.logObs = log.startLogging(sys.stdout)

    @classmethod
    def tearDownClass(cls):
        super(ClassMessageTests, cls).tearDownClass()
        if ClassMessageTests.logObs is not None:
            ClassMessageTests.logObs.stop()
        
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''
        #print("Setting up...")
        
        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True

        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        
        self.timeout = 60 # How many seconds to try before erroring out
        
    def tearDown(self):
        # Stop the nodes
#         self.leave(None, self.bsNode)
#         self.leave(None, self.normalNode)
#         #print("Tearing down...")
        pass
            
    @defer.inlineCallbacks
    def testNodeSendingToClass(self):
        
        class1 = ClassIDFactory.generateID(ClassIDFactory.WINDOWS, ClassIDFactory.LAPTOP, None)
        class2 = ClassIDFactory.generateID(ClassIDFactory.MAC, ClassIDFactory.DESKTOP, None)
        normalNodes = []
        normalObservers = []
        
        classNodes = []
        classObservers = []
        
        
        # Start a bootstrap node
        (status, self.bsNode, _observer) = yield TestUtils.startupBootstrapNode(self.myIP, 12345, 'localhost', class1)
        self.assertTrue(status, 'Could not build bootstrap node')

        # Start 10 client nodes not in the class
        for i in range(numNodes):
            (status, normalNode, observer) = yield TestUtils.startupClientNode(self.myIP, 12346+i, 'localhost', self.bsNode.nodeLocation, classID=class2)
            normalNodes.append(normalNode)
            normalObservers.append(observer)
            self.assertTrue(status, 'Could not startupClientNode')
        
        # Start a client node in the class
        for i in range(numClassNodes):
            (status, classNode, observer) = yield TestUtils.startupClientNode(self.myIP, 12346+numNodes+i, 'localhost', self.bsNode.nodeLocation, classID=class1)
            classNodes.append(classNode)
            classObservers.append(observer)

            self.assertTrue(status, 'Could not startupClientNode')
        
        # Wait for connectivity
        waiter = ConnectivityCounter()
        yield waiter.waitForConnectivity(numNodes+numClassNodes, self.bsNode) # Does not count bsNode itself.
        
        # Send a class query out
        msgNum = random.randint(0,1000000)
        
        classSpec = ClassIDFactory.generateSpec(computerType=ClassIDFactory.WINDOWS, hwType=None, userType=None) # Only Windows boxes.
        yield TestUtils.sendClassQuery(self.bsNode, classSpec, msgNum )

        yield TestUtils.wait(3)
        
        enclaveID = 1512176389782608
        
        print("DEBUG: BsNode ID: %s" % self.bsNode.nodeLocation.id)
        for o in classObservers:
            print("DEBUG:  [%s] ClassNode: %s   %s -> %s" % (o.getMessageCount(), o.chordNode.nodeLocation.id, o.chordNode.nodeLocation, o.chordNode.remote_getSuccessorLocation(enclaveID)))

        for o in normalObservers:
            print("DEBUG:  [%s] NormalNode: %s   %s -> %s" % (o.getMessageCount(), o.chordNode.nodeLocation.id, o.chordNode.nodeLocation, o.chordNode.remote_getSuccessorLocation(enclaveID)))

        
        # Verify that only one client node received it
        for o in normalObservers:
            self.assertTrue( o.getMessageCount() == 0, "Class message incorrectly went to a non-class node!")
        
        for o in classObservers:
            self.assertTrue( o.getMessageCount() == 1, "Node in class did not receive the class query! [Count:%d][ID:%s]" % (o.getMessageCount(), o.chordNode.nodeLocation.id))
        
        
        # Everyone leave
        for n in classNodes:
            yield n.leave()
        for n in normalNodes:
            yield n.leave()
        yield self.bsNode.leave()

        
        # Wait for the connections to really all close
        if Config.USE_CONNECTION_CACHE:
            yield TestUtils.wait(Config.CONNECTION_CACHE_DELAY + 3)
        else:
            yield TestUtils.wait(3)

        defer.returnValue(True)


       
