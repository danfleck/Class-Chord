'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This module creates two nodes, attempts to talk between them.

Created on Jul 3, 2014

@author: dfleck
'''
from twisted.trial import unittest
from twisted.internet import reactor, defer
from twisted.python import log


from gmu.chord import NetworkUtils, Config
from gmu.netclient.envelope import Envelope

from gmu.chord.CopyEnvelope import CopyEnvelope

import TestUtils
import datetime
import random, sys
import logging


from GmuLogObserver import GmuLogObserver

# Add chord t
class QuickSendTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(QuickSendTest, cls).setUpClass()


        #hdlr = logging.StreamHandler(sys.stdout)        
        
        QuickSendTest.logObs = log.startLogging(sys.stdout)
        #obs = GmuLogObserver(sys.stdout)
        #f = log.startLogging(hdlr)
#         print(f)
#         f = log.startLogging(sys.stdout)
#         print(f)
        #QuickSendTest.logObs = log.startLoggingWithObserver(TestUtils.printLogger)
        #QuickSendTest.logObs = log.startLoggingWithObserver(obs.emit)
        #QuickSendTest.fileLogObs = log.startLogging(open('./quickSendTest.log', 'w'))
        #observer = log.PythonLoggingObserver()
        #observer.start()
   
    @classmethod
    def tearDownClass(cls):
        super(QuickSendTest, cls).tearDownClass()
        if QuickSendTest.logObs is not None:
            QuickSendTest.logObs.stop()
#             #QuickSendTest.fileLogObs.stop()
        
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''
        #print("Setting up...")
        
        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = False
        Config.ALLOW_NO_AUTHENTICATOR = True

        # This is the IP of the node. Note: This MUST be 
        # an external ID or the code won't work!
        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        
        self.timeout = 30 # How many seconds to try before erroring out
        
    def tearDown(self):
        # Stop the nodes
#         self.leave(None, self.bsNode)
#         self.leave(None, self.normalNode)
#         #print("Tearing down...")
        pass
            
    @defer.inlineCallbacks
    def testNodeStartup(self):
        
        # Start a bootstrap node
        (status, self.bsNode, _observer) = yield TestUtils.startupBootstrapNode(self.myIP, 12345, 'localhost')
        self.assertTrue(status, 'Could not build bootstrap node')

        # Start a client node
        (status, self.normalNode, observer) = yield TestUtils.startupClientNode(self.myIP, 12346, 'localhost', self.bsNode.nodeLocation)
        self.assertTrue(status, 'Could not startupClientNode')

        # Are they connected together?
        status = yield self.doQuickSendTest()
        self.assertTrue(status, 'doQuickSendTest')
        
        # Stop the nodes
        yield self.normalNode.leave()
        yield self.bsNode.leave()

        # Wait for the connections to really all close
        if Config.USE_CONNECTION_CACHE:
            yield TestUtils.wait(Config.CONNECTION_CACHE_DELAY + 3)
        else:
            yield TestUtils.wait(3)

        defer.returnValue(True)


    @defer.inlineCallbacks
    def doQuickSendTest(self):
        '''Do a full test to send a message between nodes.'''
        env = CopyEnvelope()
        env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
        env['source'] = self.bsNode.nodeLocation
        env['type'] = 'p2p'
        env['destination'] = self.normalNode.nodeLocation.id
        env['msgID'] = random.randint(1, 99999999) # TODO: Something better here!

        message = "THIS IS A TEST"
        
        #        print("NORMAL NODE: ", self.normalNode, self.normalNode.nodeLocation.id)
        #        print("BS NODE:", self.bsNode, self.bsNode.nodeLocation.id)
        #        print(env)
        status = yield self.bsNode.sendSyncMessage(message, env)
        defer.returnValue(status)
       
