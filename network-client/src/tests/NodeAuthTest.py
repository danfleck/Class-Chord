'''
Created on Sep 23, 2014

@author: shiremag
'''

from twisted.trial import unittest
from twisted.internet import task, defer
from twisted.python import log, failure


from gmu.chord import NetworkUtils, Config
from gmu.chord import ChordNode
from gmu.chord.NodeLocation import NodeLocation
from gmu.chord.MetricsMessageObserver import MetricsMessageObserver

from gmu.netclient.classChordNetworkChord import classChordNetworkChord

# Testing Modules
from ConnectivityCounter import ConnectivityCounter
from SampleClient import SampleClient
import TestUtils
import sys, random
from TestMessageObserver import TestMessageObserver
from twisted.internet.defer import Deferred



class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(Test, cls).setUpClass()
        Test.logObs = log.startLogging(sys.stdout)
        
   
    @classmethod
    def tearDownClass(cls):
        super(Test, cls).tearDownClass()
        if Test.logObs is not None:
            Test.logObs.stop()
        
    def setUp(self):
        '''Start the reactor so we don't have to do it in the nodes.'''

        # Turn off warning
        Config.WARN_NO_MESSAGE_AUTHENTICATOR = True
        Config.ALLOW_NO_AUTHENTICATOR = False

        self.myIP = NetworkUtils.getNonLoopbackIP (None, None)
        log.msg('Got IP: %s:%s' % (self.myIP, type(self.myIP)))
        
        # Create Bootstrap
        log.msg("Creating Boot Strap Node")
        self.BSport = 12345
        self.bootstrapNodeLocation = NodeLocation(None, self.myIP, self.BSport)
        self.enclaveStr = 'localhost'
        
        self.allNodes = []

        
    def startBootStrapNode(self):
        d = defer.Deferred()

        # Build the client and network objects
        self.bsClient = SampleClient(self.myIP, self.BSport, None)
        self.bsNetwork = classChordNetworkChord(self.bsClient, self.BSport, self.myIP)
        bsID = self.bsNetwork.generateNodeID(str(self.BSport), self.enclaveStr) # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests
        
        # Join the network
        callFunc = lambda x, payload: self.shouldSucceedCallback(x, payload, d, self.bsNetwork)
        


        bootstrapNodeList = [ NodeLocation(None, self.myIP, self.BSport)]
        #bootstrapNodeList = None
        self.bsNetwork.start(callFunc, bsID, self.enclaveStr, "authenticate:succeed", bootstrapNodeList, True, False)
        
        return d
    
    
    def startClientNode(self, ip, port, bootstrapNodeLocation, enclaveStr):
        '''Return a deferred which should fire after join succeeds. 
           
           Order of events is:
              After the callback from networkAPI.start happens, then we do a few things and then
              this code fires d.callback() (using the "d" which is returned from this method.
        '''
        d = defer.Deferred()
        
        clientAPI = SampleClient(ip, port, None)
        networkAPI = classChordNetworkChord(clientAPI, port, ip)
        nodeID = networkAPI.generateNodeID(str(port), enclaveStr) # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests
        bootstrapNodeList = [ bootstrapNodeLocation ]
        
        # Join the network
        callFunc = lambda result, payload: self.shouldSucceedCallback(result, payload, d, networkAPI)
        networkAPI.start(callFunc, nodeID, enclaveStr, "authenticate:succeed", bootstrapNodeList, False, True)
        
        # Store a tuple for each node
        self.allNodes.append(  (clientAPI, networkAPI) )
        
        return d
    
    def startBadClientNode(self, ip, port, bootstrapNodeLocation, enclaveStr):
        '''Return a deferred which should fire after join succeeds. 
           
           Order of events is:
              After the callback from networkAPI.start happens, then we do a few things and then
              this code fires d.callback() (using the "d" which is returned from this method.
        '''
        d = defer.Deferred()
        
        clientAPI = SampleClient(ip, port, None)
        networkAPI = classChordNetworkChord(clientAPI, port, ip)
        nodeID = networkAPI.generateNodeID(str(port), enclaveStr) # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests
        bootstrapNodeList = [ bootstrapNodeLocation ]

        # Join the network
        callFunc = lambda result, payload: self.shouldFailCallback(result, payload, d, networkAPI)
        networkAPI.start(callFunc, nodeID, enclaveStr, "authenticate:failed", bootstrapNodeList, False, True)

        # Store a tuple for each node
        #self.allNodes.append(  (clientAPI, networkAPI) )
        
        return d
    
    def shouldSucceedCallback(self, result, payload, deferToFire, networkAPI):
        '''Status callback from the networkAPI.start call. Should be true.
           Uses a lambda function so we can sneak in a few more parameters :-)
        '''
        self.failUnless(result == True, 'Should succeed callback got : %s ' % result)

        deferToFire.callback(True) # We are join complete.. fire our own callback.
        

    def shouldFailCallback(self, result, payload, deferToFire, networkAPI):
        '''Status callback from the networkAPI.start call. Should be true.
           Uses a lambda function so we can sneak in a few more parameters :-)
        '''
                
        #self.failUnless(result == False, 'Should succeed callback got : %s ' % result)
        log.msg("shouldFailCallback: %s" % result, system="NodeAuthTest")
        
        self.failUnless(result == False, 'Should Fail callback got : %s ' % result)
        #self.assertRaises("Node could not authenticate to network.", ChordNode.ChordNode.authenticateJoin)


        deferToFire.callback(True) # We are join complete.. fire our own callback.

    def tearDown(self):
        pass

    @defer.inlineCallbacks
    def testNodeAuth(self):
        log.msg("---> Starting BootstrapNode")
        yield self.startBootStrapNode()
        
        log.msg("---> Creating Good Client")
        yield self.startClientNode(self.myIP, 12350, self.bootstrapNodeLocation, self.enclaveStr)
        
        log.msg("---> Creating Bad Client")
        yield self.startBadClientNode(self.myIP, 12351, self.bootstrapNodeLocation, self.enclaveStr)
                    
        # Shut it all down
        for (clientAPI, networkAPI) in self.allNodes:
            yield networkAPI.disconnect()
        
        yield self.bsNetwork.disconnect()
        
        if Config.USE_CONNECTION_CACHE:
            yield TestUtils.waitForConnectionCache()
        else:
            yield TestUtils.wait(2)
        
        defer.returnValue(True)

