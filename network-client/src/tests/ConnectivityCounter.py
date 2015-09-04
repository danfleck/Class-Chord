'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Count the number of nodes connected to a node network

Created on Aug 15, 2014

@author: dfleck
'''
from twisted.internet import defer,reactor, task

from gmu.chord.CopyEnvelope import CopyEnvelope

import datetime, time, random
from twisted.python import log
import TestUtils


class ConnectivityCounter(object):
    '''
    classdocs
    '''


    @defer.inlineCallbacks
    def waitForConnectivity(self, numToWaitFor, chordNode):
        '''Wait till we can connect to all numNodes'''

        self.node = chordNode
        self.testCounter = 1
        self.connectedNodeList = []

        
        # Need to see the messages
        self.node.addMessageObserver(self.messageReceived)
        
        yield self.getNumConnected()
        
        while len(self.connectedNodeList) < numToWaitFor:
            log.msg("DEBUG: waiting for %d nodes. Got %d" % (numToWaitFor, len(self.connectedNodeList)), system="ConnectivityCounter")
            self.testCounter += 1
            yield self.getNumConnected()
            yield TestUtils.wait(5) # Wait for messages to go around
        
        log.msg("DEBUG: waiting for %d nodes. Got %d" % (numToWaitFor, len(self.connectedNodeList)), system="ConnectivityCounter")
#         for n in self.connectedNodeList:
#             print("DEBUG:       Node:%s" % n)

        # Don't care anymore
        self.node.removeMessageObserver(self.messageReceived)

    def getNumConnected(self):
        '''Returns a deferred that will fire when the self.connectedNodeList is populated.'''
        
        chordNode = self.node
        
        # Clear the list
        del self.connectedNodeList[:]
               
        # Send out a flooding message
        msgText = { "type" : "PING", "loc" : chordNode.nodeLocation, "msgNum" : self.testCounter }
        
        # Build the envelope
        env = CopyEnvelope()
        env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
        env['source'] = chordNode.nodeLocation
        env['type'] = 'flood'
        env['enclave'] = 'ALL' # Flooding to ALL enclaves
        env['msgID'] = random.getrandbits(128) # TODO: Something better here!
        env['nodeCounter'] = 0 # Make the nodes count off                      
        
        # Send the message
        d = chordNode.sendFloodingMessage(msgText, env)
        
        return d     
        

    def messageReceived(self, msg, envelope):
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
                
                if msg['msgNum'] == self.testCounter and msg['loc'] not in self.connectedNodeList:
                    # We have a message from a current PING, count it!
                    self.connectedNodeList.append(msg['loc'])
                else:
                    # Typically this means a message came in late
                    log.msg("ConnectivityCounter got an unknown message:%s" % msg)
                            

        