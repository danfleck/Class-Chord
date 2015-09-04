'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This is used for metrics computation. In production code we wouldn't enable it.

Created on Apr 21, 2014

@author: dfleck
'''
import NodeLocation
import ChordNode
import SuccessorsList
from twisted.internet import defer
from twisted.python import log
from twisted.spread import pb
import datetime, random
from gmu.chord.CopyEnvelope import CopyEnvelope

class MetricsMessageObserver():
    
    def __init__(self, chordNode):
        self.chordNode = chordNode
        chordNode.addMessageObserver(self.messageReceived)
        self.pingbackCounter = 0
    
    @defer.inlineCallbacks
    def messageReceived(self, msg, envelope):
        '''A message was received, do something with it.'''
        
        if isinstance(msg, dict):
            if 'type' in msg:
                if msg['type'] == 'PINGBACK':
                    self.pingbackCounter += 1
                elif msg['type'] == 'PING':        
                    sender = msg['loc']
                    
                    # Ping it back!
                    msg['loc'] = self.chordNode.nodeLocation
                    msg['type'] = 'PINGBACK'
                    
                    # Build an envelope
                    env = CopyEnvelope()
                    env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
                    env['source'] = self.chordNode.nodeLocation
                    env['type'] = 'p2p'
                    env['destination'] = sender.id 
                    env['msgID'] = random.randint(1, 99999999) # TODO: Something better here!
                    
                    # Now send the message
                    try:
                        retVal = yield self.chordNode.sendSyncMessage(msg, env )
                        if retVal == False:
                            log.err("PINGBACK failed sending to loc:%s" % sender)
                    except pb.RemoteError, e:
                        log.err("PINGBACK not sent from [%s] to [%s]" % (self.chordNode.nodeLocation.port, sender.port))
                elif msg['type'] == 'AGGREGATION':
                    #print "Received Aggregation message type!!!!"
                    # add my response to the list of responses                    
                    myResponse = (msg['bytesInResponse']*'A')
                    
                    l = [['responses', myResponse]]
                    for key, value in l:
                        msg.setdefault(key, []).append(value)
                                        
                    responseLength = len(msg['responses'])                                                   
                    numNodes = msg['numNodesToAggregate']
                    queryOrigin = msg['sender']
                    successorId = self.chordNode.successorList.getSuccessor().id
                    # Print the loop information
                    #print("DEBUG: Aggregation loop: %s" % msg)
                    
                    # Build an envelope
                    env = CopyEnvelope()
                    env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
                    env['source'] = self.chordNode.nodeLocation
                    env['type'] = 'p2p'
                    env['msgID'] = random.randint(1, 99999999) # TODO: Something better here!
                    
                    
                    # TODO: make sure logic is accurate
                    if numNodes >= responseLength and queryOrigin == successorId:
                        # send to the query origin    
                        env['destination'] = queryOrigin 
                        self.chordNode.sendSyncMessage(msg, env)
                        print "Completed Aggregation: Sending to our query origin!"                        
                    elif numNodes == responseLength:
                        # send to the query origin                                                                                              
                        env['destination'] = queryOrigin
                        self.chordNode.sendSyncMessage(msg, env)                                                                        
                        # clear the responses and keep sending                        
                        msg['responses'] = []
                        env['destination'] = successorId
                        self.chordNode.sendSyncMessage(msg, env)
                        print "Continued Aggregation: Sending to our query origin!"
                    else:
                        # send to the successor                        
                        env['destination'] = successorId
                        self.chordNode.sendSyncMessage(msg, env)                                            
                        #print "Sending update to our successor!"

                    

                        
        else:
            None
            # print(msg)
                        
    def getPingbackCount(self):
        return self.pingbackCounter
    
    def resetPingbackCount(self):
        self.pingbackCounter  = 0
        return