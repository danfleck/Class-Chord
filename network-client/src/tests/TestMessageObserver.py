'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Aug 15, 2014

@author: dfleck
'''
from twisted.internet import defer
from twisted.python import log
from twisted.spread import pb
import datetime, random

class TestMessageObserver():
    
    def __init__(self, chordNode, networkAPI=None):
        self.chordNode = chordNode
        chordNode.addMessageObserver(self.messageReceived)
        self.storedMessages = []
        self.messageCount = 0
        self.processedMessages = []
        self.acks = 0
        self.networkAPI = networkAPI
        
        self.printDebug = False
    
    def messageReceived(self, msg, envelope):
        '''A message was received, do something with it.'''
        
        if isinstance(msg, dict):
            
            
            # Don't reprocess
            if envelope['msgID'] in self.processedMessages:
                return
            
            if 'type' in msg:
                
                if self.printDebug:
                    print("DEBUG: TestCounter: count:%s   MSG: %s" % (self.messageCount, msg))
                
                if msg['type'] == 'COUNT':
                    self.messageCount += 1
                    
                elif msg['type'] == 'STORE':
                    '''Store the message for later... only used in Unit Tests..
                       this shouldn't be used by client code because it will fill up
                       the buffer!
                    '''
                    self.storedMessages.append(msg)
                    
                    self.messageCount += 1
                    
                    
                    # Should we 
                    data = msg['data']
                    if data.startswith('SEND_AGG_RESPONSE'):
                        (text, aggNumber) = data.split(":")
                        aggNumber = int(aggNumber)
                        # Don't send something to myself!
                        if msg['loc'].id != self.chordNode.nodeLocation.id:
                            print("DEBUG: TMO SEND_AGG_RESPONSE: Me:%s  From:%s  MsgNum:%s ENV:%s" % (self.chordNode.nodeLocation.port, envelope['source'].port, msg['msgNum'], envelope))
                            self.processedMessages.append(envelope['msgID'])
                            
                            # Send an aggregate response back to the source
                            d = self.sendAggResponse(msg, envelope, aggNumber)
                            d.addCallback(self.logAcks)
                            d.addErrback(self.showErrback)
                        else:
                            # Who from?
                            print("DEBUG: TestMessageObserver: Got message from:%s   MSG:%s" % (envelope, msg))

        else:
            None
            # print(msg)
            
    def showErrback(self, e):
        log.err("Error in TestMessageObserver", e)
        log.err(e)        
            
    def showCallback(self, response):
        log.msg("DEBUG: TestMessageObserver: callback:%s" % response) 
        
    def logAcks(self, defResult):
        if defResult:
            self.acks += 1
        else:
            log.err("Got an ack without a True result! %s" % defResult)
        
    def getAcks(self):
        return self.acks

    def clearAcks(self):
        self.acks = 0   
    
    @defer.inlineCallbacks
    def sendAggResponse(self, msg, envelope, aggNumber):
        '''Send the aggregation response message. Returns a deferred.'''
        

        if self.networkAPI is None:
            raise Exception("Error in TestMessageObserver... you didn't set the networkAPI during instance construction!")
        
        sender = msg['loc']
        msg['data'] = "Sender port : %s" % self.chordNode.nodeLocation.port

        print("DEBUG: CALLED TestMessageObserver sendAggResponse : From:%s To:%s  " % (self.chordNode.nodeLocation.port, sender.port))
        
        d = defer.Deferred()
        statusCallback = lambda result: d.callback(result) 
        self.networkAPI.sendMessage(msg, 'agg-response', statusCallback, destination=sender.id, ttl=datetime.datetime.now() + datetime.timedelta(minutes=10), enclave='ALL', aggregationNum=aggNumber)
        status = yield d
        
        print("DEBUG: TestMessageObserver sendAggResponse: From:%s To:%s  STATUS:%s" % (self.chordNode.nodeLocation.port, sender.port, status))
        
        defer.returnValue(status)        
        
         
    def getMessageCount(self):
        return self.messageCount
    
    def resetMessageCount(self):
        self.messageCount = 0
          
    def getStoredMessages(self):
        '''Only for the unit tests.'''
        return self.storedMessages
    
    def messageNumStored(self, messageNum):
        '''Is this num in stored messages?'''
        for msg in self.storedMessages:
            if msg['msgNum'] == messageNum:
                return True
        return False
            