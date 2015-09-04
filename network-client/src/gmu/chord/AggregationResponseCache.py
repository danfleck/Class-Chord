'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Aug 25, 2014

@author: dfleck
'''

from twisted.internet import reactor, defer
from twisted.internet.defer import DeferredSemaphore
from twisted.python import log

from gmu.chord import Utils

class AggregationResponseCache(object):
    '''
    This holds all the responses being aggregated for a single destination.
    
    One of the main challenges here is to make sure while we're sending the responses,
    we don't get a new response in and not send it.
    '''


    def __init__(self, numSecondsToWait, numMessagesToWaitFor, chordNode):
        '''
        Constructor
        '''
        self.numSecondsToWait = numSecondsToWait
        self.numMessagesToWaitFor = numMessagesToWaitFor
        self.numSecondsToWait = numSecondsToWait
        self.chordNode = chordNode
        self.semaphore = DeferredSemaphore(1)
        self.messageList = [] # Holds tuples of (message, envelope)
        
        # Construct a timer to wait
        self.timerID = None
        
    def addResponse(self, message, envelope):
        '''We use a semaphore to ensure we don't modify the list while sending.'''
        d = self.semaphore.acquire()
        d.addCallback(self._addResponse, message, envelope)
        
    def _addResponse(self, dummy_defResult, message, envelope):
        '''This is called only once we have the semaphore.'''         
        self.messageList.append ( (message, envelope) )
        
        print("DEBUG: AggRespCache: %s  adding message %s " % (self.chordNode.nodeLocation.port, message))
        
        if len(self.messageList) >= self.numMessagesToWaitFor:
            # Send it!
            self._sendResponse()
        else:
            # Make sure a timer is running
            if self.timerID is None or not self.timerID.active():
                self.timerID = reactor.callLater(self.numSecondsToWait, self.sendResponse)
            
            # We're done.
            self.semaphore.release()    
        
            
    def sendResponse(self):
        '''Only call sendResponse when you have the lock.'''
        d = self.semaphore.acquire()
        d.addCallback(self._sendResponse)
        
    
    def _sendResponse(self, dummy_deferResult=None):
        '''Send the response but only after acquiring the semaphore
        '''
        # Copy the list
        messagesListCopy = self.messageList
        self.messageList = []
        
        # Release the semaphore
        self.semaphore.release()
        
        # Stop the timer if it's still going
        if self.timerID is not None and self.timerID.active():
            self.timerID.cancel()
            self.timerID = None
        
        print("DEBUG: AggResponseCache-Sending %d Messages %s" % (len(messagesListCopy), self.chordNode.nodeLocation.port))
        
        # Send a P2P message to the dest with all the responses
        d = self.chordNode.sendSyncMultipleMessage(messagesListCopy, 'p2p') # Will this break message authentication?
        d.addCallback(self.sendAcks, messagesListCopy)
        d.addErrback(self.sendResponseFailed)

#     def emptyMessageList(self, _):
#         self.messageList = []
        
    def sendAcks(self, resultsDict, messageList):
        # Send ACK messages to the nodes for which we aggregated
        
        for (_message, envelope) in messageList:
            # Get the status to return
            msgID = envelope['msgID']
            if msgID not in resultsDict:
                status = False
            else:
                status = resultsDict[msgID]

            d = self.chordNode.sendSingleAck(msgID, envelope['source'], status)
            d.addErrback(self.sendAckFailed, envelope['source'])
                        
            
    def sendAckFailed(self, fail, sourceNode):
        log.err("We failed to SendAck for source %s" % sourceNode, fail)
            
        
    def sendResponseFailed(self, theFailure):
        log.err(theFailure)    
        
        
        

        