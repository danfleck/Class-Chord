'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.


Created on Jun 19, 2014

@author: dfleck
'''
from zope.interface import implements

import sys, random
from twisted.python.log import startLogging
from twisted.python import log

from twisted.internet import interfaces, reactor
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.spread import jelly

import cPickle
class Producer(object):
    """
    Send back the requested object to the client.
    """

    implements(interfaces.IPushProducer)

    def __init__(self, proto, bigMsg):
        self._proto = proto
        
        # Create a stream for the message
        msgStream = cPickle.dumps(jelly.jelly(bigMsg))
        
        self.bigMsg = msgStream
        self.bigMsgLength = len(msgStream)
        self._currentIndex = 0
        self._paused = False
        self.chunkSize = 1024 * 5 # 5K chunks

    def pauseProducing(self):
        """
        When we've produced data too fast, pauseProducing() will be called
        (reentrantly from within resumeProducing's sendLine() method, most
        likely), so set a flag that causes production to pause temporarily.
        """
        self._paused = True
        #print 'Pausing connection from %s' % self._proto.transport.getPeer()

    def resumeProducing(self):
        """
        Resume producing integers.

        This tells the push producer to (re-)add itself to the main loop and
        produce integers for its consumer until the requested number of integers
        were returned to the client.
        """
        self._paused = False

        while not self._paused and self._currentIndex < self.bigMsgLength:
            # Send the next chunk
            if self._currentIndex + self.chunkSize > self.bigMsgLength:
                currentChunk = self.bigMsg[self._currentIndex:] # All the way to the end!
            else:
                currentChunk = self.bigMsg[self._currentIndex:self._currentIndex+self.chunkSize]
            
            
            self._proto.sendLine(currentChunk)
            self._currentIndex += self.chunkSize


        if self._currentIndex >= self.bigMsgLength: # We're done!
            self._proto.transport.unregisterProducer()
            self._proto.transport.loseConnection()
            
           

    def stopProducing(self):
        """
        When a consumer has died, stop producing data for good.
        """
        self._currentIndex = self.bigMsgLength


class LargeMessageProducer(LineReceiver):
    '''
    This module sends very large messages using Twisted's streaming protocol.
    Twisted PB can send 640K messages, but anything bigger croaks... this
    streaming version allows much bigger messages.
    '''

    def connectionMade(self):
        """
        Log the connection was made.
        """
        print 'Connection made from %s' % self.transport.getPeer()
        self.factory.numProtocols += 1

    def lineReceived(self, line):
        """
        The line received is simply a message key. If the key is valid, start sending the message,
        if the key is invalid, return an error message.        
        """
        if line in self.factory.messages:
            #print("LargeMessageProducer got line: [%s]" % line)
            msg = self.factory.messages.pop(line) # Get the value and remove from dict
            producer = Producer(self, msg)
            self.transport.registerProducer(producer, True)
            producer.resumeProducing()
        else:
            # Someone used an invalid key!
            log.err("Error: LargeMessageProducer got invalid key! [%s]" % line)
            self.sendLine("Invalid key")

    def connectionLost(self, reason):
        print 'Connection lost from %s' % self.transport.getPeer()
        self.factory.numProtocols -= 1
        
class LargeMessageFactory(Factory):
    '''The Factory manages a dictionary of messages accessed by random keys.
       The random key enables a consumer to request the specific message it wants.
       Once a message is requested once, it's deleted from the dict.
    '''
    
    def __init__(self):
        self.messages = dict()
        self.numProtocols = 0
        
    def addMessage(self, msg):
        '''Add a message with a specific key for a user to get the message.
           Once a message has been sent the key and message will be removed.
        '''
        key= random.randint(1,99999999)
        while key in self.messages:
            key= random.randint(1,99999999)
            
        self.messages[str(key)] = msg # Use strings for safety!
        return key
    
if __name__ == '__main__':
    startLogging(sys.stdout)
    factory = LargeMessageFactory("A"*1024*100)
    factory.protocol = LargeMessageProducer
    reactor.listenTCP(1234, factory)
    reactor.run()