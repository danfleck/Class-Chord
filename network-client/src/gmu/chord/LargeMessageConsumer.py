'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Jun 19, 2014

@author: dfleck
'''
from twisted.python import log

from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor, defer
from twisted.spread import jelly
from twisted.internet.protocol import Protocol, Factory
from twisted.internet.endpoints import TCP4ClientEndpoint

import cPickle

def copiedConnectProtocol(endpoint, protocol):
    """
    COPIED from Twisted because v11.1 does not have this method.
    connectProtocol API in Twisted v13.1
    (and we use v11.1 in Android right now)
    """
    class OneShotFactory(Factory):
        def buildProtocol(self, addr):
            return protocol
    return endpoint.connect(OneShotFactory())
    
class LargeMessageConsumer(LineReceiver):
    '''
    This class receives big messages which Perspective Broker cannot handle.
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.totalMessage = ""
        self.messageBegun = False
        self.erroredOut = False # Did the whole thing fail?
        
    def openConnection(self, ip, port):
        '''Open a connection to the producer'''
        endpoint = TCP4ClientEndpoint(reactor, ip, port)
        self.connectionOpen = copiedConnectProtocol(endpoint, self)
        self.finishedReceivingDefer = defer.Deferred() # Fire when complete received
        return (self.connectionOpen, self.finishedReceivingDefer)
        
    def lineReceived(self, line):
        """
        We got another line of the message
        """
        if not self.messageBegun:
            if line == "Invalid key":
                log.err("LargeMessageConsumer: Sent an invalid key. Cannot get the message.")
                self.finishedReceivingDefer.callback(False)
                self.erroredOut = True
            else:
                self.messageBegun = True
            
        self.totalMessage += line
        

    def connectionLost(self, reason):
        # Message is complete
        if not self.erroredOut:
            msgSize = len(self.totalMessage) / 1024
            log.msg("Large Message Consumer got the full message! Size[%s KB]" % msgSize, system="LargeMessageConsumer")
            
            if not isinstance(self.finishedReceivingDefer, defer.Deferred):
                raise Exception("connectionLost did not get deferred?!? %s" % self.finishedReceivingDefer)
            else:
                # Convert message back to object
                jellied = cPickle.loads(self.totalMessage)
                unjellied = jelly.unjelly(jellied)
                
                self.totalMessage = unjellied
                
                self.finishedReceivingDefer.callback(True)
                
        # TODO: What happens if we lose connection in the middle of a message?
        #       seems like finishedReceivingDefer will never be called.
        
        
if __name__ == '__main__':
    consumer = LargeMessageConsumer()
    consumer.openConnection('localhost', 1234)
    reactor.run()
