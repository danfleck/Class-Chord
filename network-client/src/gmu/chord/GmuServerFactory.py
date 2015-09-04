'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Mar 9, 2014

@author: dfleck
'''

from twisted.spread import pb
from twisted.python import log



class GmuServerFactory(pb.PBServerFactory):
    '''
    classdocs
    '''


    def __init__(self, *args, **kwargs):
        '''
        Constructor
        '''
        pb.PBServerFactory.__init__(self, *args, **kwargs)
        
        self.allProtocols = set()  # Store all the protocols which are active.
        
    def buildProtocol(self, addr):
        
        p = pb.PBServerFactory.buildProtocol(self, addr)
        p.notifyOnDisconnect(self.root.clientDisconnected)
        p.notifyOnConnect(self.clientConnected)
        
        #print("DEBUG: buildProtocol: %s" % p)
        self.p = p
        return p
        
        
    def clientConnectionMade(self, protocol):
        '''This is called when the connection to a specific Broker has been made.
           protocol is a spread.pb.Broker instance.
        '''
        #print("DEBUG: clientConnection made in GmuServerFactory! %s" % protocol)
        
        self.allProtocols.add(protocol)
        
        notifyFunc = lambda : self.protocolDisconnected(protocol)
        protocol.notifyOnDisconnect(notifyFunc)
        
    def protocolDisconnected(self, protocol):
        #print("DEBUG: clientConnection disconnected in GmuServerFactory! %s" % protocol)
        self.allProtocols.discard(protocol)
        
        
    def disconnectAll(self):
        '''Disconnect all protocols currently connected.'''
        
        log.msg("disconnectAll killing %d connections." % len(self.allProtocols), system="GmuServerFactory")
        
        for p in self.allProtocols:
            try:
                #p.transport.loseConnection()
                p.transport.abortConnection()
                log.msg("Transport killing connection.", system="GmuServerFactory")
            except Exception:
                log.err("Could not lose the connection in GmuServerFactory.")
                
        # Empty out
        self.allProtocols.clear()
            
        
    def clientConnected(self):
        '''When a connection happens, send the protocol to the Chord node
           in case we want it.
        '''
        self.root.clientConnected(self.p)
        