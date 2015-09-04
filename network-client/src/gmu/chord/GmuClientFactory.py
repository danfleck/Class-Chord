'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Mar 7, 2014

@author: dfleck
'''

from twisted.spread import pb


class GmuClientFactory(pb.PBClientFactory):
    '''
    classdocs
    '''
    def __init__(self, *args, **kwargs):
        pb.PBClientFactory.__init__(self, *args, **kwargs)
        self.connectionLostDefered = None
        self.didConnect = False
        
    def clientConnectionLost(self, connector, reason, reconnecting=0):
        pb.PBClientFactory.clientConnectionLost(self, connector, reason, reconnecting)
        
        if self.connectionLostDefered is not None:
            self.connectionLostDefered.callback(True)
        
    def setConnectionLostDefered(self, d):
        self.connectionLostDefered = d
        
        
    def clientConnectionFailed(self, connector, reason):
        
        pb.PBClientFactory.clientConnectionFailed(self, connector, reason)
        
        if self.connectionLostDefered is not None:
            self.connectionLostDefered.callback(True)
            
           
    def isConnected(self):
        '''Check if the Factory is still connected or not.'''
        return self._broker and not self._broker.disconnected
    
            