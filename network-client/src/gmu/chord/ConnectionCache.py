'''
This module will cache connections for the code. It essentially functions as a singleton and
a global connection cache. 

Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Nov 24, 2014

@author: dfleck
'''

from twisted.internet import defer, reactor, error
import Config, types, Utils
from datetime import datetime, timedelta
from GmuClientFactory import GmuClientFactory


# Store the connections and factory in a tuple
connections = dict()

def connectTCP(ip, port, timeout=Config.NETWORK_CONNECTION_TIMEOUT, metricsCounter=None):
    '''Get a connection from the cache if possible, if not, 
       create a new connection.
    '''
    
    vals = _getConnection(ip, port, timeout)
    #print("DEBUG: connectTCP: to:%s:%s  got from cache? %s " % (ip, port, vals != None))
    if vals == None:
        #print("DEBUG: connectTCP: making connection")
        (factory, conn) = _makeConnection(ip, port, timeout)        
        #print("DEBUG: connectTCP: did I make it?")
        if metricsCounter != None:
            metricsCounter.madeOutgoingConnection() # Counts attempts, because it's easier!
    else:
        # We got it from the cache!
        (factory, conn) = vals
        
    #print("DEBUG: connectTCP: returning... %s %s" % (factory, conn))

    return (factory, conn)
    
def _getConnection(ip, port, timeout):
    
    '''Get a connection from the dict if possible.'''
    key = _makeKey(ip, port, timeout)
    
    if key in connections:
        (factory, conn) = connections[key]
        
        # GmuClientFactory knows if the broker is connected or not.
        if factory.isConnected():
            conn._gmuConnects += 1
            return (factory, conn)
        
    return None
        
def _printCache():
    for key in connections.iterkeys():
        
        (factory, conn) = connections[key]
        
        print("Connection Cache Key: %s  : Connected: %s" % (key, factory.isConnected()) )
    
def disconnectAll():
    '''Attempt to disconnect all connections in the cache. Usually done
       when the node is leaving.
    '''
    for (factory, conn) in connections.itervalues():
        try:
            conn.disconnect()
        except Exception, e:
            print("DEBUG: disconnectAll: %s" % e)
            
            
    
    
def cacheDisconnect(self):
    '''Someone asked to disconnect'''
    
    self._gmuDisconnects += 1
    self._gmuLastDisconnectTime = datetime.now()
    
    
    if self._gmuDisconnects >= self._gmuConnects:
        reactor.callLater(Config.CONNECTION_CACHE_DELAY, _gmuFinalDisconnect, self)
        
def _gmuFinalDisconnect(self):
    '''Cache timer has expired, do a real disconnect if needed.'''
    
    if self._gmuDisconnects >= self._gmuConnects:
        # Check the time to be certain we should disconnect
        cacheTime = timedelta(seconds = Config.CONNECTION_CACHE_DELAY)
        if self._gmuLastDisconnectTime + cacheTime < datetime.now():
            #print("DEBUG: ConnectionCache really disconnecting. %s" % self._gmuKey)
            self._origDisconnect()
            try:
                del connections[self._gmuKey]
            except KeyError:
                pass 
        
    
    
    
    
def _makeKey(ip, port, timeout):
    return "%s:%s:%s" % (ip, port, timeout)
    
def _makeConnection(ip, port, timeout):
    '''Make a new connection.'''
    
    d = defer.Deferred()
    d.addErrback(Utils.getConnErrorback)
    factory = GmuClientFactory(d)   # pb.PBClientFactory()
    factory.setConnectionLostDefered(d)# Fire this deferred when connection lost
    factory.noisy = False
    
    con = reactor.connectTCP(ip, port, factory, timeout)
    
    # Store it in the dict
    key = _makeKey(ip, port, timeout)
    connections[key] = (factory,con)
    
    # Override the conncetion's "disconnect" method so it remains cached.
    con._origDisconnect = con.disconnect
    con.disconnect = types.MethodType(cacheDisconnect, con)
    #con._gmuFinalDisconnect = types.MethodType(_gmuFinalDisconnect, con)
    
    # Some cache specific vars
    con._gmuConnects = 1
    con._gmuDisconnects = 0
    con._gmuLastDisconnectTime = None
    con._gmuKey  = key
    
    return (factory,con)
    

