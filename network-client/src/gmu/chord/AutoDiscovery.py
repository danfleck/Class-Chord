'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This module holds all the classes used for AutoDiscovery. It uses multicast and must 
have a multicast address and port specified. It does allow multiple nodes to share the 
same addr/port locally on a single machine.

Created on Oct 2, 2014

@author: dfleck
'''

from twisted.application import internet, service
from twisted.internet.protocol import DatagramProtocol
from twisted.python import log
from twisted.internet import reactor, defer

import sys
import Config

broadcastIsAlive= "TAPIO-ALIVE?:"
broadcastNodeLocation = "TAPIO-IS-ALIVE"

class AutoDiscoveryProtocol(DatagramProtocol):
    '''This is the protocol Bootstrap servers run to listen for local requests
      for auto-discovery.
    '''
    noisy = False

    def __init__(self, autoDiscoveryServer):
        
        # These are the parameters of the BootStrap Chord Node -- not the multicast addr/port!
        self.autoDiscoveryServer = autoDiscoveryServer
        self.sigRecvLength = len(broadcastIsAlive)

        
    def startProtocol(self):
        # Return a deferred which fires when join succeeds or fails
        return self.transport.joinGroup(Config.AUTO_DISCOVERY_MULTICAST_IP)
 

    def stopListening(self, _=None):
        '''Stop listening and shutdown. Returns a deferred.'''
        return self.transport.leaveGroup(Config.AUTO_DISCOVERY_MULTICAST_IP)
    
    def datagramReceived(self, datagram, (host, port) ):
        '''We received a broadcast message from another node!
           Tell them where we are.
        '''
        if datagram[:self.sigRecvLength] == broadcastIsAlive:
            # Parse it to get the enclave IDs being asked for
            fields = datagram.split(":") # TAPIO-ALIVE?:EnclaveID
            if len(fields) != 2:
                log.err("Got invalid packet back when looking for Bootstrap node. %s" % len(fields))
            else:
                
                enclaveIDrequested = fields[1]
                
                if enclaveIDrequested == 'ANY' or self.autoDiscoveryServer.hasEnclaveID(int(enclaveIDrequested)):
                    # Now we should respond
                    locMsg = self.autoDiscoveryServer.getLocMessage()
                    self.transport.write(locMsg, (Config.AUTO_DISCOVERY_MULTICAST_IP, Config.AUTO_DISCOVERY_PORT)) # Send it back to the originator (still using multicast though)
        
                

class BootstrapDiscoveryProtocol(DatagramProtocol):
    '''This is the protocol a node runs while it's looking for local servers during auto-discovery.
       After it finds a server, it should shutdown this protocol.       
    '''
    noisy = False
    

    def __init__(self, controller):
        self.sigRecvLength = len(broadcastNodeLocation)
        self.controller = controller

    def startProtocol(self):
        # Join the multicast address, so we can receive replies:
        # return a Deferred which fires when join succeeds or fails
        return  self.transport.joinGroup(Config.AUTO_DISCOVERY_MULTICAST_IP)
        
                
    def stopListening(self, _=None):
        '''Stop listening and shutdown. Returns a deferred.'''
        return self.transport.leaveGroup(Config.AUTO_DISCOVERY_MULTICAST_IP)
        
        

    def lookForNodes(self, enclaveRequested):
        '''Broadcast out a message looking for alive nodes.'''
        pingMsg = "%s%s" % (broadcastIsAlive, enclaveRequested)
        
        
        self.controller.clearNodeLocs()
        
        # Send to multicastIP:port - all listeners on the multicast address
        # (including us) will receive this message.
        self.transport.write(pingMsg, (Config.AUTO_DISCOVERY_MULTICAST_IP, Config.AUTO_DISCOVERY_PORT))
        
        if BootstrapDiscoveryProtocol.noisy:
            log.msg("SEND " + pingMsg)


    def datagramReceived(self, datagram, (host, port) ):
        '''We received a broadcast message from another node!
           Tell them where we are.
        '''

        if datagram[:self.sigRecvLength] == broadcastNodeLocation:
            # We got a message back which should have the IP/Port of a bootstrap 
            # node in it.
            fields = datagram.split(":")
            if len(fields) != 3:
                log.err("Got invalid packet back when looking for Bootstrap node. %s" % len(fields))
            else:
                self.controller.addNodeLoc(fields[1], fields[2])
                



class AutoDiscoveryServer(object):

    def __init__(self, chordNode):
        log.msg("Starting up AutoDiscoveryServer")
        self.chordNode = chordNode
        self.myIP = chordNode.nodeLocation.ip
        self.myPort = chordNode.nodeLocation.port
        
        self.makeService()


    def getLocMessage(self):
        locMsg = "%s:%s:%s" % (broadcastNodeLocation, self.myIP, self.myPort)
        return locMsg

    def hasEnclaveID(self, enclaveIDRequested):
        return self.chordNode.remote_hasEnclaveId(enclaveIDRequested)
        
        
    def makeService(self):
        self.proto = AutoDiscoveryProtocol(self)
        self.listenPort = reactor.listenMulticast(Config.AUTO_DISCOVERY_PORT, self.proto, listenMultiple=True)
        return self.proto
    
    @defer.inlineCallbacks
    def stopListening(self):
        '''Leave the multicast group and stop listening.'''
        
        try:
            # Leave the multicast group
            yield self.proto.stopListening()
            
            # Stop listening on the port
            yield self.listenPort.stopListening()
            
            # Log it.
            if AutoDiscoveryProtocol.noisy:
                log.msg( "AutoDiscoveryServer: stopListening completed successfully")
            
        except Exception, e:
            log.err(e, "AutoDiscoveryServer had an error trying to stopListening.")
            
        defer.returnValue(True)
            

class AutoDiscoveryClient(object):

    def __init__(self):
        self.nodeLocations = []
        self.lookingDeferred = None
        self.makeService()
        
        
    def makeService(self):
        self.proto = BootstrapDiscoveryProtocol(self)
        self.listenPort = reactor.listenMulticast(Config.AUTO_DISCOVERY_PORT, self.proto, listenMultiple=True)
        return self.proto
    
    @defer.inlineCallbacks
    def stopListening(self):
        '''Leave the multicast group and stop listening.'''
        
        try:
            # Leave the multicast group
            yield self.proto.stopListening()
            
            # Stop listening on the port
            yield self.listenPort.stopListening()
            
            # Kill the timer task if they are running
            if self.timedTask.active():
                self.timedTask.cancel()
            
            # Kill the deferred task
            if not self.lookingDeferred.called:
                self.lookingDeferred.cancel()
            
            # Log it.
            if AutoDiscoveryProtocol.noisy:
                log.msg( "AutoDiscoveryClient: stopListening completed successfully")
            
        except Exception, e:
            log.err(e, "AutoDiscoveryClient had an error trying to stopListening.")
            
        defer.returnValue(True)
        
    def lookForNodes(self, enclaveRequested='ANY', timeout=5):
        '''Look for bootstrap nodes, but timeout in timeout seconds.
           Returns a deferred which fires when a node is found or timeout.
        '''   
        self.lookingDeferred = defer.Deferred()
        self.proto.lookForNodes(enclaveRequested)
        self.timedTask = reactor.callLater(timeout, self.doTimeout)
        
        return self.lookingDeferred
        
    def doTimeout(self):
        '''We timed out waiting for nodes to respond. Exit.'''
        try:
            self.lookingDeferred.callback("TIMEOUT")
        except defer.AlreadyCalledError:
            pass
        
    def nodesFound(self):
        '''One or more nodes have been found! Yay, tell the caller.'''
        try:
            self.lookingDeferred.callback(self.nodeLocations)
            self.timedTask.cancel()
        except defer.AlreadyCalledError:
            pass            
        
        
    def getNodeLocationsFound(self):
        '''Get the list of node locations we've found so far.'''
        return self.nodeLocations
        
    def addNodeLoc(self, bsIP, bsPort): 
        if AutoDiscoveryProtocol.noisy:
            log.msg("RECV: Found Bootstrap INFO   IP:Port %s:%s " % (bsIP, bsPort))
        self.nodeLocations.append( (bsIP, bsPort ))  # Store for later!
        
        # Call the callback!
        self.nodesFound()
    
    def clearNodeLocs(self):
        self.nodeLocations = []




if __name__ == '__main__':

    
    # Start a server
    server = AutoDiscoveryServer("192.168.2.66", 99999)
    
    # Start a client
    client = AutoDiscoveryClient()
    reactor.callLater(2, client.lookForNodes)
    reactor.callLater(3, client.stopListening)
    #reactor.callLater(4, client.stopListening)
    
    log.startLogging(sys.stdout)
    reactor.run()

