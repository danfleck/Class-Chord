'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This is a group of network utilities we need. (Mostly for IP addresses)

Created on Apr 30, 2014

@author: dfleck
'''

import socket, platform, sys

from twisted.internet import defer, reactor, task, error
from twisted.python import log
from twisted.spread import pb

import struct
import array
import socket

from gmu.chord import Utils, Config

# Conditional import
try:
   import fcntl
except ImportError:
   print("Could not import fcntl. Okay on Windows.")


class IPNode(pb.Root):
    
    def __init__(self):
        None
        
    @defer.inlineCallbacks
    def getRemoteIP(self, nodeLocation):
        '''Ask the nodeLocation node for my IP.'''
        
        # Connect to the remote node
        (factory, conn) = Utils.getRemoteConnection(nodeLocation)
        try:
            nodeRef = yield factory.getRootObject()
        
            # Ask for the IP
            myIP = yield nodeRef.callRemote("getClientIP")
        except Exception, e:
            log.err()
            raise e
        finally:
            # Close the connection to closerNodeLoc
            conn.disconnect()

        # Return it
        defer.returnValue(myIP)
        

def getNonLoopbackIP(bootstrapIP=None, bootstrapPort=None, preferredIPPrefix=None):
    '''Return the IP address of the box, but not the loopback one!'''
    
    try:
        myIP = socket.gethostbyname(socket.gethostname())
        if not myIP.startswith("127.0"):
            # We got it the easy way!
            return myIP
    except Exception, e:
        log.err("Couldn't get IP using gethostbyname. [%s]" % e, system="NetworkUtils")
    
    if platform.system() == "Linux":
        import fcntl

        myIP = getLinuxIP(preferredIPPrefix)
        if myIP is not None:
            return myIP
        else:
            raise Exception("This Linux device is not connected to the network.");
         
    else:
        if Config.ALLOW_LOCAL_IP_ADDRESS:
            return "127.0.0.1"
        else:
            raise Exception("Could not find non-local IP address. No one will be able to connect")
         
    

def getLinuxIP(preferredIPPrefix=None):
    '''Get the IP address using Linux specific commands.
       If present, look for the preferred IP Prefix (preferredIPPrefix)'''
    
    currentIP = None
    interfaces = all_interfaces()
    for i in interfaces:
        (name, ip) = i
        
        if name.startswith('eth') or name.startswith('wlan'):
            if currentIP == None:
                currentIP = ip
            elif preferredIPPrefix != None and ip.startswith(preferredIPPrefix):
                currentIP = ip
            
    if currentIP == None:
        log.err("NetworkUtils could not find eth inet address", system="NetworkUtils")
    return currentIP 
    
    
    
    
def all_interfaces():
    '''Get all network interfaces on 64 or 32 bit Unixes.
       Code from: http://code.activestate.com/recipes/439093-get-names-of-all-up-network-interfaces-linux-only/
    '''
    is_64bits = sys.maxsize > 2**32
    struct_size = 40 if is_64bits else 32
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    max_possible = 8 # initial value
    while True:
        bytes = max_possible * struct_size
        names = array.array('B', '\0' * bytes)
        outbytes = struct.unpack('iL', fcntl.ioctl(
            s.fileno(),
            0x8912,  # SIOCGIFCONF
            struct.pack('iL', bytes, names.buffer_info()[0])
        ))[0]
        if outbytes == bytes:
            max_possible *= 2
        else:
            break
    namestr = names.tostring()
    return [(namestr[i:i+16].split('\0', 1)[0],
             socket.inet_ntoa(namestr[i+20:i+24]))
            for i in range(0, outbytes, struct_size)]



def set_portMapping(port):
    import miniupnpc # This import will fail on Android


    u = miniupnpc.UPnP()
    u.discoverdelay = 200
    eport = port
    result = False
    try:
        print "Discovering... delay=%ums" % u.discoverdelay
        ndevices = u.discover()
        u.selectigd()
        print "local ip address: ", u.lanaddr
        externalipaddress = u.externalipaddress()
        print 'external ip address :', externalipaddress
        print u.statusinfo(), u.connectiontype()
        
        # find a free port for mapping
        r = u.getspecificportmapping(eport, 'TCP')
        while r != None and eport < 65536:
            eport = eport + 1
            r = u.getspecificportmapping(eport, 'TCP')
        
        print 'trying to redirect %s port %u TCP => %s port %u TCP' % (externalipaddress, eport, u.lanaddr, port)
        b = u.addportmapping(eport, 'TCP', u.lanaddr, port,'UPnP IGD external port %u' % eport, '')
        if b:
            print 'Success. Now waiting for connections on external IP: %s  port: %u' % (externalipaddress, eport)
            result = True
        else:
            print 'Failed'
            result = False
                    
    except Exception, e:
        print 'Exception: ', e
        
    return result






    
    
    
