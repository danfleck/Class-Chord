'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This will just hold some common functions used for testing.

Created on Aug 15, 2014

@author: dfleck
'''

from twisted.internet import defer,reactor
from twisted.python import log


from gmu.chord.ChordNode import ChordNode
from gmu.chord.GmuServerFactory import GmuServerFactory
from gmu.chord.MetricsMessageObserver import MetricsMessageObserver
from gmu.chord.CopyEnvelope import CopyEnvelope
from gmu.chord.FingerEntry import FingerEntry
from gmu.chord import Config, Utils, ClassIDFactory

from SampleClient import SampleClient
from gmu.netclient.classChordNetworkChord import classChordNetworkChord

import datetime, random, hashlib

from TestMessageObserver import TestMessageObserver

import os, psutil


import socket
from socket import AF_INET, SOCK_STREAM, SOCK_DGRAM

AD = "-"
AF_INET6 = getattr(socket, 'AF_INET6', object())
proto_map = {
    (AF_INET, SOCK_STREAM): 'tcp',
    (AF_INET6, SOCK_STREAM): 'tcp6',
    (AF_INET, SOCK_DGRAM): 'udp',
    (AF_INET6, SOCK_DGRAM): 'udp6',
}

loggingOn = False
import sys

def printLogger(aDict):
    print(aDict)
    sys.stdout.flush()
    
@defer.inlineCallbacks        
def waitForConnectionCache(_=None):
    if Config.USE_CONNECTION_CACHE:
        log.msg("Waiting for ConnectionCache to close out...")
        yield wait(Config.CONNECTION_CACHE_DELAY + 2)
            

def generateID(ipAddr, port, theEnclave, classID):
    '''Generates the node's ID from it's IP addr/port.
       The enclave is added on as the high order bits. 
       
       The final ID looks like   enclaveBits | uniq ID bits
       So, if the enclave is 0xFF11 and the uniq ID is 0x12345 theId=0xFF1112345          
    '''
    
    if classID == None:
        # Build a random class spec
        classID = ClassIDFactory.generateID()
    
    h = hashlib.new('sha1')
    h.update(ipAddr)
    h.update(str(port))
    
    # Grab the bits from the ip/port hash
    ipPortBits = bin(int(h.hexdigest(), 16))
    
    ipPortInt = int(ipPortBits, 2)
    
    theId = Utils.generateNodeID(ipPortInt, theEnclave, classChars=classID)
        
    return theId
    
@defer.inlineCallbacks
def startupBootstrapNode(ip, port=12345, enclave='localhost', classID=None):
    '''Start a Bootstrap node'''
        
    # Generate an ID
    nodeID = generateID(ip, port, enclave, classID) 
    print("DEBUG: BS nodeID is %s" % nodeID)
    
    bsNode = ChordNode(ip, port, nodeID)
    serverFactory = GmuServerFactory(bsNode, unsafeTracebacks=True)
    MetricsMessageObserver(bsNode)
    testObserver = TestMessageObserver(bsNode)
    enableAutoDiscovery = False
    
    status = yield bsNode.join(None, enableAutoDiscovery,  enclave, None, True, serverFactory)
    
    defer.returnValue( (status, bsNode, testObserver) )


@defer.inlineCallbacks    
def startupClientNode(ip, port, enclave, bootstrapNodeLocation, allowFloatingIP=True, classID=None):
    '''Start a client node and connect it to the network.
    
       Returns  (status=True|False, node)
    '''
    # Generate an ID
    nodeID = generateID(ip, port, enclave, classID)
    print("DEBUG: Client nodeID is %s" % nodeID)    
    
    normalNode = ChordNode(ip, port, nodeID, allowFloatingIP)
    MetricsMessageObserver(normalNode)
    testObserver = TestMessageObserver(normalNode)
    enableAutoDiscovery = False
    
    #import TestUtils
    #yield TestUtils.wait(4)
    authenticationPayload = "open-sesame"
    status = yield normalNode.join([ bootstrapNodeLocation ] , enableAutoDiscovery, enclave, authenticationPayload, False)
    defer.returnValue( (status, normalNode, testObserver) )


@defer.inlineCallbacks
def startNodeUsingAPI(ip, port, bootstrapNodeLocation, enclaveStr, useAutoDiscover, isBootstrapNode):
    '''Starts up a client node but uses the API.
       Once complete returns a tuple (joinStatus, clientAPI, networkAPI)
       
       Return a deferred which should fire after join succeeds. 
       
    '''
    d = defer.Deferred()
    
    clientAPI = SampleClient(ip, port, None)
    networkAPI = classChordNetworkChord(clientAPI, port, ip)
    nodeID = networkAPI.generateNodeID(str(port), enclaveStr) # Get the ID with the bits on it we need. Use "port" because it'll be uniq for tests
    
    # Join the network
    if bootstrapNodeLocation is None:
        bootstrapNodeList = None
    else:
        bootstrapNodeList = [ bootstrapNodeLocation ]
        
    callFunc = lambda result, payload: shouldSucceedCallback(result, payload, d)
    networkAPI.start(callFunc, nodeID, enclaveStr, "authenticate:succeed", bootstrapNodeList, isBootstrapNode, useAutoDiscover)
    
    # Wait for the join to finish
    joinStatus = yield d  # This is the value returned from shoudlSucceedCallback

    # Now return everything    
    defer.returnValue( (joinStatus, clientAPI, networkAPI) )

    

def shouldSucceedCallback(result, payload,  deferToFire):
    '''Status callback from the networkAPI.start call. Should be true.
       Uses a lambda function so we can sneak in a few more parameters :-)
    '''
    
    if result:
        deferToFire.callback(result)
    else:
        deferToFire.errback(result)
        
def defWait(dummy, seconds=5):
    return wait(seconds)

def wait(seconds=5):
    d = defer.Deferred()
    print("Waiting for %d seconds..." % seconds)
    # simulate a delayed result by asking the reactor to schedule
    # gotResults in 2 seconds time
    reactor.callLater(seconds, d.callback, True)
    return d




def sendFlood(chordNode,messageNum,enclave, data=""):
    '''Send a flooding message to the enclave specified. Content is "messageNum".
       Returns a deferred status
    '''
    # Send out a flooding message
    msgText = { "type" : "STORE", "loc" : chordNode.nodeLocation, "msgNum" : messageNum, "data" : data }
    
    # Build the envelope
    env = CopyEnvelope()
    env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
    env['source'] = chordNode.nodeLocation
    env['type'] = 'flood'
    env['enclave'] = enclave # Flooding
    env['msgID'] = random.getrandbits(128) # TODO: Something better here!

    # Send the message
    d = chordNode.sendFloodingMessage(msgText, env)
    
    return d

        

def sendP2P(src, dst, messageNum, data=""):
    '''Send a P2P message to the dst node specified. Content is "messageNum".
       Returns a defered status
    '''
    # Send out a flooding message
    msgText = { "type" : "STORE", "loc" : src.nodeLocation, "msgNum" : messageNum, "data": data }
    
    # Build the envelope
    env = CopyEnvelope()
    env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
    env['source'] = src.nodeLocation
    env['type'] = 'p2p'
    env['destination'] = dst.nodeLocation.id 
    env['msgID'] = random.getrandbits(128) # TODO: Something better here!

    # Send the message
    d = src.sendSyncMessage(msgText, env)
        
    return d

def sendClassQuery(src, classSpec, messageNum, data=""):
    '''Send a Class query message to the class spec specified. Content is "messageNum".
       Returns a deferred status
    '''
    # Send out a class message
    msgText = { "type" : "STORE", "loc" : src.nodeLocation, "msgNum" : messageNum, "data": data }
    
    # Build the envelope
    env = CopyEnvelope()
    env['ttl'] = datetime.datetime.now() + datetime.timedelta(minutes=10)
    env['source'] = src.nodeLocation
    env['type'] = 'classType'
    env['destination'] = classSpec
    env['msgID'] = random.getrandbits(128) # TODO: Something better here!

    # Send the message
    d = src.sendClassMessage(msgText, env)
        
    return d




def didNodeReceive(observers, node, messageNum):
    '''Return True if nodes received the message, False otherwise.'''
    for testObserver in observers:
        if testObserver.chordNode == node:
            return testObserver.messageNumStored(messageNum)
       
    log.err("didNodeReceive: could not find observer.")
    return False

def didReceive(observers, enclave, messageNum, expectedCount):
    '''Return True if all nodes received the message, False otherwise.'''
    actualCount = 0
    
    for testObserver in observers:
        if enclave == 'ALL' or enclave in testObserver.chordNode.remote_getEnclaveNames():
            if testObserver.messageNumStored(messageNum):
                actualCount += 1
        
    log.err("didReceive: actualCount:%d  expectedCount:%d" % (actualCount, expectedCount))
    return actualCount == expectedCount
    

def didNotReceive(observers, enclave, messageNum, expectedCount):
    '''Return True if all nodes did not receive the message, False otherwise.'''
    actualCount = 0
    
    for testObserver in observers:
        if enclave == 'ALL' or enclave in testObserver.chordNode.remote_getEnclaveNames():
            if not testObserver.messageNumStored(messageNum):
                actualCount += 1
        
    if actualCount != expectedCount:
        log.err("didNotReceive: actualCount:%d  expectedCount:%d" % (actualCount, expectedCount))
    return actualCount == expectedCount

    
def showOpenConnections():
    # Get my PID
    pid = os.getpid()
    
    # Run the lsof command
    p = psutil.Process(pid)
    
    conns = p.connections()
    
    templ = "%-5s %-30s %-30s %-13s %-6s "
    print(templ % (
        "Proto", "Local address", "Remote address", "Status", "PID" ))
        
    # Print output
    for c in conns:
        laddr = "%s:%s" % (c.laddr)
        raddr = ""
        if c.raddr:
            raddr = "%s:%s" % (c.raddr)
        print(templ % (
            proto_map[(c.family, c.type)],
            laddr,
            raddr or AD,
            c.status,
            pid or AD            
        ))    
        
    print("Total net connections: %d" % len(conns))
