'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Mar 24, 2014

@author: dfleck
'''


from twisted.internet import defer, reactor, error
from twisted.spread import pb
from twisted.python import log, failure

import traceback, datetime
import glog
from GmuClientFactory import GmuClientFactory
import Config 
from FingerEntry import FingerEntry
import hashlib, random
import AutoDiscovery
from NodeLocation import NodeLocation
import ConnectionCache
from CopyEnvelope import CopyEnvelope

def wait(seconds=5):
    d = defer.Deferred()
    print("Waiting for %d seconds..." % seconds)
    # simulate a delayed result by asking the reactor to schedule
    # gotResults in 2 seconds time
    reactor.callLater(seconds, d.callback, True)
    return d    
    
            
def getEnclaveID(nodeId):
    '''Return an integer enclave ID from the NodeID'''
    
#     uniqBits = FingerEntry.m - Config.ENCLAVE_ID_BITS
#     
#     return nodeId >> uniqBits

    theEnclave = int(str(nodeId)[0:Config.ENCLAVE_ID_BITS])
    return theEnclave
    
def getEnclaveIDFromString(enclaveStr):
    '''Figure out the enclave ID from the enclaveString passed in.'''
    # Grab the bits for the enclave ID
    h = hashlib.new('sha1')
    h.update(enclaveStr)
    enclaveBits = str(int(h.hexdigest(), 16))
    
    # Get last 16 enclave bits
    enclaveBits = enclaveBits[-Config.ENCLAVE_ID_BITS:]
    enclaveBits= "1"+enclaveBits[1:] # Make them always start with a 1 to avoid 0 padding issues when converting to/from numbers.
    
    assert(len(enclaveBits) == Config.ENCLAVE_ID_BITS)

    
    enclaveNumStr = int(str(enclaveBits))
    return enclaveNumStr
    
@defer.inlineCallbacks
def findEnclaveNames(nodeLoc):
    '''Find the enclave names that the given nodeLoc knows about.'''
    
    names = None
    try:
        (factory, conn) = getRemoteConnection(nodeLoc)
        nodeRef = yield factory.getRootObject()
        
        names = yield nodeRef.callRemote("getEnclaveNames")
            
        yield disconnect(None, conn)

    except Exception, e:
        # Connection failed in some way
        glog.debug("findEnclaveNames failed to get names. %s" % e)
    defer.returnValue(names)
    
    
def showError(theErr):
    '''Show an error, but do not put in the error log.'''
    if isinstance(theErr, failure.Failure):
        log.msg("showError: This error has been handled, printing for debug\n[%s]\n\n[%s]" %
            (theErr.getErrorMessage(), theErr.getTraceback()))

    elif isinstance(theErr, Exception):
        log.msg("showError: This error has been handled, printing for debug\n[%s]\n\n[%s]" %
            (theErr.args[0], traceback.format_exc()))
    else:
        log.msg("showError: This error has been handled, printing for debug\n[%s]" % theErr)
            
        
            
def getRemoteConnection(nodeLocation, metricsMessageCounter=None):
    '''Return a factory and connection to a remote node'''
    

    
    if Config.USE_CONNECTION_CACHE:
        (factory, con) = ConnectionCache.connectTCP(nodeLocation.ip, nodeLocation.port, timeout=Config.NETWORK_CONNECTION_TIMEOUT, metricsCounter=metricsMessageCounter)
    else:
        if metricsMessageCounter != None:
            metricsMessageCounter.madeOutgoingConnection() # Counts attempts, because it's easier!
            
        d = defer.Deferred()
        d.addErrback(getConnErrorback)
        factory = GmuClientFactory(d)   # pb.PBClientFactory()
        factory.setConnectionLostDefered(d)# Fire this deferred when connection lost
        factory.noisy = False
        con = reactor.connectTCP(nodeLocation.ip, nodeLocation.port, factory, timeout=Config.NETWORK_CONNECTION_TIMEOUT)
        con.clientLostDefered = d # A little kludgy!
        
    return (factory, con)

def getConnErrorback(err):
    log.err("There was an error trying to get a connection. [%s]" % err )
    
    
@defer.inlineCallbacks
def getRemoteConnectionFromList(nodeLocationList, exclude=None, enclaveIDToFind='ANY', metricsMessageCounter=None):
    '''Given a list of nodeLocations, see if we can connect to any one of them!
       Do it randomly though to avoid burdening any one location.
        
       nodeLocationList is a list of node locations --> [nodeLoc1, nodeLoc2, etc...]
       exclude is a NodeLocation which will be skipped (so you don't connect to yourself).
       
       returns (theNode, factory, conn, nodeLocation) if successful or
               (False, False, False, False ) if not.
       
    '''

    if not nodeLocationList:
        defer.returnValue( (False, False, False, False ))
    
    orderedList = range(len(nodeLocationList))
    random.shuffle(orderedList) # Get a random order
    
    for index in orderedList:
        nodeLocation = nodeLocationList[index]
        
        # Skip?
        if exclude is not None and exclude.ip == nodeLocation.ip and exclude.port == nodeLocation.port:
            continue
        
        try:    
            #log.msg("DEBUG: getRemoteConnectionFromList: %s" % nodeLocation)
            (factory, conn) = getRemoteConnection(nodeLocation, metricsMessageCounter)
            #log.msg("DEBUG: getting Root Node: %s" % factory._root)
            d = factory.getRootObject()
            #log.msg("DEBUG: getRemoteConnectionFromList: D is:%s " % d)
            theNode = yield d
            #log.msg("DEBUG: getRemoteConnectionFromList: GOT ROOT")
            
            # Check the enclave
            validEnclave = True
            if enclaveIDToFind is not 'ANY':
                validEnclave = yield theNode.callRemote("hasEnclaveId",enclaveIDToFind)                
                        
            if validEnclave:
                # We succeeded, return it!
                defer.returnValue(  (theNode, factory, conn, nodeLocation) )
            else:
                yield disconnect(None, conn)
            
        except (error.ConnectionRefusedError, error.TimeoutError, error.ConnectError) as e:
            glog.debug("getRemoteConnectionFromList failed to connect to : %s. Trying another location." % nodeLocation)
            pass
        except Exception: 
            log.err()
                    
    # We couldn't get there.
    defer.returnValue( (False, False, False, False) )

def disconnect(aDeferredResult, connection):
    '''Starts the connection disconnect and returns a defered 
       which fires once the disconnect is complete.
       
       Relies on the connection being opened using Utils.getRemoteConnection 
    '''
    connection.disconnect()
    if Config.USE_CONNECTION_CACHE:
        # There is really no callback to call since the connection
        # truly isn't closed right away... it's cached.
        d= defer.Deferred()
        d.callback(True)
        return d
    else:
        try:
            return connection.clientLostDefered
        except AttributeError:
            glog.debug("Connection was not made using GmuClientFactory... continuing.")
            d= defer.Deferred()
            d.callback(True)
            return d
    
@defer.inlineCallbacks
def isAlive(nodeLocation):
    '''Check if a node is alive.'''
    
    rv = False
    
    try:    
        # Check if the succesor's pred is currentPred
        (factory, conn) = getRemoteConnection(nodeLocation)
        theNode = yield factory.getRootObject()
        
        response = yield theNode.callRemote("ping")
        rv = response == "PONG"
        #glog.debug("PING response in isAlive is [%s]:" % response)
    except (error.ConnectionRefusedError, error.TimeoutError, error.ConnectError) as e:
        pass
    except : 
        traceback.print_exc()        
    finally:
        # Close the connection
        disconnect(None, conn)
        
    #glog.debug("isALive returning %s for nodeLoc %s" % (rv, nodeLocation))
    defer.returnValue(rv)

def generateNodeID(aString, enclaveStr='localhost', classChars='0'):
    '''Generates the node's ID from 'aString'
       The enclave is added on as the high order bits. 
       
       The final ID looks like  enclaveBits | classChars | uniq ID bits
       So, if the class is 0xAA11, the enclave is 0xFF11 and the uniq ID is 0x12345 
       theId=0xFF11AA1112345   
       
       There is a misnomer here... the enclaveID and classChars are really chars, not bits       
    '''
    
    if enclaveStr is None:
        enclaveStr = ""
    
    aString = str(aString)
    totalDigits = len(str(2**FingerEntry.m)) - 1
    numUniqIDBits = totalDigits - (Config.ENCLAVE_ID_BITS + Config.CLASS_ID_BITS)

    h = hashlib.new('sha1')    
    h.update(aString)
    
    # Grab the bits from the ip/port hash
    aStringBits = bin(int(h.hexdigest(), 16))
    
    # Grab the bits for the enclave ID
    enclaveNumStr = str(getEnclaveIDFromString(enclaveStr))
    
    # Make sure that the class chars are the right length
    lenClassBits = len(classChars)
    if lenClassBits != Config.CLASS_ID_BITS:
        if lenClassBits > Config.CLASS_ID_BITS:
            raise Exception("classChars [%s] is to long to fit into Config.CLASS_ID_BITS chars [%d][%d]" % (classChars,lenClassBits,Config.CLASS_ID_BITS))
        else:
            # Pad out the class bits to the right length with zeros.
            classChars = "0"*(Config.CLASS_ID_BITS - lenClassBits) + classChars
            
    
    
    assert(len(classChars) == Config.CLASS_ID_BITS)
    
    
    # We already have the class bits from ClassLookup
    uniqStr = str( int (aStringBits[-numUniqIDBits:], 2)).zfill(numUniqIDBits)

    print("ENC CHARS: %s " % enclaveNumStr)
    print("CLASS Chars: %s " % classChars)


    #theId = enclaveBits+classChars+aStringBits[-numUniqIDBits:]
    theId = enclaveNumStr+classChars+uniqStr
    
    # Convert from a string to an int
    theId =  int(theId)
    
    # Check it!
    assert theId < 2**FingerEntry.m, "The node's ID generated is too large [%s|%s|%s" % (enclaveNumStr, classChars, uniqStr)
    
    return theId


@defer.inlineCallbacks
def findLiveBootstrapNode(bootstrapNodeList, enableAutoDiscovery, excludeNode=None, enclaveIDToFind="ANY"):
    '''Given the list and autodiscovery preference, find a live bootstrap node and return it's location.
       May return None if no live nodes can be found.
    '''
    
    # Check the list first.
    #log.msg("DEBUG: Utils.findLiveBootstrapNode: getting remote conn from list...")
    (bootstrapNode, _factory, conn, nodeLocation) = yield getRemoteConnectionFromList(bootstrapNodeList, exclude=excludeNode, enclaveIDToFind=enclaveIDToFind)
    #log.msg("DEBUG: Utisl.findLiveBootstrapNode: getting remote conn from list... GOT:%s" % bootstrapNode)
    
    if bootstrapNode == False:
        # Didn't find it.
        if enableAutoDiscovery:
            # Do auto-discovery
            
            # Now try to find the bootstrap node
            autoDiscoveryClient = AutoDiscovery.AutoDiscoveryClient()
            result = yield autoDiscoveryClient.lookForNodes(enclaveIDToFind)
            autoDiscoveryClient.stopListening()  # We're done listening
            if result == 'TIMEOUT':
                log.msg("findLiveBootstrapNode timed out looking for bootstrap nodes.")
                defer.returnValue( None )
            else:
                # At this point we should have some (ip, port) pairs int result.
                (bsIP, bsPort) = result[0]
                nodeLoc = NodeLocation(None, bsIP, int(bsPort))
                defer.returnValue(nodeLoc)
                
        else: # AutoDiscovery is disabled
            defer.returnValue( None )
        
    else:
        # Close the connection
        disconnect(None, conn)
        
        # Return the location
        defer.returnValue( nodeLocation )
        
        
