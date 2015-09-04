'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Jun 3, 2014

@author: dfleck
'''
from gmu.netclient.classChordClientAPI import classChordClientAPI
from gmu.chord.FingerEntry import FingerEntry
from gmu.chord import Config
import hashlib

class SampleClient(classChordClientAPI):
    '''
    Dummy implementation of the Invincea Client
    '''
    def __init__(self, ip, port, messageReceivedObserver):
        '''
        Constructor
        '''
        self.ip = ip
        self.port= port
        self.messageReceivedObserver = messageReceivedObserver
        

    def generateNodeID(self, enclaveStr):
        '''
        Generates a node ID
        
        http://docs.python.org/2/library/uuid.html
        http://stackoverflow.com/questions/2461141/get-a-unique-computer-id-in-python-on-windows-and-linux

        OS Platform System:
        https://github.com/hpcugent/easybuild/wiki/OS_flavor_name_version
        
        Generates the node's ID from it's IP addr/port.
        This is the same as Chord's default way, but anything unique would work.
        FingerEntry.m is the number of bits
        
        return value is an int
        '''
        
        numUniqIDBits = FingerEntry.m - Config.ENCLAVE_ID_BITS

        h = hashlib.new('sha1')
        h.update(self.ip)
        h.update(str(self.port))
        
        # Grab the bits from the ip/port hash
        ipPortBits = bin(int(h.hexdigest(), 16))
        
        # Grab the bits for the enclave ID
        h = hashlib.new('sha1')
        h.update(enclaveStr)
        enclaveBits = bin(int(h.hexdigest(), 16))
        
        # Glue them together
        theId = enclaveBits[-Config.ENCLAVE_ID_BITS:]+ipPortBits[-numUniqIDBits:]
        
        #print("Num uniq bits:%d   encBits:%d" % (numUniqIDBits, Config.ENCLAVE_ID_BITS))
        #print('generatedID DEBUG: \n  %0x   \n  %0x   \n  %0x\n\n' % (int(ipPortBits,2), int(enclaveBits,2), int(theId,2)))
        
        # Convert from a binary string to an int
        theId =  int(theId,2)
                
        return theId
         
              
    def receiveMessage(self, message, envelope):
        '''
        Handle incoming message
        Message is a list containing envelope and message
        '''
        
        if self.messageReceivedObserver is not None:
            self.messageReceivedObserver(message, envelope)
        

        
    def authMessage(self, message, envelope):
        '''
        Authenticate incoming message
        Return True for Successfull authentication
        Else Return False
        '''
        return True

    def authenticateJoin(self, messagePayload):
        ''' This will return True or False on join allowable or not and 
            return a response payload which should be sent to the joining-client.
            
            return (True, payload)
            
            and this payload will be returned on joinComplete to the joining nodes.
        '''  
        (name, val) = messagePayload.split(":")
        print("DEBUG: SampleClient authenticating join with: %s" % messagePayload)
        if name == "authenticate":
            return (val == "succeed", "return payload from SampleClient")
        
        raise Exception("Sample Client doesn't understand messagePayload:%s " % messagePayload)
    
    
    def decryptEnvelope(self, encryptedEnvelope):
        '''
        Returns the un-encrypted Envelope of the Message Packet
        '''
        return encryptedEnvelope

    def decryptMessage(self, encryptedMessage):
        '''
        Returns the un-encrypted Message
        '''
        return encryptedMessage
        
        
        