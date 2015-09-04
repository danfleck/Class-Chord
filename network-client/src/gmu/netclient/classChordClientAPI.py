'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Jun 3, 2014

@author: dfleck
'''

class classChordClientAPI(object):
    '''
    This is a stub class which defines the API needed for any upper client object.
    The network layer uses these methods by calling them on a provided "classChordClientAPI" instance.
    
    Implementation classes should inherit from this.
    '''


     
    def authenticateJoin(self, messagePayload):
        ''' This will return True or False on join allowable or not and 
            return a response payload which should be sent to the joining-client.
            
            return (True, payload)
            
            and this payload will be returned on joinComplete to the joining nodes.
        '''  
        raise NotImplementedError()
        
               
    def receiveMessage(self, message, envelope):
        '''
        Handle incoming message
        '''
        raise NotImplementedError()

        
    def authMessage(self, message, envelope):
        '''
        Authenticate incoming message
        Return True for Successfull authentication
        Else Return False
        '''
        raise NotImplementedError()

    
    
    def decryptEnvelope(self, encryptedEnvelope):
        '''
        Returns the un-encrypted Envelope of the Message Packet
        '''
        raise NotImplementedError()

    def decryptMessage(self, encryptedMessage):
        '''
        Returns the un-encrypted Message
        '''
        raise NotImplementedError()
        
        