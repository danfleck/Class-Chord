'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Apr 9, 2014

@author: dfleck
'''

class MessageCache(list):
    '''
    Holds a list of tuples (envelope, message) 
    '''


        
    def __contains__(self, otherEnvelope):
        #print("\n-- Message Cache Check -- ")
        for value in self:
            (envelope, message) = value
            #print("MessageCache comparing: [%s][%s]" % (otherEnvelope['msgID'], envelope['msgID']))
            if envelope['msgID'] == otherEnvelope['msgID']:
                #print("Message Cache: TRUE\n -- ")
                return True
        #print("Message Cache: FALSE\n -- ")
        return False
        
        
