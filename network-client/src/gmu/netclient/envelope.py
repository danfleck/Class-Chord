'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This is the message envelope. It is really just a dictionary, but we want to 
pre-define the allowable keys.

Created on Apr 3, 2014

@author: dfleck
'''
import datetime

class Envelope(dict):
    '''
    This is the message envelope. It is really just a dictionary, but we want to 
    pre-define the allowable keys.
    '''

    _allowedKeys = ["ttl",    # Holds a timstamp value (seconds since epoch) after which this packet should be discarded
                    "msgID",  # Unique ID for this message
                    "source", # Chord: it is a nodeLocation instance
                    "type",   # message type -- valid values are "agg-response", "class", "flood", or "p2p" (point to point)
                    "destination", # Chord: it is a NodeID; UUID of the destination for p2p or class specification for class. For a flooding
                                  # message leave it blank.
                                  # For a class message this should be a specifier like this:   232|5-9|11?0|1,2
                                  

                    "enclave", # Blank for p2p messages
                               # For flooding can be "ALL", blank=default enclave name or an enclave name
                               # For aggregation response can be "ALL", blank=default, or an enclave name however
                                    # it will be replaced by a single enclave since the messages are truly 
                                    # destined for a single node specified in "destination"

                    "nodeCounter", # If this is present in a flooding message, then the nodes
                                  # will count off and set their nodeIndex. 
                                  # The node index is used for agg-response messages, so 
                                  # at some point before an agg-response this message should be sent out.
                                  # It's used in the code for aggregation messages to decide if you're an 
                                  # aggregation point. 
                                  
                    "aggregationNumber", # Only valid for agg-response messages. This is the number of responses to 
                                        # aggregate before sending to final dest.
                    
                    "classQuery", # Default for all but unique class queries
                                  # Can be a numeric string with "?" as a wildcard
                    ]

 
        
    def __setitem__(self, key, val):
        if key not in Envelope._allowedKeys:
            raise KeyError("key not in set of valid envelope keys")
        
        if key == 'ttl':
            self.validateTTL(val)
        dict.__setitem__(self, key, val)        
        
    def validateTTL(self, val):
        '''TTL must be a datetime!'''
        return isinstance(val, datetime.datetime) 
        
        
        
    def isValid(self):
        '''Is this a valid, complete envelope? Basically, does it have all the keys?'''
        valid = True
        for keyName in self._allowedKeys:
            if keyName not in self:
                print("Error: Envelope is missing key [%s]" % keyName)
                valid = False
        
        return valid
    
        