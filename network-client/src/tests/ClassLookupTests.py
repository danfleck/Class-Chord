'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

This code is used to test individual parts of the ClassLookup class.

Created on Dec 11, 2014

@author: dfleck
'''
from twisted.trial import unittest
from twisted.internet import task, defer
from twisted.python import log, failure

from gmu.chord import NetworkUtils, Config
from gmu.chord.NodeLocation import NodeLocation
from gmu.chord.MetricsMessageObserver import MetricsMessageObserver

from gmu.netclient.classChordNetworkChord import classChordNetworkChord
from gmu.chord import ClassLookup

import sys, random

class ClassLookupTests(unittest.TestCase):

   
    def setUp(self):
        self.origClassIdBits = Config.CLASS_ID_BITS
        Config.CLASS_ID_BITS = 16
        
    def tearDown(self):
        Config.CLASS_ID_BITS = self.origClassIdBits
   
    def testMessageInMyClass(self):
        '''Check that the messageInMyClass method works well.'''
       
        
        # Test for valid conditions 
        self.doMessageInMyClassTrue("00000000|11|22|33|44", "0000000011223344")
        self.doMessageInMyClassTrue("00000000|11-20|22|33|44", "0000000011223344")
        self.doMessageInMyClassTrue("00000000|5,11,20|22|33|44", "0000000011223344")
        self.doMessageInMyClassTrue("00000000|??|22|33|44", "0000000011223344")
        self.doMessageInMyClassTrue("00000000|?1|22|33|44", "0000000011223344")

        self.doMessageInMyClassTrue("0000000?|11|22|33|44", "0000000011223344")
        self.doMessageInMyClassTrue("0000000?|11|22|33|44", "0000000111223344")

        
        # Test for invalid conditions
        self.doMessageInMyClassFalse("00000000|19|22|33|44", "0000000011223344")
        self.doMessageInMyClassFalse("00000000|111|22|33|44", "0000000011223344")
        self.doMessageInMyClassFalse("00000000|9-10|22|33|44", "0000000011223344")
        self.doMessageInMyClassFalse("00000000|5,10,20|22|33|44", "0000000011223344")
        self.doMessageInMyClassFalse("00000000|9?|22|33|44", "0000000011223344")
        self.doMessageInMyClassFalse("00000000|?11?|22|33|44", "0000000011223344")
        self.doMessageInMyClassFalse("00000000|?11|22|33|44", "0000000011223344")
        
        
        
        
        
    def testGetNextHop(self):
        '''Check that the value returned from getNextHop is as expected.'''
        
        
        self.doGetNextHopTrue("00000000|11|22|33|44", "0000000011223344", "5555", "0000000011223344", "5556")
        
        self.doGetNextHopTrue("00000000|11|22|33|44", "0000000011223344", "5599", "0000000011223344", "5600")
        self.doGetNextHopTrue("00000000|11|22|33|44", "0000000011223344", "000", "0000000011223344", "001")
        
        self.doGetNextHopTrue("0000000?|11|22|33|44", "0000000011223344", "9999", "0000000111223344", "0000")

        self.doGetNextHopTrue("00000000|9?|22|33|44", "0000000094223344", "9999", "0000000095223344", "0000")

        self.doGetNextHopTrue("00000000,00000001|3?|22|33|44", "0000000039223344", "9999", "0000000130223344", "0000")
        self.doGetNextHopTrue("00000000-00000001|3?|22|33|44", "0000000039223344", "9999", "0000000130223344", "0000")

        self.doGetNextHopTrue("0000000?|3?|22|33|44", "0000000039223344", "9999", "0000000130223344", "0000")


        self.doGetNextHopTrue("00000000|11-12|22|33|44", "0000000011223344", "9999", "0000000012223344", "0000")
        
        self.doGetNextHopTrue("00000000|11,99|22|33|44", "0000000011223344", "9999", "0000000099223344", "0000")
        
        self.doGetNextHopTrue("00000000|11,99|22,23|33|44", "0000000011223344", "9999", "0000000011233344", "0000")
        
        # Need to test what happens when we have no next hop (nothing valid higher in the class)!
        # In these cases the code wraps to the lowest value (which should be the next hop).
        self.doGetNextHopTrue("00000000|11|22,77|33|44", "0000000011773344", "9999", "0000000011223344", "0000")
        self.doGetNextHopTrue("00000000|11|22|33|44", "0000000011223344", "9999", "0000000011223344", "0000")
        self.doGetNextHopTrue("00000000|11|22-44|33|44", "0000000011443344", "9999", "0000000011223344", "0000")
        self.doGetNextHopTrue("00000000|11|2?|33|44", "0000000011293344", "9999", "0000000011203344", "0000")
        

        
        
        # Now test the case where a node is NOT in the class, but gets the message.
        # The code should return the next higher valid class ID.
        self.doGetNextHopTrue("00000000|11|22|33|44", "0000000010223344", "5555", "0000000011223344", "0000")
        self.doGetNextHopTrue("0000000?|11|22|33|44", "0000000012223344", "5555", "0000000111223344", "0000")
        
        self.doGetNextHopTrue("00000000|11,44|22|33|44", "0000000010223344", "5555", "0000000011223344", "0000")
        self.doGetNextHopTrue("00000000|11,44|22|33|44", "0000000020223344", "5555", "0000000044223344", "0000")
        self.doGetNextHopTrue("0000000?|11,44|22|33|44", "0000000060223344", "5555", "0000000111223344", "0000")


        self.doGetNextHopTrue("00000000|11-44|22|33|44", "0000000010223344", "5555", "0000000011223344", "0000")
        self.doGetNextHopTrue("00000000|11-44|22|33|44", "0000000020223344", "5555", "0000000020223344", "5556")
        self.doGetNextHopTrue("0000000?|11-44|22|33|44", "0000000060223344", "5555", "0000000111223344", "0000")

        self.doGetNextHopTrue("00000000|5?|22|33|44", "0000000040223344", "5555", "0000000050223344", "0000")
        self.doGetNextHopTrue("0000000?|5?|22|33|44", "0000000060223344", "5555", "0000000150223344", "0000")
        

        # Need to test what happens when we have no next hop (nothing valid higher in the class and current is not valid)!
        # In these cases the code wraps to the lowest value (which should be the next hop).
        self.doGetNextHopTrue("00000000|11|22,77|33|44", "0000000011993344", "9999", "0000000011223344", "0000")
        self.doGetNextHopTrue("00000000|11|22|33|44", "0000009911223344", "9999", "0000000011223344", "0000")
        self.doGetNextHopTrue("00000000|11|22-44|33|44", "0000000011993344", "1000", "0000000011223344", "0000")
        self.doGetNextHopTrue("00000000|11|2?|33|44", "0000000011663344", "2000", "0000000011203344", "0000")
        

        
    def doGetNextHopTrue(self, spec, theID, uniqBits, expectedClassValue, expectedUniqBits):
        
        nextID = self.doGetNextHop(spec, theID, uniqBits, expectedClassValue, expectedUniqBits)
        
        nextID = nextID[Config.ENCLAVE_ID_BITS:]
        
        result = nextID == expectedClassValue + expectedUniqBits
        self.assertTrue(result, "doGetNextHopTrue failed %s|%s -> %s|%s  Actual:%s" % (theID, uniqBits, expectedClassValue, expectedUniqBits, nextID))

            
    def doGetNextHopNone(self, spec, theID, uniqBits, expectedClassValue, expectedUniqBits):
        
        nextID = self.doGetNextHop(spec, theID, uniqBits, expectedClassValue, expectedUniqBits)
        
        result = nextID == 'None'
        self.assertTrue(result, "doGetNextHopTrue failed %s|%s -> %s|%s  Actual:%s" % (theID, uniqBits, expectedClassValue, expectedUniqBits, nextID))

            
    def doGetNextHop(self, spec, theID, uniqBits, expectedClassValue, expectedUniqBits):
        # Build an envelope
        envelope = dict()
        envelope['destination'] = spec 
        
        # Build an ID 
        theID = self.buildNodeId(theID, uniqBits) 
        
        nextID = ClassLookup.getNextHop(theID, envelope)
        
        return str(nextID) 
        
            
    def doMessageInMyClassTrue(self, spec, theID):
        result = self.doMessageInMyClass(spec, theID)
        self.assertTrue(result, "messageInMyClassTrue failed [%s][%s]" % (theID, spec))
          
    def doMessageInMyClassFalse(self, spec, theID):
        result = self.doMessageInMyClass(spec, theID)
        self.assertFalse(result, "messageInMyClassFalse failed [%s][%s]" % (theID, spec))
          
                
    def doMessageInMyClass(self, spec, theID):
        # Build an envelope
        envelope = dict()
        envelope['destination'] = spec 
        
        # Build an ID 
        theID = self.buildNodeId(theID, "11111111") 
        
        # Check if it's right
        return ClassLookup.messageInMyClass(theID, envelope)
        
        
                

    def buildNodeId(self, classID, uniqBits):
        
        enclaveBits = "1" + ("0" * (Config.ENCLAVE_ID_BITS-1))
        
        if len(classID) != Config.CLASS_ID_BITS:
            raise Exception("Class ID must be the right number of bits! [%s] [%d] %d" % (classID, len(classID), Config.CLASS_ID_BITS))
        
        return enclaveBits + classID + uniqBits
        
        
        
        
        
        