'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Provides class-based routing lookups

Created on Sept 20, 2014

@author: SReese
'''
from twisted.internet import defer
import Utils
from __builtin__ import str
import Config
import random

def messageInMyClass(myId, envelope):
    '''Check if this message matches my class ID.
        Return True/False 
    '''
    # Get the class spec from the envelope and break it into atoms
    atoms = getClassAtomsFromEnvelope(envelope)
    
    myId = str(myId)
    # Get the class value from myId
    classID = myId[ Config.ENCLAVE_ID_BITS : Config.ENCLAVE_ID_BITS + Config.CLASS_ID_BITS]
    
    #print("DEBUG: messageInMyClass: %s    %s " % (myId, classID))
    # Compare the ClassID by each of the atoms
    for a in atoms:
        if not a.isValid(classID):
            return False
    return True
    

def getNextHop(myId, envelope):
    '''Get the next valid hop given the node's ID and the envelope.
       If there is no valid next hop, this method can return None!
    '''

    # Get the class value from myId
    myId = str(myId) # Just in case!
    classID = myId[ Config.ENCLAVE_ID_BITS : Config.ENCLAVE_ID_BITS + Config.CLASS_ID_BITS]
    uniqBits = myId[Config.ENCLAVE_ID_BITS + Config.CLASS_ID_BITS : ]
    enclaveID = myId[0:Config.ENCLAVE_ID_BITS]
    
    if messageInMyClass(myId, envelope):
        # See if I can add one to it just by default? If it doesn't carry the one, then
        # I should be good. 
        allNines = True
        for digit in uniqBits:
            allNines = allNines and (digit == '9')
        
        if not allNines:
            uniqBitsInt = int(uniqBits) + 1
            uniqBitsStr = str(uniqBitsInt)
            # Pad to the right length
            origLen = len(uniqBits)
            paddingDigits = origLen - len(uniqBitsStr)
            return long(enclaveID+  classID + "0"*paddingDigits + uniqBitsStr)
    
    # At this point I'm going to need to update the class part because either:
    #     1. The Uniq ID was all 9s so we roll over to 0
    #     2. The current classID does not fit the spec and we need it to be.
    uniqBits = "0" * len(uniqBits)
    
    newClassID = ""
    # Get the class spec from the envelope and break it into atoms
    atoms = getClassAtomsFromEnvelope(envelope)

    # --------------------------------------------------
    # Find the first invalid atom starting from the left 
    # --------------------------------------------------
    for ind in range(len(atoms)): # Start at the "left" side of the ID (high order bits)
        a = atoms[ind]
        if not a.isValid(classID):
            # Then increment until it *is* valid, and set the remaining values to their 
            # min value
            firstInvalid = ind
            break
    else: 
        # They are all valid atoms, but we still need to increment the last one
        firstInvalid = len(atoms)-1
                     
    # --------------------------------------------------
    # Now the atoms to the left of firstInvalid should be okay, the ones to the right
    # need to be set to min values and the invalid one incremented.
    # --------------------------------------------------
    for ind in reversed(range(len(atoms))):
        if ind > firstInvalid:
            minValue = atoms[ind].getMinValue()
            newClassID = minValue + newClassID
            
        elif ind == firstInvalid:
            # Increment it
            (nextID, carry) = atoms[ind].getNextId(classID)
            newClassID = nextID + newClassID
        else:
            # These are to the left of first invalid
            
            # If carry was true, we have to increment it, 
            # otherwise we can keep the same value
            if carry:
                (nextID, carry) = atoms[ind].getNextId(classID)
                newClassID = nextID + newClassID                
            else:
                nextID = atoms[ind].getClassId(classID) 
                newClassID = nextID + newClassID
       
    
    if carry:
        # This means we wrapped, so we need to get the very minimum classID
        print("DEBUG: getNextHop   myId:%s   %s --> None " % (myId, classID))
        #print("DEBUG: getNextHop   enclaveID %s newClassID %s uniqBits %s " % (enclaveID, newClassID ,uniqBits))
        #return None
        return  long(enclaveID+ newClassID + uniqBits)
                
    else:    
        print("DEBUG: getNextHop  myId:%s    %s --> %s " % (myId, classID, newClassID))
        return long(enclaveID+ newClassID + uniqBits)
    
    
    


class ClassAtom(object):
    '''This class is a piece of a class ID.'''
    
    def __init__(self, specifier, startIndex):
        '''Specifier should be a string like one of these:
            "3456" --- Only valid value is exactly 3456
            "3,5,6" --- Valid values are 3 or 5 or 6
            "3-10"  --- Valid range is [3,10] (inclusive)
            "34?9"  --- Valid values are 3409-3499
        '''
        
        self.startIndex = startIndex
        
        self.specifier = specifier
        
          
        if not isinstance(specifier, str):
            raise Exception("Class Atom specifier must be a string! [%s]" % type(specifier))
        
        # Figure out the type
        if specifier.find("-") != -1:
            self.atomType = 'R' # Range   e.g. 56-99
            fields = specifier.split("-")
            if len(fields) != 2:
                raise Exception("Invalid range specifier. Must be Num-Num. Yours was [ %s ]" % specifier)
            self.rangeBegin = int(fields[0])
            self.rangeEnd = int(fields[1])
            
            self.numChars = len(fields[1])
            
        elif specifier.find(",") != -1:
            self.atomType = 'M' # Multiple Values    e.g. 7,8,10,33
            vals = specifier.split(",")
            self.multipleValues = [int(s) for s in vals]
            self.multipleValues.sort() # So they are ascending
            
            self.numChars = max( [ len(x) for x in vals] )
            
        elif specifier.find("?") != -1:
            self.atomType = 'W' # Wildcard     e.g. 45?  
            self.wildIndexes = []
            
            ind = self.specifier.find("?")
            while ind != -1:
                self.wildIndexes.append(ind)                
                ind = self.specifier.find("?", ind+1)
                
            temp = range(len(specifier))
            
            self.notWildIndexes = set(temp) - set(self.wildIndexes)
            
            self.maxWild = int (   "9" * len(self.wildIndexes) )
            
            self.totalMinWild = int( self.specifier.replace("?", "0") )
            self.totalMaxWild = int( self.specifier.replace("?", "9") )
            
            self.numChars = len(self.specifier)
                
        else:
            try:
                int(specifier)
                self.atomType = 'E' # Exact    e.g. 33349  
                self.numChars = len(self.specifier)
                
            except ValueError:
                raise Exception("Invalid specifier in ClassAtom: %s" % specifier)
            
        
        # This is a string formatting spec to get the right number of chars out.
        self.charSpec = "%%0%dd" % self.numChars  # ends up like this: %05d
                
        
    def debugGetRandomValue(self):
        '''Find a random valid value from the spec. This is just for testing.'''
        if self.atomType == 'E':
            return self.specifier # It's exact, can't change it!
        
        elif self.atomType == 'R':
            val = random.randint(self.rangeBegin, self.rangeEnd)
             
            
        elif self.atomType == 'M':
            ind = random.randint(0, len(self.multipleValues))
            val = self.multipleValues[ind]
            
        
        elif self.atomType == 'W':
            # Pull out the not-wild digits and make sure they match the not-wild digits
            # in the spec
            val = ""
            
            for digit in self.specifier:
                if digit == '?':
                    digit = random.randint(0,9)
                val += str(digit)
                
        return self._convertToString(val)

        
        
        
    def getClassId(self, fullIdString):
        '''Just return the unmodified part of the full ID this atom cares about.
        '''
        # Grab the right chars:
        idString = fullIdString[self.startIndex:self.startIndex+self.numChars]
        return idString

     
        
    def getNextId(self, fullIdString):
        '''Return the next ID in this class.
           Also return if the ID's wrapped (carry the one == True)
           Returns (nextID, carry)
        '''
        carry= False
        
        # Grab the right chars:
        idString = fullIdString[self.startIndex:self.startIndex+self.numChars]
        currentID = int(idString)
        
        if self.atomType == 'E':
            if currentID < int(self.specifier):
                return ( self.specifier, False) # It's exact, can't change it!
            else:
                return ( self.specifier, True) # It's exact, can't change it, but we need to increment
        
        elif self.atomType == 'R':
            
            nextID = currentID + 1
            
            if nextID > self.rangeEnd:
                nextID = self.rangeBegin
                carry = True
            else:
                carry = False
        
        elif self.atomType == 'M':
            for nextID in self.multipleValues:
                if nextID > currentID:
                    carry = False
                    break
            else:
                nextID = self.multipleValues[0]
                carry = True
        
        elif self.atomType == 'W':
            # Pull out the wild digits
            
            if currentID < self.totalMinWild:
                carry = False
                nextID = self.totalMinWild
            elif currentID >= self.totalMaxWild:
                carry = True
                nextID = self.totalMinWild
            else:
                wildDigits = [ idString[x] for x in self.wildIndexes ]
                wildInt = int("".join(wildDigits)) + 1
            
                # Need to replace the question marks with 
                # digits from the wildInt
                wildIntStr = str(wildInt).rjust(len(self.wildIndexes), "0") # result is zero padded string
                carry = False
                ind = 0
                nextID = ""
                for c in self.specifier:  # Seems like there should be a better way
                    if c == "?":
                        c = wildIntStr[ind]
                        ind += 1
                    nextID += c
                nextID = int(nextID)
                
            
        print("DEBUG: Spec:%s Orig:%s  Incremented: %s " % (self.specifier, idString, self._convertToString(nextID)))
        # Convert to a string and return    
        return self._convertToString(nextID), carry
            
            
    def _convertToString(self, anInt):
        '''Convert the int to a string with the right number of padded digits'''
        return self.charSpec % anInt
        
    
    def isValid(self, fullIdString):
        '''Is the idString passed in valid for this class?'''
        
        # Grab the right chars:
        idString = fullIdString[self.startIndex:self.startIndex+self.numChars]
        currentID = int(idString)

        #print("DEBUG: ClassLookup validating: %s  <--> %s " % (self.specifier, idString))
        
        if self.atomType == 'E':
            return idString == self.specifier # It's exact, can't change it!
        
        elif self.atomType == 'R':
            return  currentID >= self.rangeBegin and currentID <= self.rangeEnd 
            
        elif self.atomType == 'M':
            return currentID in self.multipleValues
            
        
        elif self.atomType == 'W':
            # Pull out the not-wild digits and make sure they match the not-wild digits
            # in the spec
            notWildDigits = [ idString[x] for x in self.notWildIndexes ]
            notWildSpec = [ self.specifier[x] for x in self.notWildIndexes ]
                
            
            return notWildDigits == notWildSpec
        
    def getMinValue(self):
        '''Get the minimum valid value for this atom.'''
        
        val = ""
        if self.atomType == 'E':
            val= self.specifier # It's exact, can't change it!
        
        elif self.atomType == 'R':
            val = self.rangeBegin 
            
        elif self.atomType == 'M':
            val =  self.multipleValues[0] # They are sorted

        elif self.atomType == 'W':
            # Set all wild digits to zero
            val =  self.specifier.replace("?", "0")
            
        if isinstance(val, int):
            val = self._convertToString(val)
            
        return val
                
        
        
        
    def length(self):
        return self.numChars
                
                
                        
def getClassAtomsFromEnvelope(envelope):        
    '''Create a list of class Atoms from the envelope.'''
    
    classSpec = envelope['destination']
    
    fields = classSpec.split("|")
    
    atoms = []
    
    startIndex = 0
    for f in fields:
        
        classAtom = ClassAtom(f, startIndex)
        atoms.append(classAtom)
        
        startIndex += classAtom.length()
    
    return atoms
            
    
        
        
        
class classLookup():
 
    def generateClassID(self, theJoinClass):
        '''Create the class specification
           Did not use a dictionary as they are
           not ordered by insertion.
        '''
        classSpec = "{0:016b}".format(int(theJoinClass))
                
        print ("classSpec: %s" % classSpec)
        return classSpec 
        
    def getClassFromID(self, nodeStr):
        '''Figure out the class ID from node location.'''
        # Convert the integer to binary                            
        nodeStr = bin(int(nodeStr))
        # Convert the class bits to 4 char int
        classString = "{0:04d}".format(int(nodeStr[18:34],2))
        #classSpec = "{0:016b}".format(int(classStr))
            
        return classString        
        
    def classSplit(self, classID):
        '''Not currently used
           Convert the 16 char class into something meaningful. 
           This was used when trying to squeeze in four classes.
        '''
        n = 4
        classItems = [classID[i:i+n] for i in range(0, len(classID), n)]
        return classItems

    def isValidClass(self, aClass, msgClass):
        '''aClass is an int, needs to be a string'''

        strClass = str(aClass)  
        
        diff = len(msgClass) - len(strClass)
        strClass = "0"*diff + strClass
        return self.isValidInClass(strClass, msgClass)

    def isValidInClass(self, msgID, msgClass):
        '''Compare the class to each successor
           and return when we get a match.
           "?" designates a wildcard character.
        '''        
        for index in range(len(msgClass)):        
                             
            if msgClass[index] != '?' and msgClass[index] != msgID[index]:
                
                return False 
                
        return True

    def getNextHop(self, myID, msgClass):
        '''myID is my current ID 
           msgClass is class description like: "0019" or "???8", etc...
           this method attempts to find the next valid class for the msg.
           Currently no logic to perform a query such as greater or less than.
        '''
        # Get the class from the ID
        classID = self.getClassFromID(myID)
        plusOne = str(int(classID)+1)
        if self.isValidInClass(plusOne,msgClass):
            return plusOne
        else:
            # Increase the class until it's valid            
            msgClass = str(msgClass)
            classID = str(classID)
            #print ("classID: %s" % classID)
            myClass = int(classID[0:len(msgClass)])
            #print ("myClass: %s" % myClass)
            tooHigh = int("1" + (len(msgClass) * "0"))
            nextClass = myClass + 1
            while nextClass < tooHigh:
                if self.isValidClass(nextClass, msgClass):
                    # Make the nextClass a String of the right length
                    strClass = str(nextClass)
                    diff = len(msgClass) - len(strClass)
                    strClass = "0"*diff + strClass

                    return strClass

                nextClass += 1
            
            return None


    @defer.inlineCallbacks
    def getSucc(self, nodeLoc, enclaveId):
        
        (factory, conn) = yield Utils.getRemoteConnection(nodeLoc)
        remoteRef = yield factory.getRootObject()

        succLoc = yield remoteRef.callRemote("getSuccessorLocation", int(enclaveId))
        
        if conn is not None:
            yield conn.disconnect()
        defer.returnValue(succLoc)
        