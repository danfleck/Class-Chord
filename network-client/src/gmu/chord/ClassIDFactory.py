'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Specifies the class specs for each part of the class description, also 
can generate class IDs.

Created on Dec 12, 2014

@author: dfleck
'''

from ClassLookup import ClassAtom
import Config

# Computer type
WINDOWS = "10"
MAC = "11"
LINUX = "12"
#ANDROID = "13"
#IPAD = "14"

COMPUTER_TYPE_SPEC="10-12"


# Hardware type
LAPTOP = "20"
DESKTOP = "21"
SERVER = "22"

HARDWARE_TYPE_SPEC="20-22"


# User type
LOCAL = "30"
REMOTE = "31"

USER_TYPE_SPEC="30-31"


# # OS Level
# OS_LEVEL_SPEC = "00-20"
# 
# 
# # CLEARANCE
# NONE = "00"
# SECRET = "05"
# TS = "10"
# TSSCI = "20"
# 
# CLEARANCE_SPEC = "00,05,10,20"


def getClassIDNumChars():
    return Config.CLASS_ID_BITS

def generateSpec(computerType=None, hwType=None, userType=None):
    '''Return the class spec itself for use in a query.
       Any values of None will return the global spec (allowing any valid value)
    '''
    
    if computerType == None:
        computerType = COMPUTER_TYPE_SPEC

    if hwType == None:
        hwType = HARDWARE_TYPE_SPEC
        
    if userType == None:
        userType = USER_TYPE_SPEC
        
    return "%s|%s|%s" % (computerType,hwType,userType)
    
    
    
def generateID(computerType=None, hwType=None, userType=None):
    '''Any "None" value will be chosen randomly.'''
    
    
    if computerType == None:
        atom = ClassAtom(COMPUTER_TYPE_SPEC, 0)
        computerType = atom.debugGetRandomValue()

    if hwType == None:
        atom = ClassAtom(HARDWARE_TYPE_SPEC, 0)
        hwType = atom.debugGetRandomValue()
        
    if userType == None:
        atom = ClassAtom(USER_TYPE_SPEC, 0)
        userType = atom.debugGetRandomValue()
        
    classID = computerType + hwType + userType
    
    return classID