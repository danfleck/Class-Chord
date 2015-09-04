'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Holds a fixed size list of successor locations (in order).

Created on Mar 24, 2014

@author: dfleck
'''


from twisted.internet import defer, error
from twisted.spread import pb
from twisted.python import log

import Config
import glog
import Utils
import traceback, exceptions
from FingerEntry import FingerEntry

class SuccessorsList(object):
    '''
    Contains a list of successors and keeps it sorted.
    '''
    
    

    def __init__(self, nodeLocation, metricsMessageCounter):
        '''
        nodeLocation is the location of the node that owns this list. It's to make sure 
        that any ID less than the node ID is put at the end!
        '''
        self.theList = []
        self.nodeLocation = nodeLocation
        self.metricsMessageCounter = metricsMessageCounter
        
    
    def addValues(self, listOfSuccesors):
        '''Add the given list to our list'''
        listModified = False
        
        origSuccessor = self.getSuccessor() # Solely for debugging when successors change
        
        
        # If we are combining lists, make  it work.
        if isinstance(listOfSuccesors, SuccessorsList):
            listOfSuccesors = listOfSuccesors.theList

        
        #glog.debug("[%s] AddValues in SuccList got: %s" % (self.nodeLocation, str(listOfSuccesors)), system='SuccessorsList')

        lastID = self.getLastID() # What is currently the last ID in the list?
        
        # ListOfSuccessors can also be a single successor entry or a list
        if isinstance(listOfSuccesors, list):
            #glog.debug("Checking list of succ ", system=self.nodeLocation)    
            # It's a list, so check each one and add if needed
            for i in listOfSuccesors:
                if self.shouldBeInList(i, lastID):
                    #glog.debug("%s: succList addValues adding:%s" % (self.nodeLocation, i))
                    self.theList.append(i)
                    listModified=True
#                 else:
#                     #glog.debug("%s: succList NOT addValue:%s   %s  %s " % (self.nodeLocation, i, i not in self.theList, self.shouldBeInList(i, lastID)))
#                     glog.debug("%s: succList NOT addValue:%s" % (self.nodeLocation, i))

        elif self.shouldBeInList(listOfSuccesors, lastID):
            #glog.debug("%s: addValues adding single:%s" % (self.nodeLocation, listOfSuccesors))
            self.theList.append(listOfSuccesors)
            listModified=True
            
        if listModified:
            self.theList.sort(key=self.getSortKey)
            self.theList = self.theList[:Config.NUM_SUCCESSORS_TO_MAINTAIN]
            
            # For safety, remove the dead ones
            self.removeDeadSuccessors()
            
        self.checkFinalSuccessor(origSuccessor, "addValues") #DEBUG
   
    def getLastID(self):
        '''Which ID is currently the last one in the list?
           Returns the value useful for sorting purposes or None if empty
        '''
        if len(self.theList) == 0:
            return None
        
        lastEntry = self.theList[-1]
        
        # Is the last one none?
        if lastEntry is None:
            return None
        
        lastID = lastEntry.id
        if lastID <= self.nodeLocation.id:
            lastID = lastID + 2**FingerEntry.m
            
        return lastID

        
    def shouldBeInList(self, loc, lastID):
        '''Should the nodeLoc be in the list?
           It can be in the list if the last entry is a None or 
           the locId is less than the current end of the list.
        '''
        
        if loc in self.theList:
            return False
        
        if lastID is None or len(self.theList) < Config.NUM_SUCCESSORS_TO_MAINTAIN:
            return True
        
        
    
        if loc.id > self.nodeLocation.id:
            newID = loc.id
        else:
            # This makes sure any lower number wraps around in the list.
            # Because if it's my successor and it's lower than me, it must have 
            # crossed a zero boundary.
            newID = loc.id + 2**FingerEntry.m
            
        if loc.id > 2**FingerEntry.m:
            log.err("LOC ID > Max ID Error in SuccessorsList")
            raise Exception("LOC ID > Max ID Error in SuccessorsList")
        
        #log.msg("shouldBeInList [%s] \n%s\n%s" % (newID < lastID, newID, lastID), system=self.nodeLocation)
        return newID < lastID
        
        
        
    def getSortKey(self, loc):
        if loc.id > self.nodeLocation.id:
            return loc.id
        else:
            # This makes sure any lower number wraps around in the list.
            # Because if it's my successor and it's lower than me, it must have 
            # crossed a zero boundary.
            return loc.id + 2**FingerEntry.m     
        
    def getSuccessor(self):
        if len(self.theList) == 0:
            # I guess I'm my own successor
            return None
        else:
#             # DEBUG printing:
#             print("DEBUG: Successors of: %s [%d]" % (self.nodeLocation, len(self.theList)))
#             for s in self.theList:
#                 print("   Successor is %s" % s)
            
            return self.theList[0]
    
    @defer.inlineCallbacks
    def getLiveSuccessor(self):
        '''Return a connection to a live successor node or None if there are none.
           The caller should yield this call and also must remember to close the 
           open connection returned!
        '''
        
        currentSuccessor = self.getSuccessor()
        succRef = None
        conn = None
        connected = False # Have we connected yet?
        
        while currentSuccessor != None and currentSuccessor != self.nodeLocation and connected == False:
            try:
                # Try to connect
                #glog.debug("getLive connecting %s -> %s" % (self.nodeLocation, currentSuccessor))
                (factory, conn) = Utils.getRemoteConnection(currentSuccessor, self.metricsMessageCounter)
                succRef = yield factory.getRootObject()
                connected = True
                
            except Exception,  e:
                # ConnectionRefused is okay, just means the node died. Other errors though maybe not!
                if not isinstance(e, error.ConnectError):
                    log.err(e, "getLiveSuccessor: Trying again. BadSuccessor[%s] " % currentSuccessor, system=str(self.nodeLocation))

                self.removeLocationFromList(currentSuccessor)

                # Try the next one!        
                currentSuccessor = self.getSuccessor()
                log.msg("getLiveSuccessor: Checking [%s] " % currentSuccessor, system=str(self.nodeLocation))                    


        
        if currentSuccessor is None:
            log.err("Node has no live successors. Hopefully rebuild will fix.", system=str(self.nodeLocation))

        # Return the location, reference and connection
        defer.returnValue( (currentSuccessor, succRef, conn) )
            
        
    @defer.inlineCallbacks
    def removeDeadSuccessors(self):
        # Prune my successor's list for "alive" only

        # Track changes to successor for debugging
        origSuccessor = self.getSuccessor() # Solely for debugging when successors change

        
        subFromI = 0
        
        for i in range(len(self.theList)):
            newIndex = i - subFromI
            if newIndex < 0 or newIndex >= len(self.theList):
                continue
             
            currentSuccLocation = self.theList[newIndex]
        
            # We are always alive
            if currentSuccLocation.id == self.nodeLocation.id:
                continue
            
            alive = yield Utils.isAlive(currentSuccLocation)
            if not alive:
                # Successor wasn't alive, remove from my list of successors
                glog.debug("Removing dead successor location: ID %s" % currentSuccLocation.id, system=self.nodeLocation)
                subFromI = self.removeLocationFromList(currentSuccLocation)
               
                
        self.checkFinalSuccessor(origSuccessor, "removeDeadSuccessors") #DEBUG
        return
    
    @defer.inlineCallbacks
    def rebuildSuccessorList(self, enclaveId, bootstrapNodeLocList = None):
        
        # Track changes to successor for debugging
        origSuccessor = self.getSuccessor() # Solely for debugging when successors change
        
        # Prune my successor's list for "alive" only
        yield self.removeDeadSuccessors()
        
        subFromI = 0
        
        for i in range(len(self.theList)):
            currentSuccLocation = self.theList[i-subFromI]
        
            try:    
                # Check if the succesor's pred is currentPred
                (factory, conn) = Utils.getRemoteConnection(currentSuccLocation, self.metricsMessageCounter)
                succ = yield factory.getRootObject()
                successorsSuccessorList = yield succ.callRemote("getSuccessorList", enclaveId)
            
            
                # Add them to my successorsList
                self.addValues(successorsSuccessorList)
                
                # Close the connection
                Utils.disconnect(None, conn)

                # Once we find one live successor list, it's good enough. We can go.
                #glog.debug("rebuildSuccessorList: connected successor is: %s" % currentSuccLocation)
                defer.returnValue(True)
            except Exception : 
                # Successor wasn't alive, remove from my list of successors
                glog.debug("Removing dead successor during rebuild location: ID %s" % currentSuccLocation.id, system='rebuildSuccessorList')
                subFromI = self.removeLocationFromList(currentSuccLocation)
        
        

        # Attempt to re-bootstrap in
        yield self.reBootstrap(bootstrapNodeLocList, enclaveId)
    
        self.checkFinalSuccessor(origSuccessor, "rebuildSuccessorList") #DEBUG
        
        defer.returnValue(True)
    
    def removeLocationFromList(self, theLoc):
        '''Removes all occurrences of theLoc from the list. Returns number actually removed.'''
        removed = 0
        while theLoc in self.theList: # Remove all occurrences
            try:
                self.theList.remove(theLoc) # Cause it's dead!
                removed += 1 # Should be a simpler way right?
            except ValueError :
                None # Don't worry if it already got removed.        
        
        return removed
        
    @defer.inlineCallbacks
    def reBootstrap(self, bootstrapNodeLocList, enclaveId):
        '''Try to re-bootstrap into the network being managed by bootstrapNodeLoc'''
        try:
            glog.debug("SuccessorsList: all successors are dead... attempting to re-bootstrap", system=str(self.nodeLocation))
            # If I am my own successor but I'm not a bootstrap node, try to bootstrap
            # back into the network.
            
            # Try to get a connection to any of the bootstrap nodes.
#                 if bootstrapNodeLoc is not None and \
#                    bootstrapNodeLoc.id != self.nodeLocation.id:
            # We have no successors, so we'd better bootstrap back in.
            (bootstrapNode, factory, conn, nodeLocation) = yield Utils.getRemoteConnectionFromList(bootstrapNodeLocList, metricsMessageCounter=self.metricsMessageCounter)
            
            if bootstrapNode is False:
                # Could not get a connection
                
                # DPF - This is a change on Oct, 2014 -- even if I am NOT a bootstrapNode,
                #     - I will add myself as a successor. Need to see if this breaks stuff.
                #     - old version only did this for bootstrap nodes.
                self.addValues(self.nodeLocation)
            else:
                
                succLoc = yield bootstrapNode.callRemote("findSuccessorLocation", self.nodeLocation.id, enclaveId)
        
                if succLoc is not None:
                    (factory, conn2) = Utils.getRemoteConnection(succLoc, self.metricsMessageCounter)
                    succNode = yield factory.getRootObject()
        
                    successorsSuccessorList = yield succNode.callRemote("getSuccessorList", enclaveId)
            
                    # Add the successor to my list
                    self.addValues(succLoc)
                
                    # Add the successorsList to my list also (for efficiency)
                    self.addValues(successorsSuccessorList)
                    
                    # Close connections
                    Utils.disconnect(None, conn2)
                                    
                # Close the connections
                Utils.disconnect(None, conn)

                
        except Exception, e: 
            log.msg("bootstrap node is dead. Try again later. [%s]" % e, system="SuccessorsList")
            log.err(e, "bootstrap node is dead. Try again later.", system="SuccessorsList")
                     
                           
    
    def checkFinalSuccessor(self, origSuccessor, msg=""):
        '''DEBUG method to check if the current (final) successor is the same as the original?'''
        # DEBUG Information
        finalSuccessor = self.getSuccessor()
        if origSuccessor != finalSuccessor:
            glog.debug("%s: Successor changed from [%s] to [%s] for node [%s]" % (msg, origSuccessor, finalSuccessor, self.nodeLocation.port), system='SuccessorsList')
                
   
    
    
# Make sure the NodeLocation is copyable.
class CopySuccessorsList(SuccessorsList, pb.Copyable, pb.RemoteCopy):
    pass

pb.setUnjellyableForClass(CopySuccessorsList, CopySuccessorsList)
    
    