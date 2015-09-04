'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Apr 9, 2014

@author: dfleck
'''

from gmu.netclient.envelope import Envelope # NetClient
from twisted.spread import pb



# Make sure the Envelope is copyable.
class CopyEnvelope(Envelope, pb.Copyable, pb.RemoteCopy):
    
    def setCopyableState(self, state):
        #print("CopyEnv setCopyable State: %s" % state)
        self.update(state)

    def getStateToCopy(self):
        #print("CopyEnv getStateToCopy State: %s" % self)
        return self.copy()

pb.setUnjellyableForClass(CopyEnvelope, CopyEnvelope)
pb.setUnjellyableForClass("gmu.netclient.envelope.Envelope", CopyEnvelope)
pb.setUnjellyableForClass("gmu.chord.CopyEnvelope.CopyEnvelope", CopyEnvelope) 
