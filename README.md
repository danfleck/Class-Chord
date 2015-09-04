# Class-Chord
Class-Chord: A Modified Chord Messaging System that support dynamic sub-groups

## Synopsis

This package is the peer-to-peer networking code developed by George Mason University. This code is developed as a library which supports P2P messaging and 
is called by other code. The code is the first implementation of the Class-Chord algorithm described in the paper: 

Dan Fleck, Fengwei Zhang, Sharath Hiremagalore, Stephen Reese, and Liam McGhee, "Class-Chord: Efficient Messages to Classes of Nodes in Chord," in proceedings of CSCloud 2015. 

Please reference this paper when using or modifying this code.

## Code Example

To use the code requires several APIs to be implemented.

1. netclient.classChordClientAPI: Users should inherit from this base class and implement all methods defined. An instance of this object is used by the 
netclient.classChordNetworkAPI code.

2. netclient.classChordNetworkAPI: This is the interface which is callable by other code. All interactions with the networking layer should be done using the method calls defined in this API. The details of each method call are described fully in the comments within the Python module. The GMU implementation being used currently uses a Chord networking overlay to support the API. The true class which should be instantiated is netclient.classChordNetworkChord however only methods defined in the classChordNetworkAPI should be used.

A typical usage scenario of the API would be:
        1. Client sends a flooding query
        2. Each receiving node processes the query and sends back a p2p or agg-response response message.

## Motivation

The P2P layer ensures that a large burden is not leavied on any single node. The goal is for the code to be able to support a 30K node network without causing excessive network traffic or issues when a great dea of churn happens. Additionally, the networking layer will handle some security issues MITM (for example). However, the implementation sends all encryption/decryption tasks to the client layer. This was the approach decided by the project team.

## Installation

The networking library is based on Twisted and requires several Python packages:
        - twisted
        - zope.interface
        - miniupnpc - to support NAT traversal

To install the Tapio networking package use: python setup.py install

## API Reference


To start a node, the general process is:
-- Create an classChordNetworkAPI instance:
	    aClient = SampleClient(....)
        self.bsNetwork = classChordNetworkChord(aClient, myPort, myIP)
        
-- Startup the node. 
        self.bsNetwork.start(statusFunctionCallback, nodeID, enclaveToJoin, authenticationPayload, bootstrapNodeList, isBootstrapNode, useAutodisovery)

		The parameters will be set as follows:
		
		For a bootstrap node:
			enclaveToJoin --> A string naming the enclave to join or to form
			bootstrapNodeList --> A list of NodeLocations [ NodeLocation1, NodeLocation2, ...]. 
								  This can be blank. If present, then the bootstrap nodes you pass in must 
								  be part of the enclaveToJoin or the code will error out.
			isBootstrapNode --> Am I a bootstrap node? True | False
			useAutoDiscovery  --> Should I try to autodiscover bootstrap nodes? True | False  
								         
-- Upon completion, the statusFunctionCallback will be called with a status and authenticationMessageResponse:
	statusFunctionCallbcak(theJoinStatus, authenticationMessageResponse)
									                     
  
## Tests

A full suite of unit tests can be run autmatically using: python setup.py test
These tests take approximately 10-15 mins to run fully. 

## Contributors

This package was written for the DARPA ICAS project by George Mason University (GMU).

Primary authors include: Dan Fleck, Fengwei Zhang, Sharath Hiremagalore, Stephen Reese, and Liam McGhee

## License

This code is released under the Apache 2.0 License.

