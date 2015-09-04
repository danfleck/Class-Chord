'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Simple Python config file. Possibly should override this with ConfigParser instead.

Created on Mar 20, 2014

@author: dfleck
'''
from twisted.python.logfile import DailyLogFile
import sys

# ===================================
# Constants (DO NOT CHANGE THESE!)
# ===================================
# Log Levels -- 
DEBUG = 0
PRODUCTION = 1 

# ===================================
# Setup Variables --- Change these!
# ===================================

USE_CONNECTION_CACHE = False
CONNECTION_CACHE_DELAY = 7 # Seconds to delay disconnect



# Should we warn about no authenticator?
WARN_NO_MESSAGE_AUTHENTICATOR = False 
ALLOW_NO_AUTHENTICATOR = True


# Should we allow the code to use a 127.0.0.1 address?
ALLOW_LOCAL_IP_ADDRESS = True

# How many chars is the enclave ID
ENCLAVE_ID_BITS = 16

# How many chars is the class ID
CLASS_ID_BITS = 6 

# How much info to log?
LOGLEVEL =  PRODUCTION

# Should we capture Debugging metrics? (minor performance hit)
DEBUG_METRICS = True

# Where should we log to?
LOGFILE = sys.stdout
#LOGFILE = DailyLogFile.fromFullPath("./GMU-network.log")

# How many successors should each node maintain to 
# deal with failures?
NUM_SUCCESSORS_TO_MAINTAIN = 4

# Seconds between maintenance calls (how fast to do things))
MAINT_CALL_SECONDS = 5.0

# How often to check for network separation (two chord rings formed)
# Shouldn't need to do this hardly ever!
CHECK_SEPARATION_CALL_SECONDS = 60.0

# How frequently to rebuild successors list
MAINT_CALL_REBUILD_SUCC_SECONDS = 60.0

#SSL off - 0
#SSL on - 1
SSL_ON = 0

#Path to the Private SSL key
SSL_PRIVATE_KEY_PATH = '/home/shiremag/Documents/testSSL/self-ssl.key'

#Path to the SSL Certificate
SSL_CERT_PATH = '/home/shiremag/Documents/testSSL/self-ssl.crt'

# AutoDiscovery Parameters
AUTO_DISCOVERY_PORT = 12299
AUTO_DISCOVERY_MULTICAST_IP = "228.0.0.5"

#Default network connection timeout
NETWORK_CONNECTION_TIMEOUT = 30 # Seconds



