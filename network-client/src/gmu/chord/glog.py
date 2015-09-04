'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

glog is simply a GMU Logger to enable log levels.

Created on Mar 20, 2014

@author: dfleck
'''
from twisted.python import log
import Config

def debug(*args, **kwargs):
    if Config.LOGLEVEL == Config.DEBUG:
        log.msg(*args, **kwargs)
    