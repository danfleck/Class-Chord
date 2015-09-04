'''
Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

Created on Dec 1, 2014

@author: dfleck
'''

from datetime import datetime
from twisted.python.log import *
from twisted.python import log

class GmuLogObserver:
    """
    Log observer that writes to a file-like object.

    @type timeFormat: C{str} or C{NoneType}
    @ivar timeFormat: If not C{None}, the format string passed to strftime().
    """
    timeFormat = None

    def __init__(self, f):
        self.write = f.write
        self.flush = f.flush


    def getTimezoneOffset(self, when):
        """
        Return the current local timezone offset from UTC.

        @type when: C{int}
        @param when: POSIX (ie, UTC) timestamp for which to find the offset.

        @rtype: C{int}
        @return: The number of seconds offset from UTC.  West is positive,
        east is negative.
        """
        offset = datetime.utcfromtimestamp(when) - datetime.fromtimestamp(when)
        return offset.days * (60 * 60 * 24) + offset.seconds


    def formatTime(self, when):
        """
        Format the given UTC value as a string representing that time in the
        local timezone.

        By default it's formatted as a ISO8601-like string (ISO8601 date and
        ISO8601 time separated by a space). It can be customized using the
        C{timeFormat} attribute, which will be used as input for the underlying
        L{datetime.datetime.strftime} call.

        @type when: C{int}
        @param when: POSIX (ie, UTC) timestamp for which to find the offset.

        @rtype: C{str}
        """
        if self.timeFormat is not None:
            return datetime.fromtimestamp(when).strftime(self.timeFormat)

        tzOffset = -self.getTimezoneOffset(when)
        when = datetime.utcfromtimestamp(when + tzOffset)
        tzHour = abs(int(tzOffset / 60 / 60))
        tzMin = abs(int(tzOffset / 60 % 60))
        if tzOffset < 0:
            tzSign = '-'
        else:
            tzSign = '+'
        return '%d-%02d-%02d %02d:%02d:%02d%s%02d%02d' % (
            when.year, when.month, when.day,
            when.hour, when.minute, when.second,
            tzSign, tzHour, tzMin)

    def emit(self, eventDict):
        text = textFromEventDict(eventDict)
        if text is None:
            return

        timeStr = self.formatTime(eventDict['time'])
        fmtDict = {'system': eventDict['system'], 'text': text.replace("\n", "\n\t")}
        msgStr = log._safeFormat("[%(system)s] %(text)s\n", fmtDict)

        #util.untilConcludes(self.write, timeStr + " " + msgStr)
        #util.untilConcludes(self.flush)  # Hoorj!
        
        #util.untilConcludes(sys.__stdout__.write, msgStr)
        print(msgStr)

    def start(self):
        """
        Start observing log events.
        """
        addObserver(self.emit)

    def stop(self):
        """
        Stop observing log events.
        """
        removeObserver(self.emit)
