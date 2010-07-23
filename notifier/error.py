# -*- Mode: Python; tab-width: 4 -*-

# Copyright (c) 2005-2010 Slide, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#     * Neither the name of the author nor the names of other
#       contributors may be used to endorse or promote products derived
#       from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

'''error

Definitions for access/service return code errors/exceptions.
'''
import exceptions

SUCCESS        = 0
#
# old style, pass through rc values
#
UNKNOWN        = 1
DUPLICATE_KEY  = 2
EXEC_TRACEBACK = 5
AFFINITY_ERROR = 6
#
# new style exceptions.
#
table  = {}
lookup = lambda i, *a: table.get(i, AccessError)(*a)

ACCESS_ERROR_MASK = 0x400 #starting at 1K to avoid collision.

class AccessError(exceptions.Exception):
    id = 0x400 + 0

class DatabaseUnavailable(AccessError):
    '''DatabaseUnavailable

    Database was unavailable to service the request
    '''
    id = 0x400 + 1

class NoServiceHandler(AccessError):
    '''NoServiceHandler

    The requested service handler does not exist.
    '''
    id = 0x400 + 2

class ServiceTraceback(AccessError):
    '''ServiceTraceback

    Unknown/Unhandled exception occured while executing the request.
    '''
    id = 0x400 + 3

class LockTimeout(AccessError):
    '''LockTimeout

    resource lock timed out/heavy lock contention
    '''
    id = 0x400 + 4

class ParameterError(AccessError):
    '''ParameterError

    The request had incorrect/inconsistent parameters.
    '''
    id = 0x400 + 5

class NoServiceDefined(AccessError):
    '''NoServiceDefined

    The request was made with no service defined.
    '''
    id = 0x400 + 6

#
# Build ID/exception table
#
for v in locals().values():
    try:
        if issubclass(v, AccessError):
            table[v.id] = v
    except TypeError:
        pass

table[None] = AccessError
#
# end..
