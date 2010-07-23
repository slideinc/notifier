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

"""
server convenience functions
"""

from gogreen import coro

import logging

SRV_LOG_FRMT = '[%(name)s|%(coro)s|%(asctime)s|%(levelname)s] %(message)s'

LOGLEVELS = dict(
    CRITICAL=logging.CRITICAL, DEBUG=logging.DEBUG, ERROR=logging.ERROR,
    FATAL=logging.FATAL, INFO=logging.INFO, WARN=logging.WARN,
    WARNING=logging.WARNING,INSANE=5)

PEER_VERSION_MIN  = 0
PEER_VERSION_RPC  = 1
PEER_VERSION_SUB  = 2
PEER_VERSION_LST  = 3
PEER_VERSION_ADR  = 4
PEER_VERSION_PING = 5
PEER_VERSION = PEER_VERSION_PING

ITEM_STATS_ORIGINAL = 1
ITEM_STATS_MINMAX   = 2
ITEM_STATS_VERSION  = ITEM_STATS_MINMAX

REC_STATS_ORIGINAL = 1
REC_STATS_VERSION  = REC_STATS_ORIGINAL

DBPOOL_OFFLINE_TIMEOUT = 60
# end...
