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
Wire protocol to read/write entire commands
"""

import socket
import select
import struct
import errno

import message

COMMAND_READ_SIZE = 32*1024

class ConnectionError(RuntimeError):
    pass

class ReadWriter(object):
    def __init__(self, connection, **kwargs):
        self.connection = connection
        self._recv_data = ''
        self._recv_size = 0
        self._send_data = ''
        self.serializer = message.BinSerializer(**kwargs)

    def send_size(self):
        return len(self._send_data)
    def recv_size(self):
        return len(self._recv_data)

    def read_command(self):
        while not self._recv_size or self._recv_size > len(self._recv_data):
            try:
                partial = self.connection.recv(COMMAND_READ_SIZE)
            except socket.error, e:
                if e[0] == errno.EINTR:
                    del(e)
                    continue

                partial = ''

            if not partial:
                raise ConnectionError(*locals().get('e', (0, 'empty recv')))
            
            self._recv_data += partial
            if not self._recv_size and not (len(self._recv_data) < 4):
                self._recv_size = struct.unpack('!i', self._recv_data[:4])[0]
                self._recv_data = self._recv_data[4:]

        result = self.serializer.deserialize(self._recv_data[:self._recv_size])
        self._recv_data = self._recv_data[self._recv_size:]

        if len(self._recv_data) < 4:
            self._recv_size = 0
        else:
            self._recv_size = struct.unpack('!i', self._recv_data[:4])[0]
            self._recv_data = self._recv_data[4:]

        return result

    def write(self):
        while self._send_data:
            try:
                size = self.connection.send(self._send_data)
            except socket.error, e:
                if e[0] == errno.EINTR:
                    continue
                else:
                    raise ConnectionError(*e)

            self._send_data = self._send_data[size:]

    def write_int(self, i):
        i = int(i)
        return '%s%s%s%s' % (
            chr((i & 0xff000000) >> 24),
            chr((i & 0x00ff0000) >> 16),
            chr((i & 0x0000ff00) >> 8),
            chr((i & 0x000000ff)))

    def queue_command(self, command):
        response = self.serializer.serialize(command)
        self._send_data += self.write_int(len(response)) + response

    def write_command(self, command):
        self.queue_command(command)
        self.write()

    def flush(self):
        self.write()

    def close(self):
        self.connection.close()

    def wake(self):
        # only support interrupted read, if we have it.
        if hasattr(self.connection, 'wake'):
            self.connection.wake(select.POLLIN)

    def settimeout(self, timeout):
        self.connection.settimeout(timeout)

    def gettimeout(self):
        return self.connection.gettimeout()

    def setsockopt(self, level, option, value):
        return self.connection.setsockopt(level, option, value)

    def getsockopt(self, level, option):
        return self.connection.getsockopt(level, option)

    def fileno(self):
        return self.connection.fileno()
#
# end...
