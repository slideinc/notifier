#
#
#
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
