"""message
Base classes for the binary BT protocol and C decoder wrapper.
"""

import struct
from mod import wbin

NONE_ID = 0
INT_ID = 1
STRING_ID = 2
DATA_ID = 3
LIST_ID = 4
DICT_ID = 5
CMD_ID = 666
RESPONSE_ID = 667
PUSH_ID = 668

MIN_SIZE = len(struct.pack('!H', 0))

class Base(object):
    pass

class Cmd(Base):
    def __init__(self, cmd, params = {}):
        self.cmd = cmd
        self.params = params
        self.id = CMD_ID
        self.retry = 0
    def __repr__(self):
        return "cmd(%s) params(%s)" %(self.cmd, repr(self.params))
        
class Response(Base):
    def __init__(self, calling_cmd, data):
        self.cmd = calling_cmd
        self.params = data
        self.id = RESPONSE_ID
        self.retry = 0
    def __repr__(self):
        return "calling_command(%s) data (%s)" % (self.cmd, repr(self.params))

class Push(Base):
    def __init__(self, cmd, data):
        self.cmd = cmd
        self.params = data    
        self.id = PUSH_ID
        self.retry = 0
    def __repr__(self):
        return "cmd(%s) params(%s)" %(self.cmd, repr(self.params))

class BinSerializer(object):
    def __init__(self, *args, **kwargs):
        self._clist = {CMD_ID:Cmd, RESPONSE_ID:Response, PUSH_ID:Push}

        self._ser_func = kwargs.get('ser', {}).get('func',   None)
        self._ser_size = kwargs.get('ser', {}).get('offset', 0x8000)
        self._des_func = kwargs.get('des', {}).get('func',   None)
        self._des_size = kwargs.get('des', {}).get('offset', 0x8000)

    def _serialize(self, o):
        if self._ser_func is not None:
            return wbin.serialize(o, self._ser_func, (o,), self._ser_size)
        else:
            return wbin.serialize(o)

    def _deserialize(self, s):
        if self._des_func is not None:
            return wbin.deserialize(s, self._des_func, (s,), self._des_size)
        else:
            return wbin.deserialize(s)
        
    def deserialize(self, s):
        if not isinstance(s, type('')):
            raise TypeError, 'type %s not %s' % (type(s), type(''))

        if len(s) < MIN_SIZE:
            raise ValueError, 'string too short: %d' % (len(s))

        o = 0
        c = self._clist.get(struct.unpack('!H', s[o:2])[0], None)
        if c is None:
            return self._deserialize(s)

        o += 2
        l  = struct.unpack('!i', s[o:o+4])[0]
        o += 4
        n  = s[o:o+l]
        o += l

        if c is Cmd:
            p = self._deserialize(struct.pack('!H', DICT_ID) + s[o:])
        else:
            p = self._deserialize(s[o:])

        return c(n, p)

    def serialize(self, o):
        if not isinstance(o, Base):
            return self._serialize(o)

        s = struct.pack('!H', o.id) + struct.pack('!i', len(o.cmd)) + o.cmd
        p = self._serialize(o.params)

        if isinstance(o, Cmd):
            s += p[2:]
        else:
            s += p

        return s
#
# end..
