#!/usr/local/bin/python
#
#
"""notifier

Threads implementing event notification and tracking.
"""

from gogreen import coro
from gogreen import backdoor

import socket
import time
import logging
import struct
import traceback
import sys
import os
import getopt
import time
import resource
import select
import heapq
import inspect
import random
import string
import copy
import exceptions

coro.socket_emulate()

import message
from util import interval
from btserv import command
from btserv import server
from btserv import ntree

from configs.config import config

SEND_THRESHOLD = 128*1024
_BUF_SIZE_K = (sys.platform == 'darwin' and 4 or 256)
SEND_BUF_SIZE  = _BUF_SIZE_K*1024
RECV_BUF_SIZE  = _BUF_SIZE_K*1024

SHUTDOWN_TIMEOUT = 30

PEER_TYPE_NONE      = 0
PEER_TYPE_FULL      = 1
PEER_TYPE_PUBLISHER = 2

SEARCH_TREE_DEF  = (('object', 'basic'), ('id', 'mask'), ('cmd', 'basic'))
SEARCH_TREE_PATH = map(lambda i: i[0], SEARCH_TREE_DEF)

EXTERN_TREE_DEF  = (('object', 'basic'), ('id', 'basic'))

CLIENT_TREE_DEF  = (('command', 'basic'), ('object', 'basic'), ('id', 'mask'))
CLIENT_TREE_PATH = map(lambda i: i[0], CLIENT_TREE_DEF)
#
# build search vector which works for local and remote lookup
#
TOTAL_TREE_PATH = ('command', 'object', 'id', 'cmd')

SUB_TYPE_FULL  = {'type': 'full'}
SUB_TYPE_SLICE = {'type': 'slice', 'weight': 1.0}

CLIENT_STATE_INIT        = 1
CLIENT_STATE_CONNECTING  = 2
CLIENT_STATE_CONNECTED   = 3
CLIENT_STATE_ESTABLISHED = 4
CLIENT_STATE_RECONNECT   = 5
CLIENT_STATE_EXITING     = 6
CLIENT_STATE_DEAD        = 7

def init_threashold(client, count):
    if client._state > CLIENT_STATE_CONNECTED:
        return True

    if client._rcnt < count:
        return False

    return True


class NotifierError(RuntimeError):
    pass
class SearchTreeError (RuntimeError):
    pass
class SubscriptionError (RuntimeError):
    pass


def serialize_callback(offset, o):
    if not isinstance(o, dict):
        coro.log.info('long wire: %d %r' % (offset, type(o)))
        return None

    value = map(lambda i: o.get(i), ['object','id','cmd', 'seq'])
    if filter(bool, value):
        coro.log.info('long wire: %d %r' % (offset, value))
    else:
        coro.log.info('long wire: %d %r' % (offset, o.keys()))

    coro.Yield(0.0)

def deserialize_callback(offset, s):
    coro.log.info('long wire: %d' % (offset,))
    coro.Yield(0.0)


class SubscriptionTracker(object):
    def __init__(self, vector, group = None, external = ()):
        self._st   = ntree.SearchTree(vector)
        self._vt   = map(lambda i: i[0], vector)
        self._se   = ntree.SearchTree(external)
        self._ve   = map(lambda i: i[0], external)
        self._grp  = group
        self._subs = {}

    def _external_insert(self, data, optional = {}):
        if not self._ve:
            return {}

        size = data.get(self._ve[-1], {}).get('count', -1)
        esub = dict(map(
            lambda i: (i, data.get(i, {}).get('value')),
            self._ve))

        if not size:
            esub.update({'group': self._grp, 'info': optional})
            osub = ntree.Subscription(esub)
            self._se.insert(osub, osub)

            return esub

        itype = optional.get('type', 'full')
        subs  = self._se.lookup(esub)

        for sub, opts in subs:
            ctype = sub.get('info', {}).get('type', 'full')
            if ctype != itype:
                raise SubscriptionError(
                    'type mismatch: %s != %s' % (itype, ctype))

        return {}

    def _external_remove(self, info):
        if not self._ve:
            return {}

        if info.get(self._ve[-1], {}).get('count', -1):
            return {}

        esub = dict(map(
            lambda i: (i, info.get(i, {}).get('value')),
            self._ve))

        osub, opts = self._se.lookup(esub)[0]
        none = self._se.remove(osub, osub)

        return dict(osub)

    def subscribe(self, *args, **kwargs):
        optional = kwargs.get('optional', SUB_TYPE_FULL)
        vector   = args[:-1]
        object   = args[-1]
        fullv    = dict(zip(self._vt, vector))

        result = self._st.insert(fullv,    object,    optional)
        try:
            subscription = self._external_insert(result, optional = optional)
        except exceptions.Exception, error:
            self._st.remove(fullv, object)
            raise error

        self._subs.setdefault(object, []).append(vector)
        return subscription

    def unsubscribe(self, *args):
        vector = args[:-1]
        object = args[-1]
        fullv  = dict(zip(self._vt, vector))

        result = self._st.remove(fullv, object)

        self._subs[object].remove(vector)
        if not self._subs[object]:
            del(self._subs[object])

        return self._external_remove(result)

    def unsubscribe_all(self, object):
        results = []

        for vector in self._subs.get(object, [])[:]:
            results.append(self.unsubscribe(*vector + (object, )))

        return filter(bool, results)

    def lookup(self, params):
        return self._st.lookup(params)

    def subscribes(self):
        if self._ve:
            return map(lambda i: dict(i[0]), self._se.values())
        else:
            return []

class RPCHandle(object):
    def __init__(self, destination, params, seq = None):
        self._alive = False
        if not hasattr(destination, 'rpc_response'):
            raise ValueError, 'destination has no rpc response handle'

        self._response = destination
        self._results = []
        self._params = params
        self._seq = seq
        self._alive = True

    def __getitem__(self, name):
        return self._params[name]

    def __del__(self):
        if not self._alive:
            return None

        if self._seq is None:
            self._response.rpc_response(
                self._params['object'], self._params['id'],
                self._params['cmd'], self._results)
        else:
            self._response.rpc_response(
                self._params['object'], self._params['id'],
                self._params['cmd'], self._results, self._seq)
            
    def __eq__(self, other):
        return self._response is other

    def get(self, key, default = None):
        return self._params.get(key, default)

    def params(self):
        return copy.copy(self._params)

    def append(self, value):
        self._results.append(value)

    def kill(self):
        self._alive = False

class RPCMetaHandle(object):
    def __init__(self, obj, ids, cmd, arg, destination, seq = None):
        self._alive = False
        if not hasattr(destination, 'rpc_response'):
            raise ValueError, 'destination has no rpc response handle'

        self._response = destination
        self._results  = map(lambda i: None, xrange(len(ids)))

        self._obj = obj
        self._ids = ids
        self._cmd = cmd
        self._arg = arg
        self._seq = seq
        self._itr = ids[:]

        self._alive = True

    def __del__(self):
        if not self._alive:
            return None

        if self._seq is None:
            self._response.rpc_response(
                self._obj, self._ids, self._cmd, self._results)
        else:
            self._response.rpc_response(
                self._obj, self._ids, self._cmd, self._results, self._seq)

    def __iter__(self):
        return self

    def next(self):
        if not self._itr:
            raise StopIteration

        params = dict(zip(
            SEARCH_TREE_PATH,
            (self._obj, self._itr.pop(), self._cmd)))
        params.update({'args': self._arg})

        return RPCHandle(
            self,
            params,
            seq = len(self._itr))

    def rpc_response(self, obj, _id, cnd, results, seq):
        self._results[seq] = results


class RPCSync(object):
    '''RPCSync

    Used by the asynchronous notification server to create a synchronous
    RPC call. The calling thread creates the object, makes the RPC request
    using the object as the response destination, and then blocks waiting
    for the object to receive the results. Once the object receives the
    response it is saved and the requesting thread awoken, which can then
    collect the response data and move on.
    '''
    def __init__(self, vector):
        self._waiter  = coro.coroutine_cond()
        self._results = map(lambda i: None, xrange(len(vector)))
        self._pending = {}
        #
        # create a dictionary of pending vector elements to result
        # array offsets.
        #
        filter(
            lambda i: self._pending.setdefault(i[0], set([])).add(i[1]),
            zip(vector, xrange(len(vector))))

    def rpc_response(self, object, id, cmd, results):
        offsets = self._pending.get(id)
        if offsets is not None:
            self._results[offsets.pop()] = results

        if not offsets:
            self._pending.pop(id, None)

        if not self._pending:
            self._waiter.wake_all()

    def wait_response(self, timeout = None):
        while self._pending:
            result = self._waiter.wait(timeout = timeout)
            if result is None:
                break

        for offset_list in self._pending.values():
            for offset in offset_list:
                self._results[offset] = None

        return self._results


class RPCStation(object):
    '''RPCStation

    Create a sequenced record and collection point for outstanding RPC
    requests on which a thread can wait for responses.
    '''
    def __init__(self):
        self._waiter  = coro.coroutine_cond()
        self._pending = {}
        self._results = {}
        self._seq     = 1

    def _wait_one(self, seq, timeout = None):
        while seq in self._pending:
            result = self._waiter.wait(timeout = timeout)
            if result is None:
                return False

        return True

    def wait(self, seq_list, **kwargs):
        strip = seq_list[:]

        while strip:
            if not self._wait_one(strip.pop(), kwargs.get('timeout', None)):
                break
        #
        # Either all sequence numbers are no longer pending,
        # or timeout expiredd. Collect results, noting that
        # in case of timeout, result may not be present.
        #
        result = []

        for seq in seq_list:
            req, rep = self._results.pop(seq, ((), None))
            result.append((seq, req, rep))

        return result

    def rpc_response(self, object, id, cmd, results, seq):
        #
        # drop results that are no longer pending.
        #
        if seq not in self._pending:
            return None

        self._results[seq] = (self._pending.pop(seq, ()), results)
        self._waiter.wake_all()

    def push(self, obj, id, cmd, args):
        try:
            self._pending[self._seq] = (obj, id, cmd, args)
            return self._seq
        finally:
            self._seq += 1

    def pop(self, seq_set, timeout = None):
        present = seq_set & set(self._results)

        while not present and seq_set & set(self._pending):
            result = self._waiter.wait(timeout = timeout)
            if result is None:
                return None, (), None

            present = seq_set & set(self._results)
            
        if not present:
            return None, (), None

        seq = present.pop()
        req, rep = self._results.pop(seq)
        return seq, req, rep

    def clear(self, seq_list):
        for seq in seq_list:
            self._pending.pop(seq, None)
            self._results.pop(seq, None)

        return None


class NotifyClientBase(coro.Thread):
    '''NotifyClientBase

    coroutine enabled notification client base class. Subclassed by
    the full notifier client and the notification publisher.
    '''
    def __init__(self, *args, **kwargs):
        super(NotifyClientBase, self).__init__(*args, **kwargs)

        self._send_q = None
        self._exit   = False
        self._type   = PEER_TYPE_NONE
        self._msg    = None

        self._peer_id   = None
        self._peer_ver  = 0
        self._peer_idle = 0
        self._peer_addr = None

        self._idletime = config.PUBSUB_IDLE_TIMEOUT
        self._lastsend = 0
        self._lastrecv = 0

        self._in_error = False

        self.conn    = None

    def execute(self):
        while not self._exit:
            result = self.execute_once()
            if not result:
                break

    def execute_once(self):
        #
        # generate a ping request if necessary:
        #
        self.ping()
        #
        # queue a few commands if available for transmition
        #
        while self._send_q and self.conn.send_size() < SEND_THRESHOLD:
            cmd = self._send_q.pop(0)
            #
            # queue the command, flushing the connection will send.
            #
            try:
                self.conn.queue_command(cmd)
            except exceptions.Exception, e:
                self.warn('Error queueing command: %r %r', e, cmd)
        #
        # if data is queued, flush the connection
        #
        send_size = self.conn.send_size()
        if send_size:
            #
            # data waiting to be flushed to the network, setting the
            # timeout ensures that we do not wait on the receiver to
            # read the data. (dirty bastard :)
            #
            self.conn.settimeout(0.0)
            try:
                self.conn.flush()
            except command.ConnectionError:
                self._msg = 'Remote host closed connection'
                return False
            except coro.CoroutineSocketWake:
                pass
            except coro.TimeoutError:
                pass
            except:
                self.traceback()
                return False
        #
        # IF some data was flushed on the connection then mark
        # the time of the most recent send. This will drive any
        # necessary idle pings.
        #
        if send_size != self.conn.send_size():
            self._lastsend = time.time()
        #
        # set read timeout based on queue depth, we need to make
        # read progress to avoid live-lock on the send queue.
        # write timeout is set as well to ensure that reads get
        # serviced reqularly.
        #
        if self._send_q or self.conn.send_size() or self._exit:
            timeout = 0.0
        else:
            timeout = self.recv_timeout()

        self.conn.settimeout(timeout)
        #
        # read command
        #
        try:
            cmd = self.conn.read_command()
        except command.ConnectionError:
            self._msg = 'Remote host closed connection'
            return False
        except coro.CoroutineSocketWake:
            return True
        except coro.TimeoutError:
            if self.expired():
                self.warn(
                    'peer %r idle %r timeout.',
                    self.peer_addr,
                    self._peer_idle)
                return False
            else:
                return True
        except:
            self.traceback()
            return False

        self._lastrecv = time.time()

        if not isinstance(cmd, message.Push):
            self.warn("Unknown type: %s", type(cmd))
            return True

        self.debug("Notification: %r", cmd)

        try:
            return self.dispatch(cmd)
        except:
            self.error('dispatch error on command: %r' % (cmd,))
            self.traceback()
            return False

    def expired(self):
        if self._peer_idle:
            return not (self._peer_idle > (time.time() - self._lastrecv))
        else:
            return False

    def recv_timeout(self):
        if self._peer_ver < server.PEER_VERSION_PING:
            return None
        
        current = time.time()
        timeout = self._idletime  - (current - self._lastsend)
        if self._peer_idle:
            timeout = min(
                timeout,
                self._peer_idle - (current - self._lastrecv))

        return max(timeout, 0)

    def ping(self):
        if self._peer_ver < server.PEER_VERSION_PING:
            return None

        if self._send_q or self.conn.send_size():
            return None

        if self._idletime < (time.time() - self._lastsend):
            self._send_q.append(message.Push('ping', {}))

    def bump(self):
        if self._peer_ver < server.PEER_VERSION_PING:
            return False

        if self._send_q:
            return False

        if self.conn is None:
            return False

        if self.conn.send_size():
            return False

        self.push(message.Push('ping', {}))
        return True


class NotifyPublisher(NotifyClientBase):
    '''NotifyPublisher

    coroutine enabled notification publisher.
    '''
    def __init__(self, *args, **kwargs):
        super(NotifyPublisher, self).__init__(*args, **kwargs)

        if kwargs.get('full_notifier', False):
            self._icmd    = 'establish'
            self._self_id = 'zp:%d:%d' % (os.getpid(), self._thread_id)
        else:
            self._icmd    = 'publisher'
            self._self_id = '%s:%x:%x' % (
                server.gethostname(), os.getpid(), self._thread_id)

        self._host    = ''
        self._port    = 0
        self._rpc_seq = 0
        self._rpc_q   = {}
        self._active  = coro.coroutine_cond()
        self._pending = None
        self._cond    = None

    def run(self, addrs):
        self._addrs = addrs

        while not self._exit:

            if not self.establish():
                return None

            self.info(
                "Publisher connection established: <%s:%d>",
                self._host,
                self._port)
            #
            # all other exits should be logged.
            self._msg      = 'abnormal exit'
            self._send_q   = []
            self._lastsend = time.time()
            self._lastrecv = time.time()
            #
            # inner loop run for the life of the connection
            #
            self.execute()
            #
            # handle connection termination
            #
            if not self._exit:
                self.info('Reconnect on close: reason: %s', self._msg)

            self.conn.close()
            self.conn = None

            self._send_q = None
            self.rpc_drop()

        return None

    def complete(self):
        self._send_q = None
        self.rpc_drop()

        if self.conn is not None:
            self.conn.close()
            self.conn = None

        if self._msg is not None:
            self.info('Connection closed: reason: %s', self._msg)

        return None

    def shutdown(self):
        if not self._exit:
            self._exit = True
            self._msg  = 'shutdown'

        if self._cond is not None:
            self._cond.wake_all()

        if self.conn is not None:
            self.conn.wake()

        if self._pending is not None:
            self._pending.wake()
    
    def connect(self):
        for host, port in self._addrs:
            try:
                self._pending = coro.make_socket(
                    socket.AF_INET, socket.SOCK_STREAM)
                self._pending.connect((host, port))
            except:
                self._pending = None
            else:
                self._host = host
                self._port = port
                break

        conn, self._pending = self._pending, None
        return conn

    def establish(self):
        self._cond = coro.coroutine_cond()
        self._tout = 0.0
        #
        # Keep trying for a connection until either:
        # 1. self._exit is set
        # 2. A valid connect is established.
        #
        connected = False
        while not self._exit:
            self._cond.wait(self._tout)

            self._tout  = max(self._tout, 1)
            self._tout *= config.PUBSUB_MAX_RETRY_BACKOFF
            self._tout  = min(config.PUBSUB_MAX_RETRY_WAIT, self._tout)
            #
            # Try all of our known partners.
            #
            conn = self.connect()
            if conn is None:
                continue

            if self.handshake(conn):
                #
                # Done if we communicated with our partner.
                #
                break

        self._cond = None
        self._active.wake_all()
            
        return not self._exit

    def handshake(self, s):
        conn = command.ReadWriter(s)
        conn.settimeout(config.PUBSUB_HANDSHAKE_TIMEOUT)

        data = {
            'id':      self._self_id,
            'idle':    self._idletime * 2,
            'version': server.PEER_VERSION}
            
        push = message.Push(self._icmd, data)

        try:
            conn.write_command(push)
        except:
            self._msg = 'write error during handshake'
            return False

        try:
            result = conn.read_command()
        except:
            self._msg = 'read error during handshake'
            return False

        conn.settimeout(None)

        if result.cmd != 'establish':
            self._msg = 'wrong command <%s> during handshake' % (result.cmd)
            return False

        self._peer_id   = result.params['id']
        self._peer_ver  = result.params.get('version', 0)
        self._peer_idle = result.params.get('idle', 0)
        #
        # get peer address
        #
        if self._peer_ver < server.PEER_VERSION_ADR:
            addr = self._peer_id.split(':')[:2]
            addr = (addr[0], int(addr[1], 16))
        else:
            addr = tuple(result.params['addr'])

        self.peer_addr = addr
        self.conn = conn
        return True

    def dispatch(self, cmd):
        if cmd.cmd == 'rpc_response':
            self.rpc_response(cmd.params['seq'], cmd.params['results'])

        elif cmd.cmd in ['shutdown', 'subscribe', 'unsubscribe', 'ping']:
            # ignore information messages from remote server.
            pass
        else:
            self.warn('received unhandled command: <%r>', repr(cmd))

        return True

    def push(self, msg):
        if self._send_q is None:
            return False

        try:
            self._send_q.append(msg)
            self.conn.wake()
        except:
            self.error('Error pushing cmd to thread %d' % (self.thread_id()))
            self.traceback()

        return True
    
    def rpc_response(self, seq, value):
        if not self._rpc_q.has_key(seq):
            return None

        for item in value:
            self._rpc_q[seq].append(item)

        del(self._rpc_q[seq])
    #
    # Consumer API
    #
    def readiness(self, timeout = None):
        '''readiness

        Wait for the publisher to service requests up to timeout seconds
        '''
        if self.conn is None:
            self._active.wait(timeout)

    def publish(self, object, id, cmd, args):
        '''publish

        Send a one-way notification message
        '''

        params = dict(zip(SEARCH_TREE_PATH, (object, id, cmd)))
        params.update({'args': args})

        self.push(message.Push('update', params))

    def rpc(self, object, id, cmd, args, timeout = None, **kwargs):
        '''rpc

        Synchronous RPC call. Result is RPC response result.
        '''
        return self.rpcs(object, [id], cmd, args, timeout)[0]

    def rpcs(self, object, id_list, cmd, args, timeout = None, **kwargs):
        '''rpcs

        Synchronous RPC call for a set of IDs. Result is RPC response result.
        '''

        sync = RPCSync(id_list)

        for value in id_list:
            if not self.rpc_call(object, value, cmd, args, sync):
                sync.rpc_response(object, value, cmd, None)

        return sync.wait_response(timeout)

    def rpc_call(self, object, id, cmd, args, source):
        '''rpc_call

        Asynchronous RPC call. Result is RPC status, and RPC response
        result is delivered to source rpc_resonse method if status
        was True.
        '''

        self._rpc_seq += 1

        params = dict(zip(SEARCH_TREE_PATH, (object, id, cmd)))
        params.update({'args': args, 'seq': self._rpc_seq})

        self._rpc_q[self._rpc_seq] = RPCHandle(source, params)

        result = self.push(message.Push('rpc_call', params))
        if not result:
            del(self._rpc_q[self._rpc_seq])

        return result

    def rpc_drop(self, destination = None):
        '''rpc_drop

        Cancel all RPC requests, whose source is the destination object,
        without generating a response. When destination is not set all
        outstanding RPC requests are cancelled.
        '''

        for seq, rpc in self._rpc_q.items():
            if destination == rpc or destination is None:
                rpc.kill()
                del(self._rpc_q[seq])
#
# convenience for connection to random btserv.
#
def get_publisher_thread():
    return NotifyPublisher(args = (server.get_publish_address(),))

class NotifyClient(NotifyClientBase):
    '''NotifyClient

    notification publish/subscribe connection handler.
    '''
    def __init__(self, *args, **kwargs):
        super(NotifyClient, self).__init__(*args, **kwargs)

        self._server  = kwargs['server']
        self._self_id = self._server._self_id
        self._state   = CLIENT_STATE_INIT

        self.peer_addr = kwargs.get('addr', None)
        self.root_addr = self._server.addr
        self.peerlocal = False

        self._rcnt = None

    def run(self, conn = None):
        self.state(CLIENT_STATE_CONNECTING)

        while not self._exit:
            if not self.establish(conn):
                break

            if not self._server.peer_add(self):
                break

            self.state(CLIENT_STATE_ESTABLISHED)
            #
            # all other exits should be logged.
            self._msg = 'abnormal exit'
            self._send_q   = []
            self._lastsend = time.time()
            self._lastrecv = time.time()
            #
            # publish subscriptions
            self.publish_subscriptions()
            #
            # inner loop run for the life of the connection
            #
            self.execute()
            #
            # handle connection termination
            #
            if not self.reconnect():
                break

            self.debug(
                'Reconnect on close: %s reason: %s',
                self.peer_addr,
                self._msg)

            self.state(CLIENT_STATE_RECONNECT)

            self._server.client_unsubscribe_all(self)
            self._server.rpc_drop(self)

            self._send_q = None

            self.conn.close()
            self.conn = None

        self.state(CLIENT_STATE_EXITING)
        return None

    def complete(self):
        self._send_q = None

        if self.conn is not None:
            self.conn.close()
            self.conn = None

        if self._server is not None:
            self._server.client_unsubscribe_all(self)
            self._server.rpc_drop(self)
            self._server.peer_del(self)

            self.state(CLIENT_STATE_DEAD)
            self._server = None

        self.debug(
            '%s exiting: %s reason: %r',
            self.__class__.__name__,
            self.peer_addr,
            self._msg)

        return None

    def state(self, state):
        self._state = state
        self._server.peer_ping()

    def handshake(self, s):
        if config.PUBSUB_WARN_LIMIT:
            self.conn = command.ReadWriter(
                s,
                ser = {
                    'func':   serialize_callback,
                    'offset': config.PUBSUB_WARN_LIMIT
                    },
                des = {
                    'func':   deserialize_callback,
                    'offset': config.PUBSUB_WARN_LIMIT
                    }
                )
        else:
            self.conn = command.ReadWriter(s)

        self.conn.settimeout(config.PUBSUB_HANDSHAKE_TIMEOUT)
        self.conn.setsockopt(
            socket.SOL_SOCKET, socket.SO_SNDBUF, SEND_BUF_SIZE)
        self.conn.setsockopt(
            socket.SOL_SOCKET, socket.SO_RCVBUF, RECV_BUF_SIZE)

        result = self._handshake()
        if not result:
            self.conn.close()
            self.conn = None

        return result

    def _handshake(self):
        data = {
            'id':      self._self_id,
            'addr':    self.root_addr,
            'idle':    self._idletime * 2,
            'version': server.PEER_VERSION}

        push = message.Push('establish', data)
        try:
            self.conn.write_command(push)
        except:
            return False

        try:
            result = self.conn.read_command()
        except:
            return False

        self.conn.settimeout(None)
        #
        # decode response and determine peer type.
        #
        if result.cmd not in ('establish', 'publisher'):
            self.warn('wrong command <%s> during handshake', result.cmd)
            return False

        self._peer_id   = result.params['id']
        self._peer_ver  = result.params.get('version', 0)
        self._peer_idle = result.params.get('idle', 0)

        if result.cmd != 'establish':
            self._type = PEER_TYPE_PUBLISHER
            return True

        self._type = PEER_TYPE_FULL
        #
        # get peer address
        #
        if self._peer_ver < server.PEER_VERSION_ADR:
            addr = self._peer_id.split(':')[:2]
            addr = (addr[0], int(addr[1], 16))
        else:
            addr = tuple(result.params['addr'])

        if self.peer_addr is not None and self.peer_addr != addr:
            self.warn(
                'peer address mismatch: %r %r %r',
                type(self),
                self.peer_addr,
                addr)

        self.peer_addr = addr
        self.peerlocal = bool(self.peer_addr[0] == self.root_addr[0])
        return True

    def peer_id(self):
        return self._peer_id

    def type(self):
        return self._type

    def dispatch(self, cmd):
        if cmd.cmd == 'update':
            self._server._publish(
                cmd.params['object'],
                cmd.params['id'],
                cmd.params['cmd'],
                cmd.params['args'],
                remote = bool(self._type != PEER_TYPE_FULL))
        elif cmd.cmd == 'rpc_call':
            if isinstance(cmd.params['id'], list):
                self._server._rpcs_call(
                    RPCMetaHandle(
                        cmd.params['object'],
                        cmd.params['id'],
                        cmd.params['cmd'],
                        cmd.params['args'],
                        self,
                        seq = cmd.params.get('seq', None)),
                    remote = bool(self._type != PEER_TYPE_FULL))
            else:
                self._server._rpc_call(
                    RPCHandle(self, cmd.params, cmd.params.get('seq', None)),
                    remote = bool(self._type != PEER_TYPE_FULL))
        elif cmd.cmd == 'shutdown':
            # ignore information message from remote server.
            pass
        elif cmd.cmd == 'rpc_response':
            if self._type == PEER_TYPE_FULL:
                self._server.rpc_response(
                    cmd.params['seq'], cmd.params['results'], True)
        elif cmd.cmd == 'subscribe':
            if isinstance(cmd.params, dict):
                subscriptions = [cmd.params]
            else:
                subscriptions = cmd.params

            for subscription in subscriptions:
                self._server.client_subscribe(
                    subscription.get('group'),
                    subscription.get('object'),
                    subscription.get('id', (0, 0)),
                    self,
                    subscription.get('info', SUB_TYPE_FULL))

        elif cmd.cmd == 'unsubscribe':
            self._server.client_unsubscribe(
                cmd.params.get('group'),
                cmd.params.get('object'),
                cmd.params.get('id', (0, 0)),
                self)

        elif cmd.cmd == 'ping':
            # increased last receive time for this peer.
            pass
        else:
            self.warn('received unknown command: <%r>', repr(cmd))

        return True

    def push(self, cmd, head = False):
        if self._send_q is None:
            return False

        if head:
            self._send_q.insert(0, cmd)
        else:
            self._send_q.append(cmd)
            
        try:
            self.conn.wake()
        except:
            self.error('Error pushing cmd to thread %d' % (self.thread_id()))
            self.traceback()
            return False
        else:
            return True

    def notify(self, object, id, cmd, args):
        params = dict(zip(SEARCH_TREE_PATH, (object, id, cmd)))
        params.update({'args': args})
        self.push(message.Push('update', params))

    def rpc_call(self, object, id, cmd, args, seq, srv):
        if self._peer_ver < server.PEER_VERSION_RPC:
            return False

        params = dict(zip(SEARCH_TREE_PATH, (object, id, cmd)))
        params.update({'args': args, 'seq': seq})

        return self.push(message.Push('rpc_call', params), head = True)

    def rpc_response(self, object, id, cmd, results, seq = None):
        params = dict(zip(SEARCH_TREE_PATH, (object, id, cmd)))
        params.update({'results':results})
        if seq is not None:
            params.update({'seq': seq})
        
        self.push(message.Push('rpc_response', params), head = True)
        return None

    def publish_subscriptions(self):
        if self._type == PEER_TYPE_PUBLISHER:
            return None

        if self._peer_ver < server.PEER_VERSION_SUB:
            return None

        if self._peer_ver < server.PEER_VERSION_LST:
            for value in self._server.client_subscribes():
                self.push(message.Push('subscribe', value))

            return None

        subscriptions = self._server.client_subscribes()
        if not subscriptions:
            return None

        self.push(message.Push('subscribe', subscriptions))


class PassiveNotifyClient(NotifyClient):
    def establish(self, conn):
        self.state(CLIENT_STATE_CONNECTED)

        if conn is None:
            raise NotifierError, 'Unexpected conn argument: %r' % (conn,)

        return self.handshake(conn)

    def cmp(self):
        return self.peer_addr > self.root_addr
    def reconnect(self):
        return False

    def shutdown(self):
        if not self._exit:
            self._exit = True
        if self.conn is not None:
            self.conn.wake()

class ActiveNotifyClient(NotifyClient):
    def __init__(self, *args, **kwargs):
        super(ActiveNotifyClient, self).__init__(*args, **kwargs)

        self.pending = None
        self._tout   = 0.0
        self._cond   = None

    def toggle_error(self, entering):
        if self._exit:
            return None

        if entering and not self._in_error:
            self._in_error = True
            self.error(
                'Active handshake entering error state: %r' % (
                self.peer_addr,))

        if not entering and self._in_error:
            self._in_error = False
            self.error(
                'Active handshake leaving error state: %r' % (
                self.peer_addr,))
        
    def connect(self):
        if self._exit:
            return None

        try:
            self.pending = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.pending.connect(self.peer_addr)
        except:
            self.pending = None

        conn, self.pending = self.pending, None
        return conn

    def establish(self, conn):
        if conn is not None:
            raise NotifierError, 'Unexpected conn argument: %r' % (conn,)

        self._cond = coro.coroutine_cond()
        self._tout = 0.0
        self._rcnt = -1

        while not self._exit:
            self._cond.wait(self._tout)

            self._rcnt += 1
            self._tout  = max(self._tout, 1)
            self._tout *= config.PUBSUB_MAX_RETRY_BACKOFF
            self._tout  = min(config.PUBSUB_MAX_RETRY_WAIT, self._tout)
            self._tout  = random.uniform(
                self._tout - (self._tout * config.PUBSUB_MAX_RETRY_SKEW),
                self._tout + (self._tout * config.PUBSUB_MAX_RETRY_SKEW))
            
            self.state(CLIENT_STATE_CONNECTING)

            conn = self.connect()
            if conn is None:
                continue

            if self.handshake(conn):
                break

            self.toggle_error(True)

        self.state(CLIENT_STATE_CONNECTED)

        self.toggle_error(False)
        self._cond = None
        return not self._exit

    def bump(self):
        if self._cond is not None:
            self._tout = 0.0
            self._cond.wake_all()

            return True

        return super(ActiveNotifyClient, self).bump()

    def cmp(self):
        #
        # The result is knowable even before this active connection is
        # launched. We perform the execution for now, in case we want to
        # use it as a signal to the remote side to launch an active
        # connection
        #
        return self.root_addr > self.peer_addr
    def reconnect(self):
        return not self._exit
    def shutdown(self):
        if not self._exit:
            self._exit = True
        if self._cond is not None:
            self._cond.wake_all()
        if self.conn is not None:
            self.conn.wake()
        if self.pending is not None:
            self.pending.wake()

def _slice_options(weight):
    if weight == SUB_TYPE_SLICE.get('weight'):
        optional = SUB_TYPE_SLICE
    else:
        optional = SUB_TYPE_SLICE.copy()
        optional.update({'weight': weight})

    return optional

def weighted_choice(targets):
    accw = sum(map(lambda i: i[1].get('weight', 1.0), targets))
    rand = random.random() * accw

    for choice in targets:
        accw -= choice[1].get('weight', 1.0)
        if rand > accw:
            break

    return choice

def _target_filter(targets):
    if len(targets) < 2:
        return map(lambda i: i[0], targets)

    result = filter(lambda i: i[1]['type'] == 'full', targets)
    if len(result) != len(targets):
        result.append(
            weighted_choice(
                filter(lambda i: i[1]['type'] == 'slice', targets)))

    return map(lambda i: i[0], result)

class NotifyServer(coro.Thread):
    #
    # internal API
    #
    def __init__(self, *args, **kwargs):
        super(NotifyServer, self).__init__(*args, **kwargs)
        self._st = SubscriptionTracker(
            SEARCH_TREE_DEF, group = 'update', external = EXTERN_TREE_DEF)
        self._rp = SubscriptionTracker(
            SEARCH_TREE_DEF, group = 'rpc_call', external = EXTERN_TREE_DEF)
        self._cl = SubscriptionTracker(
            CLIENT_TREE_DEF)

        self._full = {}
        self._pubs = {}
        self._cmds = []
        self._smap = {}

        self._rpc_map = {}
        self._rpc_seq = 0
        self._rpc_q   = {}

        self._stoptime = SHUTDOWN_TIMEOUT
        self._exit     = False
        self._l        = None
        self._peerwait = coro.coroutine_cond()
        self._station  = RPCStation()

    def run(self, smap, port = 0, host = '', hostname = ''):
        #
        # first determine self and an entry in the smap
        #
        hostlist = ('localhost', hostname or server.gethostname())
        addrlist = map(lambda i: (i, port), hostlist)
        intrlist = set(smap) & set(addrlist)

        self.addr = intrlist.pop()
        if self.addr is None:
            self.error('No personalities %r in the notifier map.' % addrlist)
            return None

        if intrlist:
            self.error('Left over possible personalities: %r' % list(intrlist))
            return None
        
        self._self_id = '%s:%x' % self.addr
        #
        # listen
        #
        self._l = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM)
        self._l.set_reuse_addr()
        self._l.bind((host, port))
        self._l.listen(8192)

        self.info('Notifier listening: %r', self.addr)
        #
        # start active threads
        self.load(smap)

        # accept passive threads
        #
        while not self._exit:

            if self._cmds:
                cmd, args, kwargs = self._cmds[0]
                del(self._cmds[0])

                handler = getattr(self, cmd, None)
                if handler is None:
                    continue

                try:
                    result = handler(*args, **kwargs)
                except exceptions.Exception, e:
                    self.traceback()

            try:
                s, addr = self._l.accept()
            except socket.error, e:
                self.warn('Notify server socket error: %s', str(e))
                break
            except coro.CoroutineSocketWake:
                continue
            
            thread = PassiveNotifyClient(args = (s,), server = self)
            thread.start()
        #
        #
        self.info('Notifier exiting (children: %d)', self.child_count())

        self._peerwait.wake_all()
        self._exit = True
        self._l.close()

        for child in self.child_list():
            child.shutdown()

        zombies = self.child_wait(self._stoptime)
        if not zombies:
            return None

        self.info('httpd server timeout on %d zombies', zombies)
        for zombie in self.child_list():
            self.info('  notifier zombie: %r %r', zombie.peer_addr, zombie)

        return None

    def complete(self):
        # self._rpc_map = {}
        # self._rpc_q   = {}
        # self._full = {}
        # self._pubs = {}
        # self._subs = {}
        # self._rpcs = {}
        # self._st = None
        # self._rp = None
        return None

    def shutdown(self, **kwargs):
        if self._exit:
            return None

        self._peerwait.wake_all()
        self._stoptime = kwargs.get('timeout', self._stoptime)
        self._exit = True

        if self._l is not None:
            self._l.wake()

    def reload(self, smap):
        self._cmds.append(('load', (smap,), {}))
        self._l.wake()

    def load(self, smap):
        #
        # sets for new smap and current connected clients.
        #
        nsmap  = set(smap)
        csmap  = set(self._full.keys())

        remove = csmap - nsmap
        insert = nsmap - csmap
        #
        # remove old
        for addr in remove:
            #
            # To prevent connection thrashing match only active clients,
            # the passives will be dropped when the active is removed
            # from the remote service. 
            #
            if isinstance(self._full[addr], ActiveNotifyClient):
                self._full[addr].shutdown()
        #
        # add new
        for addr in insert:
            self._full[addr] = ActiveNotifyClient(addr = addr, server = self)
            self._full[addr].start()
        #
        # update
        self._smap = smap
        #
        # info
        self.info('Notifier map: started %d connections.', len(insert))
        self.info('Notifier map: removed %d connections.', len(remove))

    def push_all(self, msg, version = server.PEER_VERSION_MIN):
        for peer in self._full.values():
            if not (peer._peer_ver < version):
                peer.push(msg)

    def command_push_all(self, command, result):
        if result:
            self.push_all(
                message.Push(command, result),
                server.PEER_VERSION_SUB)
    #
    # external API
    #
    #
    # notification registration management
    #
    def slice(self, object, id, cmd, destination, weight = 1.0):
        result = self._st.subscribe(
            object, id, cmd, destination, optional = _slice_options(weight))
        self.command_push_all('subscribe', result)
        
    def subscribe(self, object, id, cmd, destination):
        result = self._st.subscribe(object, id, cmd, destination)
        self.command_push_all('subscribe', result)

    def unsubscribe(self, object, id, cmd, destination):
        result = self._st.unsubscribe(object, id, cmd, destination)
        self.command_push_all('unsubscribe', result)

    def unsubscribe_all(self, destination):
        results = self._st.unsubscribe_all(destination)
        for result in results:
            self.command_push_all('unsubscribe', result)
    #
    # rpc registration management
    #
    def rpc_slice(self, object, id, cmd, destination, weight = 1.0):
        result = self._rp.subscribe(
            object, id, cmd, destination, optional = _slice_options(weight))
        self.command_push_all('subscribe', result)

    def rpc_register(self, object, id, cmd, destination):
        result = self._rp.subscribe(object, id, cmd, destination)
        self.command_push_all('subscribe', result)

    def rpc_unregister(self, object, id, cmd, destination):
        result = self._rp.unsubscribe(object, id, cmd, destination)
        self.command_push_all('unsubscribe', result)

    def rpc_unregister_all(self, destination):
        results = self._rp.unsubscribe_all(destination)
        for result in results:
            self.command_push_all('unsubscribe', result)
    #
    # notification
    #
    def publish(self, object, id, cmd, args):
        self._publish(object, id, cmd, args)
    #
    # SplitAccess RPC 
    #
    def rpcs_push(self, object, id_list, cmd, args, **kwargs):
        for id in id_list:
            params = dict(zip(SEARCH_TREE_PATH, (object, id, cmd)))
            params.update({'args': args})
            
            seq = self._station.push(object, id, cmd, args)

            self._rpc_call(
                RPCHandle(self._station, params, seq = seq),
                remote = kwargs.get('remote', True),
                local  = kwargs.get('local', True))
        #
        # Nothing in the loop yields, so sequence IDs are contiguous,
        # otherwise the API would need to return the full sequence ID
        # list.
        #
        return ((seq + 1) - len(id_list), len(id_list))

    def rpcs_recv(self, seq_list, **kwargs):
        return self._station.wait(seq_list, **kwargs)

    def rpcs_clear(self, seq_list):
        return self._station.clear(seq_list)

    def rpcs_pop(self, seq_set):
        return self._station.pop(seq_set)
    #
    # Synchronous RPC
    #
    def rpcs(
        self, object, id_list, cmd, args, timeout = None,
        remote = True, local = True):
        sync = RPCSync(id_list)

        for value in id_list:
            self.rpc_call(object, value, cmd, args, sync, remote, local)

        return sync.wait_response(timeout)

    def rpc(
        self, object, id, cmd, args, timeout = None,
        remote = True, local = True):
        return self.rpcs(object, [id], cmd, args, timeout, remote, local)[0]
    #
    # Asynchronous RPC
    #
    def rpc_call(
        self, object, id, cmd, args, source,
        remote = True, local = True):
        params = dict(zip(SEARCH_TREE_PATH, (object, id, cmd)))
        params.update({'args': args})

        rpc = RPCHandle(source, params)
        return self._rpc_call(rpc, remote = remote, local = local)
    #
    # RPC responses
    #
    def rpc_response(self, seq, value = None, remote = False):
        if not self._rpc_q.has_key(seq):
            return None
        if value is not None:
            if not remote:
                value = [value]
            for item in value:
                self._rpc_q[seq].append(item)

        del(self._rpc_map[seq])
        del(self._rpc_q[seq])

    def rpc_drop(self, destination):
        for seq, rpc in self._rpc_q.items():
            if destination == rpc:
                rpc.kill()
                del(self._rpc_map[seq])
                del(self._rpc_q[seq])

        for seq, target in self._rpc_map.items():
            if destination == target:
                del(self._rpc_map[seq])
                del(self._rpc_q[seq])
    #
    # client thread API
    #
    def client_subscribe(self, command, object, id, client, info):
        self._cl.subscribe(command, object, id, client, optional = info)

    def client_unsubscribe(self, command, object, id, client):
        self._cl.unsubscribe(command, object, id, client)

    def client_unsubscribe_all(self, client):
        self._cl.unsubscribe_all(client)

    def client_subscribes(self):
        return self._st.subscribes() + self._rp.subscribes()

    def peer_ping(self):
        '''peer_ping

        wake any API consumer waiting on peer status changes
        '''
        self._peerwait.wake_all()

    def peer_add(self, client):
        if self._exit:
            return False

        try:
            if client.type() == PEER_TYPE_FULL:
                result = client.cmp()
                if not result:
                    self._full.get(client.peer_addr, client).bump()
                    return False

                previous =     self._full.pop(client.peer_addr, client)
                if previous is not client:
                    previous.shutdown()

                self._full[client.peer_addr] = client
                return True

            if client.type() == PEER_TYPE_PUBLISHER:
                self._pubs[client] = client
                return True
            #
            # unknown type
            #
            return False
        finally:
            self.peer_ping()

    def peer_del(self, client):
        try:
            if client.type() == PEER_TYPE_FULL:

                if client is self._full.get(client.peer_addr, None):
                    self._full.pop(client.peer_addr)

            elif client.type() == PEER_TYPE_PUBLISHER:

                if client is self._pubs.get(client, None):
                    del(self._pubs[client])
            else:
                pass

            return None
        finally:
            self.peer_ping()

    def peer_wait(self, active = 60.0, passive = 5.0, retry = 2):
        active  = interval.Timeout(active)
        passive = interval.Timeout(passive)
        timeout = max(active, passive)
        
        while not self._smap and timeout:
            self._peerwait.wait(timeout())

        if not self._smap:
            return False

        full = set(self._smap) - set([self.addr])
        while full and timeout:
            #
            # calculate completed/connected clients and remove them
            # from the full wait set.
            #
            done = filter(
                lambda i: init_threashold(i[1], retry),
                self._full.items())
            full = full - set(map(lambda i: i[0], done))
            #
            # if there are no longer active clients on which we
            # are waiting, switch to the passive client timeout
            #
            wait = filter(lambda i: self._full.get(i), full)
            if not wait:
                timeout = passive

            self._peerwait.wait(timeout())

        return bool(full)
    #
    # client and server API (remote variable)
    #
    def _publish(self, object, id, cmd, args, remote = True):
        params = dict(zip(TOTAL_TREE_PATH, ('update', object, id, cmd)))
        targets = self._st.lookup(params)

        if remote:
            others = self._cl.lookup(params)
        else:
            others = []

        self.debug(
            "Publication: %r remote: %r targets: %d, %d",
            (object, id, cmd),
            remote,
            len(targets),
            len(others))

        targets.extend(others)

        for target in _target_filter(targets):
            try:
                target.notify(object, id, cmd, args)
            except:
                pass

    def _rpc_call(self, rpc, remote = True, local = True):
        if local:
            targets = self._rp.lookup(rpc)
        else:
            targets = []

        if remote:
            params = dict(zip(
                CLIENT_TREE_PATH,
                ('rpc_call', rpc['object'], rpc['id'])))
            others = self._cl.lookup(params)
            others = dict(others).items()
        else:
            others = []

        self.debug(
            "RPC: %r remote: %r targets: %d, %d",
            (rpc['object'], rpc['id'], rpc['cmd']),
            remote,
            len(targets),
            len(others))

        targets.extend(others)

        for target in _target_filter(targets):
            if not hasattr(target, 'rpc_call'):
                continue

            self._rpc_seq += 1
            self._rpc_q[self._rpc_seq] = rpc
            self._rpc_map[self._rpc_seq] = target

            result = target.rpc_call(
                rpc['object'], rpc['id'], rpc['cmd'], rpc['args'],
                self._rpc_seq, self)
            #
            # since local targets do not have to return any value, the
            # defualt return value of None is treated as a sucess.
            #
            if result is False:
                del(self._rpc_map[self._rpc_seq])
                del(self._rpc_q[self._rpc_seq])

        return None

    def _rpcs_call(self, meta_rpc, **kwargs):
        for rpc in meta_rpc:
            self._rpc_call(rpc, **kwargs)
#
# standalone test interface, not for normal operation.
#
def run(smap, mainport, backport, loglevel, log):
    
    coro.spawn(backdoor.serve, backport)
    #
    # server
    server = NotifyServer(log = log, args=(smap,), kwargs={'port': mainport})
    server.set_log_level(loglevel)
    server.start()
    #
    # primary event loop.
    coro.event_loop()
    #
    # never reached...
    return None

COMMAND_LINE_ARGS = [
    'help', 'fork', 'port=', 'backdoor=', 'pidfile=', 'logfile=', 'loglevel=',
    'addr=']

def usage(name, error = None):
    if error:
        print 'Error:', error
    print "  usage: %s [options]" % name
 
def main(argv, environ):
    progname = sys.argv[0]

    backport = 9876
    mainport = 7221
    logfile  = None
    pidfile  = None
    loglevel = 'INFO'
    dofork   = False
    forklist = [progname]
    smap     = []

    dirname  = os.path.dirname(os.path.abspath(progname))
    os.chdir(dirname)

    try:
        list, args = getopt.getopt(argv[1:], [], COMMAND_LINE_ARGS)
    except getopt.error, why:
        usage(progname, why)
        return None

    for (field, val) in list:
        if field == '--help':
            usage(progname)
            return None
        elif field == '--backdoor':
            backport = int(val)
        elif field == '--port':
            mainport = int(val)
        elif field == '--logfile':
            logfile = val
        elif field == '--loglevel':
            loglevel = val
        elif field == '--pidfile':
            pidfile = val
        elif field == '--fork':
            dofork = True
            continue
        elif field == '--addr':
            try:
                s = string.split(val, ':')
                s[1] = int(s[1])
                smap.append(tuple(s))
            except:
                print 'Invalid address: %s' % (val,)

        forklist.append(field)
        if val:
            forklist.append(val)

    if dofork:
        pid = os.fork()
        if pid:
            return
        else:
            os.execvpe(progname, forklist, environ)
        
    if pidfile:
        try:
            fd = open(pidfile, 'w')
        except IOError, e:
            print 'IO error: %s' % (e.args[1])
            return None
        else:
            fd.write('%d' % os.getpid())
            fd.close()

    if logfile:
        hndlr = logging.FileHandler(logfile)
        
        os.close(sys.stdin.fileno())
        os.close(sys.stdout.fileno())
        os.close(sys.stderr.fileno())
    else:
        hndlr = logging.StreamHandler(sys.stdout)
        
    log = coro.coroutine_logger('notify')
    fmt = logging.Formatter(server.SRV_LOG_FRMT)

    log.setLevel(logging.DEBUG)

    sys.stdout = coro.coroutine_stdout(log)
    sys.stderr = coro.coroutine_stderr(log)

    hndlr.setFormatter(fmt)
    log.addHandler(hndlr)
    loglevel = server.LOGLEVELS.get(loglevel, None)
    if loglevel is None:
        log.warn('Unknown logging level, using INFO: %r' % (loglevel, ))
        loglevel = logging.INFO

    run(smap, mainport, backport, loglevel, log)
    return None

if __name__ == '__main__':
    main(sys.argv, os.environ)
