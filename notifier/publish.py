#
#
#
"""publish

Simple notification publisher.
"""

import socket
import time
import string
import os
import sys
import exceptions
import select

import message
from util import applog
from btserv import command
from btserv import server
from gogreen import coro

from configs.config import config

SEND_BUF_SIZE  = 256*1024
RECV_BUF_SIZE  = 256*1024

ERROR_DATA_MASK = select.POLLIN|select.POLLHUP|select.POLLERR|select.POLLNVAL

HANDSHAKE_TIMEOUT = 10.0
SOCKET_TIMEOUT = 5.0

class SimpleNotifyPublisher(object):
    def __init__(self, addr_list, **kwargs):
        self._peer_id = 0
        self._addr = addr_list
        self._id   = '%s:%x' % (server.gethostname(), os.getpid())
        self._conn = None
        self._err  = 0
        self._msg  = 'no error message'

        self._rpc_rep = {}
        self._rpc_req = {}
        self._rpc_seq = 1

        self._itime = kwargs.get('timeout', SOCKET_TIMEOUT)
        self._ctime = kwargs.get('connect_timeout', SOCKET_TIMEOUT)
        
    def __repr__(self):
        return 'Peer (host: %s, port: %s, id: %r)' % (
            self._addr[0][0], self._addr[0][1], self._peer_id)

    def settimeout(self, timeout):
        self._itime = timeout
        try:
            self._conn.settimeout(timeout)
        except:
            pass

    def gettimeout(self):
        return self._itime

    def setconnecttimeout(self, timeout):
        self._ctime = timeout

    def getconnecttimeout(self):
        return self._ctime

    def error_clear(self):
        self._msg = 'no error message'

    def error(self, msg):
        self._err += 1
        self._msg = msg

        if self._conn is not None:
            msg = 'Server: %r <%s>' % (self._addr[0], msg)
        else:
            msg = 'Server: None <%s>' % (msg,)

        applog.log.warn(msg)

        self._clear_conn()

    def _clear_conn(self):
        if self._conn is None:
            return None

        try:
            self._conn.close()
        except:
            pass
        #
        # clear requests for which no responses currently exist,
        # since they will not be forth comming.
        #
        while self._rpc_req:
            seq, req = self._rpc_req.popitem()
            self._rpc_rep[seq] = (seq, req, None)

        self._conn = None
        return None

    def _connect(self):
        #
        # cycle through optional addresses
        #
        for i in range(len(self._addr)):
            host, port = self._addr[i]
            conn = None

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self._ctime)
            try:
                s.connect((host, port))
            except socket.error, e:
                continue

            s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SEND_BUF_SIZE)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RECV_BUF_SIZE)
            s.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

            conn = command.ReadWriter(s)
            conn.settimeout(self._itime)
        
            push = message.Push('publisher', {'id': self._id})
            try:
                conn.write_command(push)
            except socket.error, e:
                continue
            except:
                continue

            try:
                result = conn.read_command()
            except socket.error, e:
                continue
            except:
                continue
            
            if result.cmd != 'establish':
                self.error('wrong handshake command: <%s>' % (result.cmd))
                continue
            #
            # reorder address list to place current connected sockets
            # address at the head of the list
            #
            self._addr = self._addr[i:] + self._addr[:i]

            self._peer_id = result.params['id']
            self.error_clear()

            return conn

        self.error('Could not find suitable server: %r' % self._addr)
        return None
        
    def _get_conn(self):
        if self._conn is not None:
            #
            # check for error. (we never receive unexpected data.)
            #
            if self._rpc_req:
                # expecting data, move error detection to cmd receive
                return self._conn
            #
            # no pending RPC requests, so data pending is an error.
            #
            poll = select.poll()
            poll.register(self._conn)

            result = poll.poll(0.0)
            if not result:
                return self._conn

            fileno, mask = result[0]
            if not mask & ERROR_DATA_MASK:
                return self._conn
            #
            # error/data mask, continue and create a new connection
            #
            try:
                try:
                    cmd = self._conn.read_command()
                except command.ConnectionError:
                    # expected connection error
                    applog.log.info(
                        'lost connection %r. reconnecting' % (self._addr[0],))
                except exceptions.Exception, e:
                    # unexpected error
                    applog.log.warn(
                        'unexpected error during connection check: %r' % (e,))
                else:
                    # unexpected command
                    applog.log.warn(
                        'unexpected command during connection check: %r' % cmd)
            finally:
                self._clear_conn()
        #
        # attempt to connect
        self._conn = self._connect()
        return self._conn
    
    def _rpc_send(self, conn, seq, obj, id, cmd, args):
        data = {'object': obj, 'id': id, 'cmd': cmd, 'args': args, 'seq': seq}
        msg  = message.Push('rpc_call', data)

        try:
            conn.write_command(msg)
        except command.ConnectionError, e:
            self.error('connection error writing rpc: %s' % e)
            return False
        except socket.error, e:
            self.error('rpc write error: %s' % e)
            return False
        except exceptions.Exception, e:
            self.error('unexpected rpc write error: %s' % e)
            raise e

        return True

    def _rpc_recv(self, conn):
        if conn is None:
            return None

        try:
            cmd = conn.read_command()
        except command.ConnectionError, e:
            self.error('connection error reading rpc: %s (timeout: %s)' % (e, conn.gettimeout()))
            return None
        except socket.error, e:
            self.error('rpc read error: %s' % e)
            return None
        except exceptions.Exception, e:
            self.error('unexpected rpc read error: %s' % e)
            raise e

        return cmd.params

    def _rpcs_next(self, conn):
        if conn is None:
            return None

        cmd = self._rpc_recv(conn)
        if cmd is None:
            #
            # clear connection and outstanding requests
            #
            self._clear_conn()
            return None

        seq = cmd.get('seq', 0)
        if not seq:
            #
            # unexpected message
            #
            return None

        self._rpc_rep[seq] = (
            seq,
            self._rpc_req.pop(seq),
            cmd.get('results', []))

        return seq

    def _rpcs_clear(self, seqs):
        for seq in seqs:
            self._rpc_req.pop(seq, None)
            self._rpc_rep.pop(seq, None)
    #
    # public API (advanced)
    #
    def _rpcs_send(self, obj, id_list, cmd, args):
        conn = self._get_conn()
        if conn is None:
            return (0, 0)

        for id in id_list:
            status = self._rpc_send(conn, self._rpc_seq, obj, id, cmd, args)
            if not status:
                #
                # If we get an error during send, dump the connection and
                # return an error, since the likelyhood of gettting any
                # data our of the connections is very low.
                #
                self._clear_conn()
                return (0, 0)

            self._rpc_req[self._rpc_seq] = (obj, id, cmd, args)
            self._rpc_seq += 1

        return (self._rpc_seq - len(id_list), len(id_list))

    def _rpcs_recv(self, seq_list):
        result, cnt, conn = [], 0, self._get_conn()

        while cnt < len(seq_list):
            seq  = seq_list[cnt]
            cnt += 1

            if seq in self._rpc_rep:
                #
                # RPC Response has been queued for retrieval
                #
                result.append(self._rpc_rep.pop(seq))
                continue

            if seq not in self._rpc_req:
                #
                # RPC REQ has been voided. (e.g. already retrieved by
                # a previous recv call, or never issued (e.g. invalid
                # seq))
                #
                result.append((seq, None, None))
                continue
            #
            # response has yet to be received, requeue requested sequence ID
            #
            cnt -= 1
            #
            # and fetch next response on the wire.
            #
            self._rpcs_next(conn)

        return result

    def _rpcs_pop(self, seq_set):
        #
        # look at existing repliest
        #
        for seq in self._rpc_rep.iterkeys():
            if seq in seq_set:
                return self._rpc_rep.pop(seq)

        conn = self._get_conn()
        #
        # while outstanding requests exist, keep fetching results.
        # if a match is found return it.
        #
        while self._rpc_req:
            seq = self._rpcs_next(conn)
            if seq is None:
                break

            if seq in seq_set:
                return self._rpc_rep.pop(seq)
        #
        # no matches found, either an error occured or a bad
        # sequence number. check both cases.
        #
        for seq in self._rpc_rep.iterkeys():
            if seq in seq_set:
                return self._rpc_rep.pop(seq)

        return (0, None, None)
    #
    # public API (advanced)
    #
    def rpcs_push(self, obj, id_list, cmd, args, **kwargs):
        timeout = self.gettimeout()

        self.settimeout(kwargs.get('timeout', timeout))
        try:
            return self._rpcs_send(obj, id_list, cmd, args)
        finally:
            self.settimeout(timeout)

    def rpcs_recv(self, seq_list, **kwargs):
        timeout = self.gettimeout()

        self.settimeout(kwargs.get('timeout', timeout))
        try:
            return self._rpcs_recv(seq_list)
        finally:
            self.settimeout(timeout)

    def rpcs_pop(self, seq_set, **kwargs):
        timeout = self.gettimeout()

        self.settimeout(kwargs.get('timeout', timeout))
        try:
            return self._rpcs_pop(seq_set)
        finally:
            self.settimeout(timeout)

    def rpcs_clear(self, seqs):
        return self._rpcs_clear(seqs)
    #
    # public API
    #
    def publish(self, object, id, cmd, args):
        
        params = dict(object = object, id = id, cmd = cmd, args = args)
        msg = message.Push('update', params)

        conn = self._get_conn()
        if conn is None:
            return None
        
        try:
            conn.queue_command(msg)
        except exceptions.Exception, e:
            if coro.current_thread(): # avoid log_compact_traceback in coro
                coro.log.traceback()
                return None
            self.error(applog.log_compact_traceback(
                str, 'Error queueing command: %r' % msg))
            return None

        try:
            conn.flush()
        except command.ConnectionError:
            self.error('connection error publishing command: %r' % (msg,))
        except socket.error, e:
            self.error('publish write timeout: %r' % (msg,))

        return None

    def rpcs(self, obj, idlist, cmd, args, *more, **kwargs):
        # legacy parameter check
        if more: kwargs['timeout'] = more[0]

        timeout = self.gettimeout()

        self.settimeout(kwargs.get('timeout', timeout))
        try:
            seq, cnt = self._rpcs_send(obj, idlist, cmd, args)
            if not cnt:
                return []

            result = self._rpcs_recv(xrange(seq, seq + cnt))
            if not result:
                return []

            if len(result) != cnt:
                applog.log.warn(
                    'RPC size mismatch. <%d:%d>' % (cnt, len(result)))
                return []

            return map(lambda i: i[-1], result)
        finally:
            self.settimeout(timeout)

    def rpc(self, obj, vid, cmd, args, *more, **kwargs):
        # legacy parameter check
        if more: kwargs['timeout'] = more[0]

        result = self.rpcs(obj, [vid], cmd, args, **kwargs)
        if not result:
            return None
        else:
            return result[0]


class SimpleNotifyTarget(object):
    def notify(self, object, id, cmd, args):
        sys.stdout.write(
            'notification: <%s:%d:%s:%r>\n' % (object, id, cmd, args))
        return None

    def rpc_call(self, object, id, cmd, args, seq, server):
        sys.stdout.write(
            'rpc call: <%s:%d:%s:%d:%r>\n' % (object, id, cmd, seq, args))
        server.rpc_response(seq, args)

    def rpc_response(self, object, id, cmd, response):
        sys.stdout.write(
            'rpc response: <%s:%d:%s:%r>\n' % (object, id, cmd, response))
#
# convenience for connection to random btserv.
#
_pub_conn = None

def get_publisher():
    global _pub_conn
    if _pub_conn is None and config.publish_web_btserv:
        assert not coro.current_thread(), 'get_publisher is not intended for coro; use context.notifier() instead'
        addr_list = server.get_publish_address()
        _pub_conn = SimpleNotifyPublisher(addr_list)

    return _pub_conn
#
# end...
