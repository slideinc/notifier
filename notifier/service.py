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

"""service

A rpc service provider, to be executed within the coroutine framework
"""

from gogreen import coro
from gogreen import corowork

import exceptions
import time
import sys
import os

import decorators
import error
import access



def statistics_filter(args, kwargs):
    return args[1].get('command', 'none')

class SimpleWorker(corowork.Worker):
    def __init__(self, *args, **kwargs):
        super(SimpleWorker, self).__init__(*args, **kwargs)

        self._notifier = kwargs['notifier']
        self._waiter   = coro.coroutine_cond()
        self._objname  = kwargs['object']
        self._msgqueue = []

    def execute(self, vid, call, seq, server, **kwargs):
        cmd      = call.get('command', None)
        args     = call.get('args', ())
        kwargs   = call.get('kwargs', {})
        tlb, tval = call.get('tlb', ('tlb-%s' % self._objname, False))
        slv, sval = call.get('slave', ('slave-read', False))
        source    = call.get('source')

        self.debug(
            'execute command %r id %r args %r kwargs %r tlb %s slv %s',
            cmd, vid, args, kwargs, (tlb,tval), (slv,sval))

        try:
            coro.set_local(tlb, tval)
            coro.set_local(slv, sval)
            if source: coro.set_local(access.CORO_LOCAL_SOURCE, source)
            try:
                result = self._execute(vid, cmd, args, kwargs)
            finally:
                coro.pop_local(slv)
                coro.pop_local(tlb)
                coro.pop_local(access.CORO_LOCAL_SOURCE)
        except error.AccessError, e:
            self.warn('AccessError: %r %r' % (e, e.args,))

            result = {
                'rc':   e.id,
                'msg':  e[0],
                'args': e.args,
                'envl': True}

            self.clear()
        except exceptions.Exception, e:
            self.traceback()
            t,v,tb = coro.traceback_info()
            result = {
                'rc':  error.ServiceTraceback.id,
                'tb':  tb,
                'msg': 'Traceback: [%s|%s]' % (t,v),
                'args': getattr(e, 'args', str(e)),
                'envl': True}
        except:
            self.traceback()
            t,v,tb = coro.traceback_info()
            result = {
                'rc':  error.ServiceTraceback.id,
                'tb':  tb,
                'msg': 'Traceback: [%s|%s]' % (t,v),
                'envl': True}

            self.clear()
        else:
            self.flush()

        server.rpc_response(seq, result)
        
    def _execute(self, vid, cmd, args, kwargs):
        handler = getattr(self, cmd, None)
        if handler is None or not getattr(handler, 'command', 0):
            return {
                'rc': error.NoServiceHandler.id,
                'msg': 'no handler: %s' % cmd,
                'args': (cmd,),
                'envl': True}

        return self._call(handler, vid, *args, **kwargs)

    def _call(self, handler, *args, **kwargs):
        self.preemptable_set(True)
        try:
            result = handler(*args, **kwargs)
        finally:
            self.preemptable_set(False)

        if getattr(handler, 'envelope', False):
            result = {'rc': 0, 'result': result, 'envl': True}

        return result
        
    def complete(self):
        super(SimpleWorker, self).complete()

        self._notifier = None
    #
    # We act as an RPC proxy to ensure that messages are queued and only
    # flushed once the transaction has completed.
    #
    def rpcs(self, *args, **kwargs):
        return self._notifier.rpcs(*args, **kwargs)

    def rpc(self, *args, **kwargs):
        return self._notifier.rpc(*args, **kwargs)

    def publish(self, object, id, cmd, args):
        self._msgqueue.append((object, id, cmd, args))

    def clear(self):
        self._msgqueue = []

    def flush(self):
        for object, id, cmd, args in self._msgqueue:
            self._notifier.publish(object, id, cmd, args)

        self.clear()
    #
    # common testing commands
    #
    @decorators.command
    def sleep(self, vid, timeout):
        self._waiter.wait(timeout)
        return {'timeout': timeout}

    @decorators.command
    def ping(self, vid, args):
        return args


class Worker(SimpleWorker):
    def __init__(self, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)

    def _execute(self, vid, cmd, args, kwargs):
        handler = getattr(self, cmd, None)
        if handler is None or not getattr(handler, 'command', 0):
            return {
                'rc': error.NoServiceHandler.id,
                'msg': 'no handler: %s' % cmd,
                'args': (cmd,),
                'envl': True}

        if not getattr(handler, 'cursor', 0):
            return self._call(handler, vid, *args, **kwargs)

        slave = getattr(handler, 'readonly', False) and access.slave_read()

        extra_dbc_hints = getattr(handler, 'extra_dbc_hints', [])
        extra = {}
        for hint in extra_dbc_hints:
            extra[hint] = kwargs.get(hint)

        if getattr(handler, 'nopartition', 0):
            dbc = self._get_dbc(None, slave = slave, **extra)
        else:
            dbc = self._get_dbc(vid, slave = slave, **extra)

        if dbc is None:
            return {
                'rc': error.DatabaseUnavailable.id,
                'msg': 'DB currently offline',
                'args': (),
                'envl': True}

        try:
            result = self._call(handler, vid, dbc.cursor(), *args, **kwargs)
            dbc.commit()
        finally:
            self._put_dbc(dbc, slave = slave)

        return result

    def complete(self):
        super(Worker, self).complete()


class Server(object):
    worker_class = Worker
    subscription = 'base'
    statistics   = 'basestat'

    def __init__(self, size, notifier, **kwargs):
        self._clean  = False

        if isinstance(size, list):
            kwargs['sizes'] = size
        else:
            kwargs['size']   = size
        kwargs['object'] = self.subscription
        kwargs['worker'] = self.worker_class
        kwargs['notifier'] = notifier
        kwargs['filter'] = lambda *a: '%s.%s' % (
            self.__module__,
            statistics_filter(*a))

        self._server = corowork.Server(**kwargs)
        self._server.name = self.__class__.__name__
        self._server.start()

        self._notifier = kwargs['notifier']
        self._bounds   = {}
        self._active   = False

        if 'loglevel' in kwargs:
            self._server.set_log_level(kwargs['loglevel'])
            self._notifier.set_log_level(kwargs['loglevel'])

        self.load_config(kwargs['bounds'])

    def __del__(self):
        if not self._clean:
            self.drain()

    def active(self):
        return bool(self._bounds)

    def drain(self, timeout = None, grace = 0.0):
        # if not active ignore grace period and timeout
        #
        if not self._active:
            timeout = None
            grace   = 0.0
        self._notifier.rpc_unregister_all(self)
        coro.sleep(grace)
        shutdown_start = time.time()
        join = self._server.shutdown(timeout = timeout)
        return {
            'join' : join, 
            'time' : time.time() - shutdown_start
        }

    def rpc_call(self, obj, id, cmd, args, seq, server):
        if obj == self.statistics:
            self._server.command_push(obj, id, cmd, args, seq, server)
        else:
            prio = getattr(
                self, 'command_prios', {}).get(args.get('command'), 0)
            self._server.request(id, args, seq, server, prio = 0)

    def load_config(self, bounds):
        #
        # work queue (re)size
        #
        self._server.resize(bounds.get('workers'))
        #
        # old and new RPC service mask/value
        #
        oldsub = (self._bounds.get('mask'),  self._bounds.get('value'))
        oldbrd = (self._bounds.get('bmask'), self._bounds.get('bvalue'))
        newsub = (bounds.get('mask'), bounds.get('value'))
        newbrd = (bounds.get('bmask'), bounds.get('bvalue'))
        #
        # old and new service enabler
        #
        oldsrv = self._bounds.get('service', bool(None not in oldsub))
        newsrv = bounds.get('service', bool(None not in newsub))

        oweight = self._bounds.get('weight', 1.0)
        nweight = bounds.get('weight', 1.0)

        if (oldsub, oldbrd, oldsrv, oweight) == (newsub,newbrd,newsrv,nweight):
            return None, None

        unreg = nweight != oweight or oldsub != newsub or \
                oldbrd != newbrd or not newsrv

        if self._active and unreg:
            self._notifier.rpc_unregister_all(self)
            self._active = False

        if not self._active and newsrv:
            for command in self._server.command_list():
                self._notifier.rpc_register(self.statistics, 0, command, self)

            self._notifier.rpc_slice(
                self.subscription, newsub, 'execute', self, weight = nweight)

            if getattr(self, 'sub_broadcast', None):
                self._notifier.rpc_register(
                    self.sub_broadcast, newbrd, 'execute', self)

            self._active = True

        self._bounds = bounds
        # JRM: i think it would be fine to add old and new brd values to
        # this return but not sure how that'll effect the itemservs so will
        # leave to libor to decide
        #
        return (oldsub, oldsrv, oweight), (newsub, newsrv, nweight)
    #
    # statistics
    #
    def rate(self):
        return self._server.stats.rate()
        
    def details(self):
        return self._server.stats.details()

    def averages(self):
        return self._server.stats.averages()
#
# end...
