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

"""access

Basic access to object services.
"""

import exceptions
import random as _random
import time
import sys
import os
import socket


from gogreen import coro
import pyinfo
import error

class ServerUnavailable(Exception):
    pass

class ServerError(ServerUnavailable):
    pass

class ClientError(ServerUnavailable):
    pass

class SplitAccessError(Exception):
    pass

class SplitServerError(ServerError, SplitAccessError):
    pass

class SplitClientError(ClientError, SplitAccessError):
    pass

class LookAside(Exception):
    pass

BLANK_TOKEN = None
CORO_LOCAL_TDATA  = 'access-trace-data'
CORO_LOCAL_TCTRL  = 'access-trace-ctrl'
CORO_LOCAL_SOURCE = 'access-call-source'

DEFAULT_RETRY = 2

ACCESS_TRACE_LIMITS = {'obj' : None, 'cmd' : None, 'vids' : None }

ACCESS_TRACE_OFF = 0
ACCESS_TRACE_TERSE = 10
ACCESS_TRACE_INFO = 20
ACCESS_TRACE_DEBUG = 30
ACCESS_TRACE_VERBOSE = 40

def _tlb_status(object):
    return coro.get_local('tlb-%s' % object, False)

def _set_trace_local(value, clear):
    if not coro.has_local(CORO_LOCAL_TCTRL):
        coro.set_local(CORO_LOCAL_TCTRL, {})
        
    data = coro.get_local(CORO_LOCAL_TCTRL)
    data['value'] = value
    data['clear'] = clear

def _get_trace_local():
    data = coro.get_local(CORO_LOCAL_TCTRL, {})
    return data.get('value', False)

def _clr_trace_local():
    if  coro.get_local(CORO_LOCAL_TCTRL, {}).get('clear', False):
        data = coro.pop_local(CORO_LOCAL_TCTRL, {})
    else:
        data = coro.get_local(CORO_LOCAL_TCTRL, {})

    return data.get('value', False)
#
# set local request parameter to use service DB slaves if possible.
#
def slave_read(status = None):
    if status is None:
        return coro.get_local('slave-read', False)
    else:
        return coro.set_local('slave-read', status)
#
# Public trace data/info function(s)
#
def trace_dump(clear = True):
    '''trace_dump

    Dump to stdout current trace data at the current trace level. An optional
    clear parameter to reset or preserve the trace data. (default: True)

    See Also: enable_trace()
    '''
    if clear:
        tdata = coro.pop_local(CORO_LOCAL_TDATA, {})
        tlevl = _clr_trace_local()
    else:
        tdata = coro.get_local(CORO_LOCAL_TDATA, {})
        tlevl = _get_trace_local()

    if not tlevl:
        return None

    total, idcnt, count = 0, 0, 0
    
    for obj, data in tdata.items():
        for cmd, (elapse, ids, cnt) in data.items():
            total += elapse
            count += cnt
            idcnt += ids

            if tlevl > ACCESS_TRACE_TERSE:
                print 'Access | %0.4f | %4d | %4d | Summary (%s.%s)' % (
                    elapse, cnt, ids, obj, cmd)

    if not total:
        return None

    lmt = has_trace_limits()
    if lmt is None:
        lmt = ''
    else:
        lmt = 'limit: %s' % lmt
    print 'Access | %.4f | %4d | %4d | Summary (TOTAL) %s' % (
        total, count, idcnt, lmt)

def enable_trace(value, clear = True):
    '''enable_trace

    Set trace level.
    
    0  - No debug info.
    10 - Info about each access call is saved. A call to trace_dump() will
         produce a single line summary (number of access calls, number of
         IDs, and total time spent in access calls) of the saved data.
    20 - A call to trace_dump() will dump a line for each object/command
         with the same information as the 'total' result above.
    30 - For each call to access echo the call parameters and elapse time
         to stdout.
    40 - When echoing access calls to stdout include the call stack.

    Note: Each level will produce all the data that the previous level
          produces, plus the additional documented data.

    constants:

      ACCESS_TRACE_OFF     = 0
      ACCESS_TRACE_TERSE   = 10
      ACCESS_TRACE_INFO    = 20
      ACCESS_TRACE_DEBUG   = 30
      ACCESS_TRACE_VERBOSE = 40

    Optional parameters:

      clear - Reset/Clear the local override when trace_dump is called with
              the clear parameter set to true. (default: True)

    See Also: trace_dump()
    '''

    _set_trace_local(value, clear)
    return _get_trace_local(), value
#
# Private/Local trace data/info support function(s)
#
sum_tuple = lambda *a: tuple(map(sum, zip(*a)))

def _trace_data(start, obj, cmd, vids, **kwargs):
    if not _get_trace_local():
        return None

    if not _trace_check_limits(obj, cmd, vids):
        return None

    elapse = time.time() - start

    if not coro.has_local(CORO_LOCAL_TDATA):
        coro.set_local(CORO_LOCAL_TDATA, {})
        
    data = coro.get_local(CORO_LOCAL_TDATA)
    data = data.setdefault(obj, {})

    look = not isinstance(vids, (list, tuple)) and 1 or len(vids)

    data[cmd] = sum_tuple(data.get(cmd, (0, 0, 0)), (elapse, look, 1))

    if _get_trace_local() > ACCESS_TRACE_DEBUG:
        stack = pyinfo.rawstack()
        stack = stack[:-5] # remove 5 levels of mod_python, publisher
        while stack and stack[0][0].startswith('access'):
            stack.pop(0)
        stack = ' @ %s' % '|'.join(['%s:%s:%s' % x for x in stack])
    else:
        stack = ''

    if _get_trace_local() < ACCESS_TRACE_DEBUG:
        return None

    print 'Access | %.4f | obj: %s cmd: %s vid: %s args: %s kwargs: %s%s' % (
        elapse, obj, cmd, vids,
        kwargs.get('args', ()), kwargs.get('kwargs', {}),
        stack)

def _execute_trace_decorator():
    '''_execute_trace_decorator

    trace decorator explicitly for the execute function.
    '''
    def function(method):
        def tracer(obj, n, cmd, vids, *args, **kwargs):
            start = _get_trace_local() and time.time() or 0
            try:
                return method(obj, n, cmd, vids, *args, **kwargs)
            finally:
                try:
                    _trace_data(start, obj, cmd, vids, **kwargs)
                except:
                    raise

        return tracer
    return function

def _complete_trace_decorator():
    '''_complete_trace_decorator

    trace decorator explicitly for *Access complete(s) methods.
    '''
    def function(method):
        def tracer(*args, **kwargs):
            start = _get_trace_local() and time.time() or 0
            try:
                return method(*args, **kwargs)
            finally:
                try:
                    _trace_data(
                        start,
                        args[0].__class__.__name__,
                        method.__name__,
                        [])
                except:
                    raise

        return tracer
    return function
        
_trace_limits = ACCESS_TRACE_LIMITS
def trace_limit(obj = None, cmd = None, vids = None):
    '''trace_limit
    
    Limit access tracing to calls only with the supplied signature. None
    values (the default) implies ignore that component; i.e., match a call
    with any value for that component.
    
    obj: Only trace calls for this access object.
    cmd: Only trace calls against this command. Note that the cmd value needs
        to make sense with the obj value. If the cmd limit isn't a command 
        associated with the obj then it effectively will not trace anything.
    vids: Only trace for this vid(s). Can be a list. Note since calls can
         be made with lists of vids this limit will match if ANY of the vids
        in the call match those in the limit.
    '''
    _trace_limits['obj']  = obj
    _trace_limits['cmd']  = cmd
    _trace_limits['vids'] = vids

def trace_limits_clear():
    global _trace_limits
    _trace_limits = {'obj' : None, 'cmd' : None, 'vids' : None }

def has_trace_limits():
    '''returns a string flagging which trace limits have been enabled. the
    string will have the following:
    
    o: if obj limits enabled
    c: if cmd limits enabled
    v: if vids limits enabled
    
    so the string 'oc' would means obj and cmd limits, while 'ov' would mean
    obj and vids limits. 
    
    None means no limits enabled.
    '''
    r = ''.join([str(x[0])[0] for x in _trace_limits.items() if x[1] is not None]) or None
    return r
        
def _trace_check_limits(obj, cmd, vids):
    
    # object check
    #
    obj_limit = _trace_limits.get('obj')
    if obj_limit is not None and obj_limit != obj:
        return False

    # cmd check
    #
    cmd_limit = _trace_limits.get('cmd')
    if cmd_limit is not None and cmd_limit != cmd:
        return False
        
    # vids check
    #
    vids_l = _trace_limits.get('vids')
    vids_l = (isinstance(vids_l, (list, tuple)) and [vids_l] or [[vids_l]])[0]
    vids_l = set(filter(lambda x: x is not None, vids_l))
    vids   = (isinstance(vids, (list, tuple)) and [vids] or [[vids]])[0]
    vids   = set(filter(lambda x: x is not None, vids))
    if vids_l and not vids_l.intersection(vids):
        return False

    return True

#
# exception propagation
#
def _unwrap(result):
    '''_unwrap

    Given a result fetched over RPC from service.base, check for an
    envelope, and if present processes the result. When the result
    is in an envelope; on success the result is removed and returned
    to the called, on failure an appropriate exception is raised.
    '''
    if not isinstance(result, dict):
        return result

    if not result.get('envl', False):
        return result

    if not result.get('rc', 0):
        return result.get('result', result)

    raise error.lookup(result.get('rc'), *result.get('args', ()))


@_execute_trace_decorator()
def execute(
    object, notifier, command, vids,
    args = (), kwargs = {}, timeout = None, retry = None, raw = False):
    #
    # emtpy sets fail to no RPC. still, match the type the caller expects.
    #
    if not vids and isinstance(vids, list):
        return []
    #
    # default retry when none specificed
    #
    if retry is None:
        retry = DEFAULT_RETRY
    #
    # lookaside determination/setup. Three possible values:
    #
    #   None  - No lookup currently in progress
    #   False - Lookup in progress but first order
    #   True  - Second order lookup in progress, do not
    #           initiate a recursive lookup.
    #
    tlb = 'tlb-%s' % object
    val = coro.get_local(tlb)

    if val is None:
        val = False
    elif not val:
        val = True
    else:
        raise LookAside((tlb, val))

    look = (isinstance(vids, (list, tuple)) and [vids] or [[vids]])[0]
    data = {
        'tlb':     (tlb, val),
        'slave':   ('slave-read', slave_read()),
        'source':  socket.gethostname(), 
        'command': command,
        'args':    args,
        'kwargs':  kwargs}
    retry += 1

    results = map(lambda i: None, xrange(len(look)))
    pending = {}
    #
    # quick pass on empty lookup set.
    #
    if not look:
        return results
    #
    # create a dictionary of pending vector elements to result
    # array offsets, de-duplicate the lookup vector, order is
    # no longer imporatant since order is now maintained in the
    # pending dictionary
    #
    filter(
        lambda i: pending.setdefault(i[0], set([])).add(i[1]),
        zip(look, xrange(len(look))))
    #
    # process blank requests
    #
    for pos in pending.pop(BLANK_TOKEN, []):
        results[pos] = [{}]
    #
    # process remaining retry number of times.
    #
    while retry and pending:
        retry -= 1
        lookup = pending.keys()
        values = notifier.rpcs(
            object,    lookup,    'execute', data,
            timeout = timeout,
            local   = not val)
        if not values:
            continue
        #
        # map values to correct result location
        #
        for i in xrange(len(lookup)):
            if not values[i]:
                push = pending.get(lookup[i]) 
            else:
                push = pending.pop(lookup[i])
            for pos in push:
                results[pos] = values[i]

    #
    # look for absence of any results, raize an error even if only
    # one element is in error
    #
    errors = filter(lambda i: not i, results)
    if errors:
        #
        # since we can only raise one type of error, decide based on
        # the type of the first list element in the error set.
        # (empty list is a server error, none is a client error
        #
        if errors[0] is None:
            raise ClientError(look, data, errors)
        else:
            raise ServerError(look, data, errors)
    #
    # if we're not looking for raw data (function called with raw=True) then
    # strip out all but the first result and _unwrap it. otherwise just
    # _unwrap all the results and return them.
    #
    if not raw:
        results = map(lambda i: _unwrap((i and [i[0]] or [None])[0]), results)
    else:
        results = map(lambda i: map(_unwrap, i), results)
    #
    # when request is for a single ID and not a list of IDs rip the
    # result to match the request.
    #
    if not isinstance(vids, list):
        results = results[0]

    return results


def random(
        object, notifier, command, args=(), kwargs={},
        timeout=None, retry=None, raw=False):
    return execute(
        object, notifier, command,
        _random.randint(0, (8 << 60) - 1),
        args, kwargs, timeout, retry, raw)


def _flatten(data):
    if isinstance(data, (int, long)):
        return (data,)

    if not data:
        return ()

    result = reduce(lambda x,y: _flatten(x) + _flatten(y), data)
    if isinstance(result, (tuple, list)):
        return tuple(result)
    else:
        return (result,)

def _fatten(request, result):
    if isinstance(request, (int, long)):
        return result[request]
    else:
        return map(lambda i: _fatten(i, result), request)
    

class SplitAccess(object):
    '''SplitAccess

    Provide a notifier/publisher wrapper which splits access calls into
    two component parts; 1) an initiation component which will send the
    resquest and return tracking sequence number(s) for the request(s),
    2) a completion component which will wait for and return responses
    for the request sequence numbers which are provided.

    Examples:

       split = SplitAccess(notifier)

      s1 = access.test.ping(split, 0, data1)
      s2 = access.test.ping(split, 0, data2)
      [r1, r2] = split.complete([s1, s2])

      s1 = access.test.ping(split, 0, data1)
      s2 = access.test.ping(split, 0, data2)
      r1, r2 = split.completes(s1, s2)

      s1 = access.test.ping(split, 0, data1)
      s2 = access.test.ping(split, 0, data2)
      [s3, s4] = access.test.ping(split, [0, 1], data)
      r1, r2, [r3, r4] = split.completes(s1, s2, [s3, s4])
    '''
    def __init__(self, notifier, **kwargs):
        self._notifier = notifier
        self._retry    = kwargs.get('retry', DEFAULT_RETRY)

    def __repr__(self):
        return repr(self._notifier)
    #
    # straight wrap for publish
    #
    def publish(self, object, id, cmd, args):
        return self._notifier.publish(object, id, cmd, args)
    #
    # RPC start
    #
    def rpcs(self, obj, vid_list, cmd, args, *more, **kwargs):
        # legacy parameter check
        if more: kwargs['timeout'] = more[0]

        seq, cnt = self._notifier.rpcs_push(obj, vid_list, cmd, args, **kwargs)
        return map(lambda i: [i], range(seq, seq + cnt))
        
    def rpc(self, obj, vid, cmd, args, *more, **kwargs):
        result = self.rpcs(obj, [vid], cmd, args, *more, **kwargs)
        if result:
            return result[0]
        else:
            return [0]
    #
    # RPC reap
    #
    @_complete_trace_decorator()
    def complete(self, seq_list, **kwargs):
        '''complete

        Given a list of one or more sequence numbers, produced by
        access requests for RPC(s), wait for the request completion
        and return the result(s) as a list in the same order as the
        requested sequence numbers.

        Examples:

          [r1, r2, r3, r4] = self.complete([s1, s2, s3, s4])

        Optional arguments are passed to the underlying publisher RPC
        completion function:

        timeout - No single socket recieve should take longer then
                  timeout seconds. (float or int are valid as well
                  as None which denotes infinite/no timeout)

        See Also: rpc(), rpcs()
        '''

        if not seq_list:
            return None

        retry = self._retry + 1

        results = map(lambda i: None, xrange(len(seq_list)))
        pending = dict(zip(seq_list, xrange(len(seq_list))))
        errored = {}

        if len(results) != len(pending):
            raise ValueError(
                'length mismatch. duplicates? <%d:%d>',
                len(results),
                len(pending))

        while retry and pending:
            retry -= 1
            
            result = self._notifier.rpcs_recv(pending.keys(), **kwargs)
            for seq, req, rep in result:
                pos = pending.pop(seq)
                tmp = errored.pop(pos, None)

                results[pos] = rep

                if rep or not req:
                    continue

                seq = self.rpc(*req, **kwargs).pop()
                if not seq:
                    continue

                pending[seq] = pos
                errored[pos] = (req, rep)
        #
        # If no errors are present, and by extension nothing is left
        # pending, then return the results to the user.
        #
        if not errored:
            return map(lambda i: _unwrap(i[0]), results)
        #
        # All remaining cases are errors which will be reported as a
        # dictionary of sequence IDs mapped to the request tuple.
        #
        errored = dict(map(
            lambda i: (seq_list[i[0]], i[1]),
            errored.iteritems()))
        #
        # Determine if any sequence ID(s) provided are reported to
        # have a local error. (either network error, or invalid
        # sequence number)
        #
        if filter(lambda i: i[1] is None, errored.itervalues()):
            raise SplitClientError(seq_list, results, errored)
        #
        # If any sequence has an empty response, meaning no service
        # was available to response, then raise an error for the
        # entire request. (default behavior, if other behaviour is
        # desired add a non-default mode to control it)
        #
        raise SplitServerError(seq_list, results, errored)

    # not decorated to prevent trace from doubled counting
    def completes(self, *args, **kwargs):
        '''completes

        Given sequence number(s), produced by access requests for
        RPC(s), wait for the request completion and return the result
        in the same order as the arguments were presented. The sequence
        number arguments can be presented as either individual arguments
        and/or lists/tuples of sequence numbers.

        Examples:

          
          r1 = self.completes(s1)
          r1, r2, = self.completes(s1, s2)
          r1, r2, (r3, r4) = self.completes(s1, s2, (s3, s4))

        Optional arguments are passed to the underlying publisher RPC
        completion function:

        timeout - No single socket recieve should take longer then
                  timeout seconds. (float or int are valid as well
                  as None which denotes infinite/no timeout)

        See Also: rpc(), rpcs()
        '''
        if not args:
            return None

        flat = _flatten(args)

        result = _fatten(args, dict(zip(flat, self.complete(flat, **kwargs))))
        if len(args) == 1:
            return result.pop()
        else:
            return result

    @_complete_trace_decorator()
    def any(self, seq_set):
        '''any

        EXPERIMENTAL

        Given a set of sequence numbers, produced by access requests for
        RPC(s), return a tuple of any one sequence number and the matching
        response. Also remove the sequence number from the given set.

        Note: No retries, minimal testing.
        '''
        if not seq_set:
            raise ValueError('empty sequence set', seq_set)

        seq, req, rep = self._notifier.rpcs_pop(seq_set)
        if rep is None:
            raise ClientError(seq, req, rep)

        if not rep:
            raise ServerError(seq, req, rep)

        seq_set.remove(seq)
        return (seq, _unwrap(rep.pop()))

    def clear(self, seqs):
        '''clear

        Clear any request/response state associated with a set or list
        of sequence numbers
        '''
        return self._notifier.rpcs_clear(seqs)
