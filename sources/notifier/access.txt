================================================
:mod:`notifier.access` -- Simplified RPC Sending
================================================

.. module:: notifier.access
.. moduleauthor:: Libor Michalek <libor@pobox.com>

.. function:: execute(object, notifier, cmd, ids, args=(), kwargs={}, timeout=None, retry=None)

    Sends an RPC request via ``notifier`` using its synchronous API, wrapping up
    args and kwargs in a manner that is understood and by the system in
    :mod:`notifier.service`.

    :param object: the service name
    :type object: str
    :param notifier:
        a notifier at least capable of sending rpcs -- it should be a
        :class:`Notifier <notifier.coroutines.Notifier>` or a
        :class:`Publisher <notifier.publish.Publisher>`, or it can also be a
        :class:`SplitAccess` wrapping either real notifier.
    :param cmd: the remote command to execute
    :type cmd: str
    :param ids: the id(s) for finding the RPC destination(s)
    :type ids: int or list of ints
    :param args:
        arguments to send with the RPC -- :mod:`service <notifier.service>`
        will use this in the handler function as **\*args**.
    :type args: tuple
    :param kwargs:
        keyword arguments to send with the RPC --
        :mod:`service <notifier.service>` will use this as **\*\*kwargs**.
    :type kwargs: dict
    :param timeout:
        a maximum time to wait for the response (the default of None means
        timeout)
    :type timeout: int
    :param retry:
        number of times to retry in the case of notifier-related errors
    :type retry: int

    :returns:
        the RPC call result, unless ``notifier`` is a :class:`SplitAccess`, in
        which case it returns an integer sequence_id.

.. function:: random(object, notifier, cmd, args=(), kwargs={}, timeout=None, retry=None)

    Sends an :func:`execute` call using a random integer for ``ids``.

    :param object: the service name
    :type object: str
    :param notifier:
        a notifier at least capable of sending rpcs -- it should be a
        :class:`Notifier <notifier.coroutines.Notifier>` or a
        :class:`Publisher <notifier.publish.Publisher>`, or it can also be a
        :class:`SplitAccess` wrapping either real notifier.
    :param cmd: the remote command to execute
    :type cmd: str
    :param args:
        arguments to send with the RPC -- :mod:`service <notifier.service>`
        will use this in the handler function as **\*args**.
    :type args: tuple
    :param kwargs:
        keyword arguments to send with the RPC --
        :mod:`service <notifier.service>` will use this as **\*\*kwargs**.
    :type kwargs: dict
    :param timeout:
        a maximum time to wait for the response (the default of None means
        timeout)
    :type timeout: int
    :param retry:
        number of times to retry in the case of notifier-related errors
    :type retry: int

    :returns:
        the RPC call result, unless ``notifier`` is a :class:`SplitAccess`, in
        which case it returns an integer sequence_id.

.. class:: SplitAccess(notifier)

    The :class:`SplitAccess` constructor takes a single argument of a notifier
    (be it a :class:`Notifier <notifier.coroutines.Notifier>` or a
    :class:`Publisher <notifier.publish.Publisher>`), and then the instance can
    itself be used in :func:`execute` or :func:`random` as the ``notifier``.
    
    When a :class:`SplitAccess` is provided as the notifier to those functions,
    then instead of returning the RPC call result, they will return an integer
    known as a sequence id. These sequence ids can be passed back in to the
    :meth:`complete` and :meth:`any` methods to resolve them to RPC results.

    .. method:: complete(seq_list)

        Block waiting until all the RPCs corresponding to the sequence ids in
        ``seq_list`` have returned, then return those results in a list
        matching the order of ``seq_list``.

        :param seq_list:
            a list of integer sequence ids that were returned from
            :func:`execute` or :func:`random` calls made with this
            :class:`SplitAccess` instance.
        :type seq_list: list of ints

        :returns: the RPC result

    .. method:: any(seq_list)

        Block waiting until at least one of the RPCs corresponding to the
        sequence ids in ``seq_list`` has returned, then return a tuple of the
        sequence id and the RPC call result.

        :param seq_list:
            a list of integer sequence ids that were returned from
            :func:`execute` or :func:`random` calls made with this
            :class:`SplitAccess` instance.
        :type seq_list: list of ints

        :returns: a (seq, rpc_result) tuple
