====================================================
:mod:`notifier.coroutines` -- The Coroutine Notifier
====================================================

.. module:: notifier.coroutines
.. moduleauthor:: Libor Michalek <libor@pobox.com>

.. class:: Notifier(notifier_map, args=())

    Notifier is the only "full" notifier, meaning it can both send remote
    procedure calls and one-way notifications and announce itself as a target
    of them.

    At creation time the Notifier needs to know about the ``(host, port)`` of
    all other notifiers with which it will communicate, so ``notifier_map`` is
    a list of those 2-tuples. ``args`` will be passed on to the :meth:`run`
    method when the Notifier's coroutine thread is started.

    .. method:: run(smap, port=0, host='')

        Being a gogreen.coro.Thread subclass, the ``Notifier.run`` arguments
        must be filled ahead of time by ``args`` and ``kwargs`` keyword
        arguments to ``Notifier.__init__``.

        :param smap:
            a list of ``(host, port)`` pairs with the locations of all the
            other Notifiers on the network.
        :type smap: list
        :param port: the port the Notifier will bind to
        :type port: int
        :param host: the host the Notifier will bind to
        :type host: str

    **One-Way Notification Registration Management**

    .. method:: subscribe(object, id, cmd, destination)

        Register for receiving one-way notifications for this set of
        identifiers.

        :param object:
            the service name for which we are subscribing to messages
        :type object: str
        :param id:
            a ``(mask, value)`` pair to match ids (an id matches if
            ``id & mask == value``)
        :type id: tuple
        :param cmd: the command name to respond to
        :type cmd: str
        :param destination:
            an object with a method ``notify(object, id, cmd, args)`` to which
            incoming notifications will be delegated.

    .. method:: slice(object, id, cmd, destination, weight=1.0)

        Register for receiving one-way notifications for this set of
        identifiers, adjusting the weight used for this notifier when the
        notification is sent to a random recipient.

        :param object:
            the service name for which we are subscribing to messages
        :type object: str
        :param id:
            a ``(mask, value)`` pair to match ids (an id matches if
            ``id & mask == value``)
        :type id: tuple
        :param cmd: the command name to respond to
        :type cmd: str
        :param destination:
            an object with a method ``notify(object, id, cmd, args)`` to which
            incoming notifications will be delegated.
        :param weight:
            the weight relative to the other notifiers registered, to turn up
            or down the traffic sent to a node when there are overlapping
            subscriptions
        :type weight: float

    .. method:: unsubscribe(object, id, cmd, destination)

        Unregister from receiving notifications to object/id

        :param object:
            the service name to unsubscribe from
        :type object: str
        :param id: the ``(mask, value)`` mask to unsubscribe from
        :type id: tuple
        :param cmd: the command name to unsubscribe from
        :type cmd: str
        :param destination:
            an object that had been a ``destination`` in a previous
            :meth:`subscribe` or :meth:`slice` call.

    .. method:: unsubscribe_all(destination)

        Unsubscribe from receiving all notifications for which the notifier is
        currently subscribed and delgating to ``destination``.

        :param destination:
            an object which had previously been the ``destination`` in one or
            more :meth:`subscribe` or :meth:`slice` calls

    **Notification Sending**

    .. method:: publish(object, id, cmd, args)

        Send a one-way notification to whoever is registered for receiving them
        with ``(object, id, cmd)``, and send ``args`` along with it.

        :param object: the service name
        :type object: str
        :param id: the id to match to find a recipient
        :type id: int
        :param cmd: the command to match with and send
        :type cmd: str
        :param args:
            the wirebin-serializable arguments to send along with the
            notification

    **RPC Registration Management**

    .. method:: rpc_register(object, id, cmd, destination)

        Register for receiving RPC requests for this set of identifiers.

        :param object: the service name for which we are subscribing
        :type object: str
        :param id:
            a ``(mask, value)`` pair to match ids (an id matches if
            ``id & mask == value``)
        :type id: tuple
        :param cmd: the command name to respond to
        :type cmd: str
        :param destination:
            an object with a method
            ``rpc_call(object, id, command, args, seq, notifier)`` to which
            incoming rpc calls will be delegated (and the return value will be
            sent back as the rpc response)

    .. method:: rpc_slice(object, id, cmd, destination, weight=1.0)

        Register for receiving RPC requests for this set of identifiers, and
        adjust the weight for this notifier when a random destination is
        selected.

        :param object: the service name for which we are subscribing
        :type object: str
        :param id:
            a ``(mask, value)`` pair to match ids (an id matches if
            ``id & mask == value``)
        :type id: tuple
        :param cmd: the command name to respond to
        :type cmd: str
        :param destination:
            an object with a method
            ``rpc_call(object, id, command, args, seq, notifier)`` to which
            incoming rpc calls will be delegated (and the return value will be
            sent back as the rpc response)
        :param weight:
            the weight relative to the other notifiers registered, to turn up
            or down the traffic sent to a node when there are overlapping
            rpc subscriptions
        :type weight: float

    .. method:: rpc_unregister(object, id, cmd, destination)

        Unregister from receiving RPCs to this ``(object, id, cmd)``

        :param object:
            the service name to unsubscribe from
        :type object: str
        :param id: the ``(mask, value)`` mask to unsubscribe from
        :type id: tuple
        :param cmd: the command name to unsubscribe from
        :type cmd: str
        :param destination:
            an object that had been a ``destination`` in one ore more previous
            :meth:`rpc_register` or :meth:`rpc_slice` call.

    .. method:: rpc_unregister_all(destination)

        Unregister for all RPCs to which we had registered with ``destination``
        as the handler.

        :param destination:
            an object that had been a ``destination`` in one ore more previous
            :meth:`rpc_register` or :meth:`rpc_slice` call.

    **RPC Calling**

    .. method:: rpc(object, id, cmd, args, timeout=None)

        Send an RPC request to a registered receiver for ``(object, id, cmd)``,
        passing arguments ``args``. Block waiting for the response, limiting
        the wait to ``timeout`` seconds (if ``timeout`` is provided).

        :param object: the service name
        :type object: str
        :param id: the integer id used in services' ``(mask, value)`` matching
        :type id: int
        :param cmd: the command name being sent
        :type cmd: str
        :param args: arguments sent in the RPC request
        :param timeout: the maximum time to wait for the response
        :type timeout: int or float

        :returns: the result returned in the RPC response

    .. method:: rpcs(object, id_list, cmd, args, timeout=None)

        Sends one RPC request per id in ``id_list``, equivalent to
        ``len(id_list)`` :meth:`rpc` calls except that when it blocks it waits on
        them all in parallel.

        :param object: the service name
        :type object: str
        :param id_list: the ``id`` list
        :type id_list: list of ints
        :param cmd: the command in the rpc requests
        :type cmd: str
        :param args: arguments send in the RPC requests
        :param timeout: the maximum time to wait for the response
        :type timeout: int or float

        :returns:
                the results returned in the RPC responses (in a list ordered
                the same as ``id_list``)

    .. method:: rpc_call(object, id, cmd, args, source)

        Asynchronously send an RPC request, providing an object with a callback
        method for when the response comes in.

        :param object: the service name
        :type object: str
        :param id: the identifier matched to find a specific handler
        :type id: int
        :param cmd: the command name to send
        :type cmd: str
        :param args: arguments sent in the RPC request
        :param source:
            an object with a
            ``rpc_response(object, id, cmd, results)`` method, which will be
            called when the response comes back. ``object``, ``id``, and
            ``cmd`` will all be the same as the original :meth:`rpc_call`, and
            ``results`` will be the value in the RPC response.
