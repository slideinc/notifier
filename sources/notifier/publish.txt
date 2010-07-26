==========================================================
:mod:`notifier.publish` -- The Non-Coroutine Half Notifier
==========================================================

.. module:: notifier.publish
.. moduleauthor:: Libor Michalek <libor@pobox.com>

.. class:: Publisher(notifier_map)

    Publisher is a "half" notifier, meaning it can send RPCs and one-way
    notifications, but is unable to broadcast itself as a target of either. To
    provide a service in this manner, use
    :class:`notifier.coroutines.Notifier`.

    The primary reason to use a Publisher over a Notifier is that it can work
    outside of a gogreen coroutine environment, making it more suitable for an
    interactive python session, for example.

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
