====================================================
:mod:`notifier.coroutines` -- The Coroutine Notifier
====================================================

.. module:: notifier.coroutines
.. moduleauthor:: Libor Michalek <libor@pobox.com>

.. class:: Notifier

    Notifier is the only "full" notifier, meaning it can both send remote
    procedure calls and one-way notifications and announce itself as a target
    of them.

    .. method:: run(smap, port=0, host='')

        Being a gogreen.coro.Thread subclass, the *Notifier.run* arguments
        must be filled ahead of time by *args* and *kwargs* keyword arguments
        to *Notifier.__init__*.

        :param smap:
            a list of *(host, port)* pairs with the locations of all the other
            Notifiers on the network.
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
        :param id: a *(mask, value)* pair to match ids
        :type id: tuple
        :param cmd: the method of the destination to call
        :type cmd: str
        :param destination:
            the object from which we grab the *cmd* method to handle the
            request.

    .. method:: slice(object, id, cmd, destination, weight=1.0)

        Register for receiving one-way notifications for this set of
        identifiers, adjusting the weight used for this notifier when the
        notification is sent to a random recipient.

        :param object:
            the service name for which we are subscribing to messages
        :type object: str
        :param id: a *(mask, value)* pair to match ids
        :type id: tuple
        :param cmd: the method of the destination to call
        :type cmd: str
        :param destination:
            the object from which we grab the *cmd* method to handle the
            request.
        :param weight:
            the weight relative to the other nodes registered for
            this event for random sends.
        :type weight: float

    .. method:: unsubscribe(object, id, cmd, destination)

        Unregister from receiving notifications to object/id

        :param object:
            the service name from which we are unsubscribing
        :type object: str
        :param id: the *(mask, value)* for which we will no longer receive
        :type id: tuple
        :param cmd: the method of the destination to call
        :type cmd: str
        :param destination:
            the object from which we would have grabbed the *cmd* method to
            handle the request.

    .. method:: unregister_all(destination)

        Unregister from receiving all notifications for which the notifier is
        currently subscribed and delgating to *destination*.

        :param destination:
            the object from which we would have grabbed the *cmd* method to
            handle the request.

    **Notification Sending**

    .. method:: publish(object, id, cmd, args)

        Send a one-way notification to whoever is registered for receiving them
        with *(object, id, cmd)*, and send *args* along with it.

        :param object: the service name
        :type object: str
        :param id: the id to match to find a suitable recipient
        :type id: int
        :param cmd: the method name on the recipient to call
        :type cmd: str
        :param args:
            the wirebin-serializable arguments to send along with the
            notification
        :type args: tuple

    **RPC Registration Management**

    .. method:: rpc_register(object, id, cmd, destination)

        Register for receiving RPC requests for this set of identifiers.

        :param object: the service name for which we are subscribing
        :type object: str
        :param id: a *(mask, value)* pair to match ids
        :type id: tuple
        :param cmd: the method of the destination call
        :type cmd: str
        :param destination:
            the object from which the notifier will *getattr()* the *cmd* to
            get the function to use to handle the RPC request.

    .. method:: rpc_slice(object, id, cmd, destination, weight=1.0)

        Register for receiving RPC requests for this set of identifiers, and
        adjust the weight for this notifier when a random destination is
        selected.

        :param object: the service name for which we are subscribing
        :type object: str
        :param id: a *(mask, value)* pair to match ids
        :type id: tuple
        :param cmd: the method of the destination call
        :type cmd: str
        :param destination:
            the object from which the notifier will *getattr()* the *cmd* to
            get the function to use to handle the RPC request.
        :param weight:
            the weight (normal is 1.0) for this notifier in calls to this
            *(object, id, cmd)* on the network
        :type weight: float

    .. method:: rpc_unregister(object, id, cmd, destination)

        Unregister from receiving RPCs to this *(object, id, cmd)*

        :param object: the service from which we are unregistering
        :type object: str
        :param id: the *(mask, value)* for which we will no longer receive RPCs
        :type id: tuple
        :param cmd: the method of the destination that would have been called
        :type cmd: str
        :param destination:
            the object from which the notifier would have calld *getattr()*
            with the *cmd*

    .. method:: rpc_unregister_all(destination)

        Unregister for all RPCs to which we had registered with *destination*
        as the handler.

        :param destination:
            An object that had been used as the *destination* in
            :meth:`rpc_register` or :meth:`rpc_slice` calls

    **RPC Calling**

    .. method:: rpc(object, id, cmd, args, timeout=None)

        Send an RPC request to a registered receiver for *(object, id, cmd)*,
        passing arguments tuple *args*. Block waiting for the response,
        limiting the wait to *timeout* seconds, if *timeout* is provided.

        :param object: the service name
        :type object: str
        :param id: the identifier matched to find a specific handler
        :type id: int
        :param cmd: the method of the service we are calling
        :type cmd: str
        :param args: arguments send in the RPC request
        :type args: tuple
        :param timeout: the maximum time to wait for the response
        :type timeout: int or float

    .. method:: rpcs(object, id_list, cmd, args, timeout=None)

        Sends one RPC request per id in *id_list*, equivalent to *len(id_list)*
        *rpc()* calls except that it waits on them all in parallel.

        :param object: the service name
        :type object: str
        :param id_list: the identifiers matched to find a specific handlers
        :type id_list: list of ints
        :param cmd: the method of the service we are calling
        :type cmd: str
        :param args: arguments send in the RPC requests
        :type args: tuple
        :param timeout: the maximum time to wait for the response
        :type timeout: int or float

    .. method:: rpc_call(object, id, cmd, args)
