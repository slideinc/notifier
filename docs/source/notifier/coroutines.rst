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
            The service name for which we are subscribing to messages
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
            The service name for which we are subscribing to messages
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
            The service name from which we are unsubscribing
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
