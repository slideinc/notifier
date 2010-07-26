======================================================
:mod:`notifier.service` -- Simplified Service Creation
======================================================

.. module:: notifier.service
.. moduleauthor:: Libor Michalek <libor@pobox.com>

The tools in this module allow a programmer to spin up a service that can
respond to RPC calls very easily.

First a :class:`Worker` subclass needs to be defined, which has methods
decorated with :func:`command <notifier.decorators.command>`. This class (the
class itself) is assigned to the ``worker_class`` attribute of a
:class:`Server` subclass, which also needs a ``subscription`` class attribute,
a string of the service name it is offering. The :class:`Server` subclass gets
instantiated with a count of worker instances it should create and control, a
:class:`Notifier <notifier.coroutines.Notifier>` it uses to register for and
receive RPC calls, and a bounds dictionary, which contains ``mask`` and
``value`` keys with integer values (more on those in a minute).

When an RPC request is made, it is always made with a ``object``, an ``id``,
and a ``cmd``. For the request to be routed to a particular :class:`Server`,
that server's ``subscription`` must match the ``object``, it must be true
that ``(<the server's "mask"> & id) == <the server's "value">``, and finally
that the ``cmd`` in the rpc call must be ``"execute"``.

If the rpc call was made with :func:`notifier.access.execute` or
:func:`notifier.access.random`, the ``cmd`` argument will be placed in the
``command`` key in the rpc request's arguments, which will be a dictionary.
The server's :class:`Worker` subclass must have that value as the name of a
:func:`command <notifier.decorators.command>`-decorated method, and that method
will then be used to handle the RPC and generate the response.

The ``bounds`` given to :class:`Server`.__init__ control which ids get through
to a server by masking with ``bounds['mask']`` and comparing the result to
``bounds['value']``. For a server that is the only provider of a service and
command, use ``{'mask': 0, 'value': 0}``.

RPC requests are routed through the notifier network according to the
``object``, ``id``, 

.. class:: Worker

    A coroutine thread that handles RPC requests.

    The primary use of this class is simply to subclass it adding handler
    methods, and assign that subclass to a :class:`Server` subclass which will
    actually create the worker instances and call their methods.

    In a Worker subclass, any methods wrapped with
    :func:`notifier.decorators.command` will be used to handle RPC requests.

.. class:: Server(worker_count, notifier, bounds=None, loglevel=DEBUG)

    Server, like :class:`Worker`, is itself primarily useful as a base class.
    Subclasses of Server should get a few class-level attributes, and then
    be instantiated to start running in a gogreen coroutine environment.

    :param worker_count:
        The number of :attr:`worker_class` instances to run, each pulling from
        the work queue.
    :type worker_count: int
    :param notifier:
        A :class:`Notifier <notifier.coroutines.Notifier>` (a half-notifier
        :class:`Publisher <notifier.publish.Publisher>` is not enough)

    .. attribute:: worker_class

        The :class:`Worker` subclass that will be used by instances of this
        Server subclass.

    .. attribute:: subscription

        The service name (string) that this Server subclass is providing.
