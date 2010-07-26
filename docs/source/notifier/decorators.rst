================================================
:mod:`notifier.decorators` -- Service Decorators
================================================

.. module:: notifier.decorators
.. moduleauthor:: Libor Michalek <libor@pobox.com>

.. function:: command(func)

    Decorator to apply to a :class:`notifier.service.Worker` subclass method to
    specify that it is an RPC handling method.
