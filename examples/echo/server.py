#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import logging
import signal

from gogreen import backdoor, coro
from notifier import coroutines, decorators, service


ECHO = "echo"
NOTIFIERS = [('localhost', 7000)]


class EchoWorker(service.Worker):
    @decorators.command
    def echo(self, id, string):
        return "echo: " + str(string)


class EchoServer(service.Server):
    worker_class = EchoWorker
    subscription = ECHO


def main():
    ns = coroutines.Notifier(args=(NOTIFIERS,), kwargs={'port': 7000})
    ns.start()

    server = EchoServer(
            100,
            notifier=ns,
            bounds={'mask': 0, 'value': 0},
            loglevel=logging.INFO)

    def shutdown(signum, frame):
        coro.spawn(lambda: server.drain() and ns.shutdown())
    signal.signal(signal.SIGINT, shutdown)

    coro.event_loop()


if __name__ == '__main__':
    main()
