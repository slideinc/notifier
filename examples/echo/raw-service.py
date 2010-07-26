#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import logging
import signal

from gogreen import coro, backdoor
from notifier import coroutines


ECHO = "echo"
NOTIFIERS = [('localhost', 7000)]


class Handler(object):
    def rpc_call(self, object, id, command, args, seq, notifier):
        string = args['args'][0]
        notifier.rpc_response(seq, "echo: " + string)


def main():
    notif = coroutines.Notifier(args=(NOTIFIERS,), kwargs={'port': 7000})
    notif.start()

    handler = Handler()

    notif.rpc_slice(ECHO, (0, 0), 'execute', handler)

    notif.set_log_level(logging.INFO)

    signal.signal(signal.SIGINT, lambda signum, frame: notif.shutdown())

    coro.event_loop()


if __name__ == '__main__':
    main()
