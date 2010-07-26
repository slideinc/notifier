#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

import functools
import logging
import signal

import pynotify

from gogreen import coro
from notifier import coroutines


NOTIFIERS = [('localhost', 7000)]
GUI_NOTIFY = 'gui-notifications'
CMD_NAME = 'pynotify'

urgencies = {
    1: pynotify.URGENCY_LOW,
    2: pynotify.URGENCY_NORMAL,
    3: pynotify.URGENCY_CRITICAL,
}


class Handler(object):
    def notify(self, object, id, cmd, args):
        args, kwargs = args
        self.pynotification(*args, **kwargs)

    def pynotification(
            self,
            title,
            message,
            urgency=2,
            timeout=3000):
        notif = pynotify.Notification(title, message)
        notif.set_urgency(urgencies.get(urgency, pynotify.URGENCY_NORMAL))
        notif.set_timeout(timeout)
        notif.show()


def main():
    pynotify.init('notifier-pynotify')

    notifier = coroutines.Notifier(args=(NOTIFIERS,), kwargs={'port': 7000})
    notifier.start()

    handler = Handler()

    notifier.slice(GUI_NOTIFY, (0, 0), CMD_NAME, handler)
    notifier.set_log_level(logging.INFO)

    signal.signal(signal.SIGINT, lambda signum, frame: notifier.shutdown())

    coro.event_loop()


if __name__ == '__main__':
    main()
