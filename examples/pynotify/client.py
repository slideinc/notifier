#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

from notifier import publish


NOTIFIERS = [('localhost', 7000)]
GUI_NOTIFY = 'gui-notifications'
CMD_NAME = 'pynotify'


def main():
    pub = publish.Publisher(NOTIFIERS)

    while 1:
        title = raw_input('title: ')
        if not title:
            break

        msg = raw_input('message: ')
        if not msg:
            break

        urgency = raw_input('urgency (1, 2 or 3)[2]: ')
        try:
            urgency = int(urgency)
        except ValueError:
            urgency = 2

        timeout = raw_input('timeout[3]: ')
        try:
            timeout = float(timeout) * 1000
        except ValueError:
            timeout = 3000

        pub.publish(
                GUI_NOTIFY, 0, CMD_NAME, ((title, msg, urgency, timeout), {}))

        print


if __name__ == '__main__':
    main()
