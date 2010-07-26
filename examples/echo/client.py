#!/usr/bin/env python
# vim: fileencoding=utf8:et:sta:ai:sw=4:ts=4:sts=4

from notifier import access, publish


ECHO = "echo"
NOTIFIERS = [('localhost', 7000)]


def main():
    pub = publish.Publisher(NOTIFIERS)

    while 1:
        string = raw_input()

        if not string.strip():
            break

        result = access.execute(
                ECHO,
                pub,
                'echo',
                hash(string),
                args=(string,))

        print result


if __name__ == '__main__':
    main()
