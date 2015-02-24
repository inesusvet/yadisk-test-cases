#!/bin/python
# -*- coding: utf-8 -*-
__author__ = 'Ivan Styazhkin <nesusvet@yandex-team.ru>'

import sys
from itertools import islice
from collections import Counter

USAGE = """Web server log-file analyser. Counts most frequent ip-addresses

Usage: python log-top.py <log-file> [<count=20>]
"""
SEPARATOR = ' '
HEADER = 'Address\t\tHit count\n'
LIMIT = 20


def get_filename():
    if len(sys.argv) < 2:
        return

    return sys.argv[1]


def get_ip_from_line(line):
    if SEPARATOR in line:
        ip, _ = line.split(SEPARATOR, 1)
        return ip


def iter_addresses(input_file):
    for line in input_file:
        yield get_ip_from_line(line)    


def main(input_file):
    """
    Предположим, у вас есть access.log веб-сервера.
    Как с помощью стандартных консольных средств найти десять IP-адресов,
    от которых было больше всего запросов?
    Как то же самое сделать с помощью скрипта на Python?
    :param input_file: Исходный файл лога
    :param output_file: Файл для вывода результата
    """
    iterator = iter_addresses(input_file)
    counter = Counter(iterator)

    return counter


if __name__ == '__main__':

    filename = get_filename()
    if not filename:
        print USAGE
        sys.exit(0)

    if len(sys.argv) == 3:
        limit = int(sys.argv[2])
    else:
        limit = LIMIT

    with open(filename) as input_file:
        sys.stdout.write(HEADER)

        counter = main(input_file)
        results = map(
            lambda args: '\t'.join(map(str, args)),
            counter.most_common(100),
        )
        sys.stdout.write('\n'.join(results))
        sys.stdout.write('\n')
