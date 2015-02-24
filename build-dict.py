#!/bin/python
# -*- coding: utf-8 -*-
__author__ = 'Ivan Styazhkin <nesusvet@yandex-team.ru>'

from itertools import izip_longest


def build_dict(keys, values, default_value=None):
    """
    Есть два списка разной длины. В первом содержатся ключи, а во втором значения.
    Напишите функцию, которая создаёт из этих ключей и значений словарь.
    Если ключу не хватило значения, в словаре должно быть значение None.
    Значения, которым не хватило ключей, нужно игнорировать.
    :param keys: Список ключей
    :param values: Список значений
    :param default_value: Значение по умолчанию
    :return: Результирующий словарь

    >>> d = build_dict(['a', 'b', 'c', 'd', 'e'], [1, 2, 3, 4])
    >>> sorted(d.items())  # отсортирую по имени ключа для наглядности
    [('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', None)]
    """
    return dict(
        izip_longest(
            keys,
            values[:len(keys)],  # Отбрасываем лишние значения
            fillvalue=default_value,
        )
    )
