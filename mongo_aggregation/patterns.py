#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from copy import copy

import six


def dollar_prefix(field):
    return field if field.startswith('$') else '${}'.format(field)


def _list_dollar_prefix(*args):
    args = list(args)
    for i, arg in enumerate(args):
        if not isinstance(arg, str):
            continue
        args[i] = dollar_prefix(arg)
    return args


def pop_dollar_prefix(field):
    return field[1:] if field.startswith('$') else field


def regex(pattern, options='', i=False, m=False, x=False, s=False, extra_fields=None):
    """Provides regular expression capabilities for pattern matching strings in queries."""
    expression = {'$regex': pattern}
    if not options:
        options = f'{"i" if i else ""}{"m" if m else ""}{"x" if x else ""}{"s" if s else ""}'
    if options:
        expression['$options'] = options
    if extra_fields:
        expression.update(extra_fields)
    return expression


def _and(*args):
    """Deprecated. Use and_ instead."""
    return and_(*args)


def and_(*args, **kwargs):
    """Performs a logical AND operation on an array of one or more expressions
    (e.g. <expression1>, <expression2>, etc.) and selects the documents that
    satisfy all the expressions in the array."""
    # Складываем все в args
    args = list(args)
    if kwargs:
        kwargs = _convert_names_with_underlines_to_dots(kwargs, convert_operators=True)
        kwargs = [{key: value} for key, value in kwargs.items()]
        args.extend(kwargs)
    return {'$and': args}


def or_(*args, **kwargs):
    """Performs a logical OR operation on an array of two or more expressions
    and selects the documents that satisfy at least one of the expressions."""
    # Складываем все в args
    args = list(args)
    if kwargs:
        kwargs = _convert_names_with_underlines_to_dots(kwargs, convert_operators=True)
        kwargs = [{key: value} for key, value in kwargs.items()]
        args.extend(kwargs)
    return {'$or': args}


def eq(first_expression, second_expression=True):
    return {'$eq': [first_expression, second_expression]}


def gt(first_expression, second_expression=None):
    if second_expression is None:
        return {'$gt': first_expression}
    return {'$gt': [first_expression, second_expression]}


def gte(first_expression, second_expression=None):
    if second_expression is None:
        return {'$gte': first_expression}
    return {'$gte': [first_expression, second_expression]}


def ne(first_expression, second_expression=True):
    if second_expression is True:
        return {'$not': [first_expression]}
    return {'$ne': [first_expression, second_expression]}


def field_in(field, values_list):
    field = dollar_prefix(field)
    return {'$or': [
        eq(field, value)
        for value in values_list
    ]}


def optimized_in(value, negative=False, operator_is_required=False):
    """
    Реализует оптимальный фильтр оператора $in. Если значение в списке единственное, то используется оператор $eq.
    Также возможно использование с отрицанием: $nin и $ne.
    Для оператора $eq по умолчанию возвращается только значение, потому что этого достаточно для обычного фильтра.
    Для возврата значения с оператором используйте operator_is_required в значении True.
    :param value: Список значений фильтра
    :param negative: Флаг, использовать ли $nin и $ne вместо $in и $eq
    :param operator_is_required: Флаг, обязательно ли возвращать оператор со значением
    :return: Значение или словарь {оператор: значение}
    >>> optimized_in(['1'])
    '1'
    >>> optimized_in(['1'], operator_is_required=True)
    {'$eq': '1'}
    >>> optimized_in(['1'], negative=True)
    {'$ne': '1'}
    >>> optimized_in(['1', '2'])
    {'$in': ['1', '2']}
    >>> optimized_in(['1', '2'], negative=True)
    {'$nin': ['1', '2']}
    """
    if len(value) == 1:
        value = value[0]
        operator = '$ne' if negative else '$eq' if operator_is_required else ''
    else:
        operator = '$nin' if negative else '$in'

    if operator:
        return {operator: value}
    return value


def field_exists(field):
    """
    >>> field_exists('field')
    {'$or': [{'$eq': ['$field', None]}, {'$gt': ['$field', None]}]}
    >>> field_exists('$field')
    {'$or': [{'$eq': ['$field', None]}, {'$gt': ['$field', None]}]}
    """
    field = dollar_prefix(field)
    return {'$or': [
        {'$eq': [field, None]},
        {'$gt': [field, None]},
    ]}


def _convert_names_with_underlines_to_dots(args, convert_operators=False):
    """Проверяем обращение к полям объекта с помощью __ в словаре"""
    for i, arg in enumerate(copy(args)):
        if '__' not in arg:
            continue
        elif arg.startswith('__') or arg.endswith('__'):
            continue
        replacement = arg.replace('__', '.')
        if isinstance(args, list):
            args.pop(i)
            args.insert(i, replacement)
        else:
            if convert_operators:
                potential_operator = replacement[replacement.rfind('.')+1:]
                if potential_operator in ['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'nin', 'push']:
                    args[replacement[:replacement.rfind('.')]] = {dollar_prefix(potential_operator): args.pop(arg)}
                    continue
                elif potential_operator in ['regex', 'iregex', 'icontains', 'contains']:
                    ignore_case = potential_operator.startswith('i')
                    args[replacement[:replacement.rfind('.')]] = regex(args.pop(arg), i=ignore_case)
                    continue
            args[replacement] = args.pop(arg)
    return args


def obj(*args, **kwargs):
    """
    Returns dictionary with specified fields as keys and values. See usage examples in tests below.

    >>> obj('name,description')
    {'name': '$name', 'description': '$description'}
    >>> obj('name', 'description')
    {'name': '$name', 'description': '$description'}
    >>> obj('name,description', id='$code')
    {'name': '$name', 'description': '$description', 'id': '$code'}
    """
    fields = [f for arg in args for f in arg.split(',') if isinstance(arg, six.string_types)]
    for arg in args:
        if isinstance(arg, dict):
            kwargs.update(arg)
    kwargs = _convert_names_with_underlines_to_dots(kwargs, convert_operators=True)
    result = {pop_dollar_prefix(field): dollar_prefix(field) for field in fields}
    result.update(kwargs)
    return result


if __name__ == '__main__':

    def _test_module():
        import doctest
        result = doctest.testmod()
        if not result.failed:
            print(f"{result.attempted} passed and {result.failed} failed.\nTest passed.")

    _test_module()
