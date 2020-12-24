#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from copy import deepcopy

import six

from mongo_aggregation.patterns import _convert_names_with_underlines_to_dots, dollar_prefix, _list_dollar_prefix


def field_is_specified(field):
    return {'$cond': [field, True, False]}


def project_set_value(expression):
    #TODO переделать на $literal если получится
    return {'$cond': [True, expression, '']}


def filter_(input, condition, as_field=''):
    """Selects a subset of an array to return based on the specified condition.
    Returns an array with only those elements that match the condition.
    The returned elements are in the original order.
    From mongo version 3.2."""
    filter_stage = {'input': input, 'cond': condition}
    if as_field:
        filter_stage['as'] = as_field
    return {'$filter': filter_stage}


def round_half_up(expression, signs_after_dot=0):
    abs_expression = {'$abs': expression}
    sign = {'$cond': [{'$gt': [expression, 0]}, 1, -1]}
    if signs_after_dot:
        rounded_abs = {'$divide': [
            {'$trunc': {'$add': [{'$multiply': [abs_expression, pow(10, signs_after_dot)]}, 0.5]}},
            pow(10, signs_after_dot)
        ]}
    else:
        rounded_abs = {'$trunc': {'$add': [abs_expression, 0.5]}}
    return {'$multiply': [rounded_abs, sign]}


def day_start(field):
    field = dollar_prefix(field)
    return {'$subtract': [
        field,
        {'$add': [
            {'$multiply': [{'$hour': field}, 3600000]},
            {'$multiply': [{'$minute': field}, 60000]},
            {'$multiply': [{'$second': field}, 1000]},
            {'$millisecond': field}
        ]}
    ]}


def month_start(field):
    field = dollar_prefix(field)
    return {'$subtract': [
        field,
        {'$add': [
            {'$multiply': [{'$subtract': [{'$dayOfMonth': field}, 1]}, 86400000]},
            {'$multiply': [{'$hour': field}, 3600000]},
            {'$multiply': [{'$minute': field}, 60000]},
            {'$multiply': [{'$second': field}, 1000]},
            {'$millisecond': field}
        ]}
    ]}


def lookup_unwind(collection, local_field='_id', as_field='', foreign_field='_id', preserveNullAndEmptyArrays=True):
    if not as_field:
        as_field = local_field
    pipeline = [
        lookup(collection, local_field, as_field, foreign_field),
        unwind(as_field, preserveNullAndEmptyArrays)
    ]
    return pipeline


def lookup(collection, local_field='_id', as_field='', foreign_field='_id'):
    if not as_field:
        as_field = local_field
    return {"$lookup": {
        'from': collection,
        'foreignField': foreign_field,
        'localField': local_field,
        'as': as_field,
    }}


def unwind(field, preserveNullAndEmptyArrays=False):
    field = dollar_prefix(field)
    if preserveNullAndEmptyArrays:
        return {'$unwind': {'path': field, 'preserveNullAndEmptyArrays': True}}
    else:
        return {'$unwind': field}


def date_to_string(field, date_format="%Y-%m-%d %H:%M:%S:%L"):
    field = dollar_prefix(field)
    return {'$dateToString': {'format': date_format, 'date': field}}


def set_value(expression):
    return {'$literal': expression}


def merge_objects(*args, **kwargs):
    """Combines multiple documents into a single document.
    $mergeObjects operator implementation.
    From MongoDB version 3.6."""
    # Put everything into args
    args = list(args)
    args = [dollar_prefix(arg) if isinstance(arg, six.string_types) else arg for arg in args]
    if kwargs:
        kwargs = _convert_names_with_underlines_to_dots(kwargs, convert_operators=True)
        args.append(kwargs)
    return {'$mergeObjects': args}


def hide_fields(fields_to_hide, visible_fields, concealer_field='_id'):
    if isinstance(fields_to_hide, str):
        fields_to_hide = fields_to_hide.split(',')
    if isinstance(visible_fields, str):
        visible_fields = visible_fields.split(',')

    projection = {
        concealer_field: {field: dollar_prefix(field) for field in fields_to_hide},
    }
    projection[concealer_field][concealer_field] = dollar_prefix(concealer_field)
    projection.update({field: 1 for field in visible_fields})
    return {'$project': projection}


def unhide_fields(hided_fields, visible_fields, concealer_field='_id'):
    if isinstance(hided_fields, str):
        hided_fields = hided_fields.split(',')
    if isinstance(visible_fields, str):
        visible_fields = visible_fields.split(',')

    projection = {
        field: '{}.{}'.format(dollar_prefix(concealer_field), field)
        for field in hided_fields
    }
    projection.update({field: 1 for field in visible_fields})
    if concealer_field == '_id' and concealer_field not in hided_fields:
        projection[concealer_field] = 0
    return {'$project': projection}


def switch(cases_list, last_as_final=False, final_else=''):
    if not cases_list: return final_else
    cases = deepcopy(cases_list)
    case, value = cases.pop(0)
    if not cases and last_as_final: return {'$literal': value}
    else_expr = switch(cases, last_as_final, final_else)
    return {'$cond': [case, value, else_expr]}


def switch_compare(field, cases_list, compare_method='$eq', final_else=''):
    if not cases_list: return final_else
    cases = deepcopy(cases_list)
    case, value = cases.pop(0)
    else_expr = switch_compare(field, cases, compare_method, final_else)
    return {'$cond': [{compare_method: [field, case]}, value, else_expr]}


def if_null(field, value):
    """$ifNull implementation."""
    return {'$ifNull': [dollar_prefix(field), value]}


def cond(condition, then_value, else_value=0):
    """$cond implementation."""
    if isinstance(condition, str):
        condition = dollar_prefix(condition)
    return {"$cond": [condition, then_value, else_value]}


def concat(*args):
    """$concat implementation."""
    return {'$concat': args}


def multiply(*args):
    """$multiply implementation."""
    return {'$multiply': _list_dollar_prefix(*args)}


def divide(first_expression, second_expression):
    """$divide implementation."""
    return {'$divide': _list_dollar_prefix(first_expression, second_expression)}


def sum_(*args):
    args = _list_dollar_prefix(*args)
    if len(args) == 1:
        return {'$sum': args[0]}
    return {'$sum': _list_dollar_prefix(*args)}


def in_(expression, array_expression):
    """$multiply implementation.

    >>> in_('a', 'b')
    {'$in': ['$a', '$b']}
    >>> in_('$a', 'b')
    {'$in': ['$a', '$b']}
    >>> in_('a', '$b')
    {'$in': ['$a', '$b']}
    >>> in_('a', ['b', 'c'])
    {'$in': ['$a', ['b', 'c']]}
    """
    return {'$in': _list_dollar_prefix(expression, array_expression)}


if __name__ == '__main__':

    def _test_module():
        import doctest
        result = doctest.testmod()
        if not result.failed:
            print(f"{result.attempted} passed and {result.failed} failed.\nTest passed.")

    _test_module()
