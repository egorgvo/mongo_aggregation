#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from .patterns import _convert_names_with_underlines_to_dots


def merge_objects(*args, **kwargs):
    """Combines multiple documents into a single document.
    $mergeObjects operator implementation.
    From MongoDB version 3.6."""
    # Put everything into args
    args = list(args)
    if kwargs:
        kwargs = _convert_names_with_underlines_to_dots(kwargs, convert_operators=True)
        args.append(kwargs)
    return {'$mergeObjects': args}
