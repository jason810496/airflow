#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Run a Python Dag's ``map()`` callables for a stub TaskHandler, in the supervisor's Python sidecar (ADR-0016)."""

from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from typing import Any

from airflow.sdk.definitions._internal.expandinput import MappedArgument
from airflow.sdk.definitions.xcom_arg import MapXComArg, ZipXComArg


def find_map_argument(task: Any, name: str) -> MapXComArg:
    """
    Find the ``map()`` a Python Dag's stub task applies to argument *name*.

    An ``expand_kwargs()`` input applies to every argument, and a mapped task group member's argument
    takes the ``map()`` of the group's expand argument it reads.

    :raises LookupError: if the argument has no ``map()``.
    """
    if task.is_mapped:
        value = task._get_specified_expand_input().value
    else:
        bound = inspect.signature(task.python_callable).bind_partial(*task.op_args, **task.op_kwargs)
        argument = bound.arguments.get(name)
        if not isinstance(argument, MappedArgument):
            raise LookupError(f"Argument {name!r} reads no mapped task group's expand argument")
        value = argument._input.value
        name = argument._key
    source = value.get(name) if isinstance(value, Mapping) else value
    if not isinstance(source, MapXComArg):
        raise LookupError(f"Argument {name!r} has no map() callables")
    return source


def apply_map(source: MapXComArg, value: Any) -> Any:
    """Apply the callables of *source* to *value* in order, as ``map()`` does, and return it as JSON."""
    # JSON has no tuple, so a zip() element arrives as a list.
    if isinstance(source.arg, ZipXComArg) and isinstance(value, list):
        value = tuple(value)
    for fn in source.callables:
        value = fn(value)
    return _to_json(value)


def _to_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _to_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_to_json(item) for item in value]
    return value
