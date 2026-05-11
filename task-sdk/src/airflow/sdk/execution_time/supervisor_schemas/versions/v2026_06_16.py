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
"""
Baseline supervisor schema version.

The head ``VersionBundle`` opens with this file as the in-progress
version. It carries no ``VersionChange`` entries today: the bundle's
seed shape is the head shape of every IPC body it references
(``StartupDetails``, ``DagFileParseRequest``, ...), and there are no
older versions to migrate down to yet.

When the first breaking change to one of those bodies ships, append a
``VersionChange`` subclass to this file (see the execution-API bundle's
``versions/`` folder for the canonical pattern) and reference it from
the ``VersionBundle`` in ``versions/__init__.py``.
"""

from __future__ import annotations
