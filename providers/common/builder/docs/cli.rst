
 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

CLI
===

The Common Provider Builder adds the ``airflow new-provider`` CLI command.

Usage
-----

Interactive mode (default)::

    airflow new-provider --interactive

Non-interactive mode::

    airflow new-provider --package-name airflow.providers.coffee.Coffee --provider-description "My Coffee provider"

Options
-------

``--package-name``
    Dotted package path in the format ``airflow.providers.<module_path>.<ClassName>``.
    The last segment must be PascalCase.

``--provider-description``
    A short description of the provider package.

``--output-dir``
    The directory where the provider package skeleton will be created.
    Defaults to the current directory.

``--interactive``
    If set, launches an interactive TUI for configuring the provider.

``--exclude-features``
    A comma-separated list of features to exclude.
