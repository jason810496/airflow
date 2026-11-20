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

Apache Airflow Local File Provider
==================================

The Local File provider has operators that create, write, check and delete files of the machine that runs
the task. They are implemented in TypeScript, so a task runs on a worker of the TypeScript runtime and
not on a Python worker. A Python Dag uses the operators of this package, which only describe them.

Guides
------

.. toctree::
   :maxdepth: 2

   Operators <operators>
   Security <security>
   changelog

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/ts/localfile/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-ts-localfile package
------------------------------------------------------

Operators to create, write, check and delete files of the machine that runs the task. They are
implemented in TypeScript, so a task runs on a worker of the TypeScript runtime.


Release: 1.0.0

Provider package
----------------

This package is for the ``ts.localfile`` provider.
All classes for this package are included in the ``airflow.providers.ts.localfile`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-ts-localfile``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0``.

==================  ==================
PIP package         Version required
==================  ==================
``apache-airflow``  ``>=3.0,<4``
==================  ==================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-ts-localfile 1.0.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ts_localfile-1.0.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ts_localfile-1.0.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ts_localfile-1.0.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-ts-localfile 1.0.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ts_localfile-1.0.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ts_localfile-1.0.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ts_localfile-1.0.0-py3-none-any.whl.sha512>`__)
