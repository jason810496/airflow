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


def get_provider_info():
    return {
        "package-name": "{{ cookiecutter.package_name }}",
        "name": "{{ cookiecutter.name }}",
        "description": "{{ cookiecutter.provider_description }}",
        {% if cookiecutter.include_integrations == "y" -%}
        "integrations": [
            {
                "integration-name": "{{ cookiecutter.name }}",
                "external-doc-url": "https://example.com",
                "tags": ["service"],
            }
        ],
        {%- endif %}
        {% if cookiecutter.include_operators == "y" -%}
        "operators": [
            {
                "integration-name": "{{ cookiecutter.name }}",
                "python-modules": ["airflow.providers.{{ cookiecutter.provider_name }}.operators.{{ cookiecutter.provider_name }}"],
            }
        ],
        {%- endif %}
        {% if cookiecutter.include_sensors == "y" -%}
        "sensors": [
            {
                "integration-name": "{{ cookiecutter.name }}",
                "python-modules": ["airflow.providers.{{ cookiecutter.provider_name }}.sensors.{{ cookiecutter.provider_name }}"],
            }
        ],
        {%- endif %}
        {% if cookiecutter.include_hooks == "y" -%}
        "hooks": [
            {
                "integration-name": "{{ cookiecutter.name }}",
                "python-modules": ["airflow.providers.{{ cookiecutter.provider_name }}.hooks.{{ cookiecutter.provider_name }}"],
            }
        ],
        {%- endif %}
        {% if cookiecutter.include_triggers == "y" -%}
        "triggers": [
            {
                "integration-name": "{{ cookiecutter.name }}",
                "python-modules": ["airflow.providers.{{ cookiecutter.provider_name }}.triggers.{{ cookiecutter.provider_name }}"],
            }
        ],
        {%- endif %}
        {% if cookiecutter.include_connection_types == "y" -%}
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.{{ cookiecutter.provider_name }}.hooks.{{ cookiecutter.provider_name }}.{{ cookiecutter.name }}Hook",
                "connection-type": "{{ cookiecutter.provider_name }}",
            }
        ],
        {%- endif %}
    }
