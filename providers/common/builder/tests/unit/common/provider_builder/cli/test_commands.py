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
from __future__ import annotations

import pytest

from airflow.providers.common.builder.config import NewProviderRequest, ProviderFeatures


class TestNewProviderRequest:
    def test_from_package_name_simple(self):
        req = NewProviderRequest.from_package_name("airflow.providers.coffee.Coffee")
        assert req.class_name == "Coffee"
        assert req.provider_name == "coffee"
        assert req.module_path == "airflow.providers.coffee"
        assert req.module_parts == ["coffee"]
        assert req.pip_package_name == "apache-airflow-providers-coffee"

    def test_from_package_name_nested(self):
        req = NewProviderRequest.from_package_name("airflow.providers.my_company.cloud.MyCloud")
        assert req.class_name == "MyCloud"
        assert req.provider_name == "cloud"
        assert req.module_path == "airflow.providers.my_company.cloud"
        assert req.module_parts == ["my_company", "cloud"]
        assert req.pip_package_name == "apache-airflow-providers-my-company-cloud"

    def test_from_package_name_three_levels(self):
        req = NewProviderRequest.from_package_name("airflow.providers.acme.storage.gcs.GcsProvider")
        assert req.class_name == "GcsProvider"
        assert req.provider_name == "gcs"
        assert req.module_parts == ["acme", "storage", "gcs"]
        assert req.pip_package_name == "apache-airflow-providers-acme-storage-gcs"

    @pytest.mark.parametrize(
        ("package_name", "expected_error_substr"),
        [
            ("foo.bar", "at least 4 segments"),
            ("foo.providers.x.X", "must start with 'airflow.providers.'"),
            ("airflow.providers.123.Foo", "not a valid Python identifier"),
            ("airflow.providers.Foo.Bar", "must be lowercase"),
            ("airflow.providers.foo.bar", "must start with an uppercase"),
        ],
    )
    def test_validate_package_name_invalid(self, package_name, expected_error_substr):
        is_valid, error = NewProviderRequest.validate_package_name(package_name)
        assert not is_valid
        assert expected_error_substr in error

    def test_validate_package_name_valid(self):
        is_valid, error = NewProviderRequest.validate_package_name("airflow.providers.coffee.Coffee")
        assert is_valid
        assert error is None


class TestProviderFeatures:
    def test_all_default_true(self):
        features = ProviderFeatures()
        assert features.disabled_features() == []

    def test_exclude_features(self):
        features = ProviderFeatures(executors=False, auth_managers=False)
        disabled = features.disabled_features()
        assert "executors" in disabled
        assert "auth_managers" in disabled
        assert "operators" not in disabled

    def test_all_feature_names(self):
        names = ProviderFeatures.all_feature_names()
        assert "operators" in names
        assert "xcom" in names
        assert len(names) == 24
