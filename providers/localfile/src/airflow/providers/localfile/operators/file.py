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

# Generated from providers/localfile/_generated/lang_sdk_operators.json by
# scripts/ci/lang_sdk_providers/generate_python_wrappers.py. Do not edit by hand.

from __future__ import annotations

from typing import Literal

from airflow.sdk import LangSdkProviderOperator


class CreateFileOperator(LangSdkProviderOperator):
    """
    Create an empty file.

    :param path: The file to create. Missing parent directories are created too.
    :param exist_ok: Succeed when the file exists already. When false, an existing file fails the task.
    """

    template_fields = ("path",)
    operator_params = ("path", "exist_ok")
    implemented_in = ("TypeScript",)
    implementation_module = "@apache-airflow/providers-localfile/operators/file"

    def __init__(
        self,
        *,
        path: str,
        exist_ok: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.exist_ok = exist_ok


class DeleteFileOperator(LangSdkProviderOperator):
    """
    Delete a file.

    :param path: The file to delete.
    :param missing_ok: Succeed when the file is missing. When false, a missing file fails the task.
    """

    template_fields = ("path",)
    operator_params = ("path", "missing_ok")
    implemented_in = ("TypeScript",)
    implementation_module = "@apache-airflow/providers-localfile/operators/file"

    def __init__(
        self,
        *,
        path: str,
        missing_ok: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.missing_ok = missing_ok


class HasFileOperator(LangSdkProviderOperator):
    """
    Check that a file exists.

    :param path: The path to look for.
    :param skip_downstream_if_missing: Skip the direct downstream tasks when nothing is at the path.
    """

    template_fields = ("path",)
    operator_params = ("path", "skip_downstream_if_missing")
    implemented_in = ("TypeScript",)
    implementation_module = "@apache-airflow/providers-localfile/operators/file"

    def __init__(
        self,
        *,
        path: str,
        skip_downstream_if_missing: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.skip_downstream_if_missing = skip_downstream_if_missing


class WriteFileOperator(LangSdkProviderOperator):
    """
    Write text to a file.

    :param path: The file to write. Missing parent directories are created.
    :param content: The text to write.
    :param mode: Replace what the file holds, or add to its end.
    :param encoding: How the text is encoded in the file.
    """

    template_fields = ("path", "content")
    operator_params = ("path", "content", "mode", "encoding")
    implemented_in = ("TypeScript",)
    implementation_module = "@apache-airflow/providers-localfile/operators/file"

    def __init__(
        self,
        *,
        path: str,
        content: str,
        mode: Literal["overwrite", "append"] = "overwrite",
        encoding: Literal["utf-8", "ascii", "latin1"] = "utf-8",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.content = content
        self.mode = mode
        self.encoding = encoding
