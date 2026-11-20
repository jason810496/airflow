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

import json

import pytest
from ci.lang_sdk_providers import generate_java_wrappers as generator

STRING = {"kind": "string"}
NULL = {"kind": "null"}
MODULE = "airflow.providers.demo.operators.notify"
PACKAGE = "org.apache.airflow.providers.demo.operators.notify"

SPEC = {
    "spec_version": "1.0",
    "provider": {"id": "demo", "package": "apache-airflow-providers-demo", "version": "1.0.0"},
    "operators": [
        {
            "name": "NotifyOperator",
            "module": MODULE,
            "kind": "operator",
            "description": 'Notify <people> & more, as \\u0041 "quoted" */ text with naïve.',
            "template_fields": ["subject"],
            "ui_color": "#e6f1f2",
            "params": [
                {
                    "name": "subject",
                    "type": STRING,
                    "required": True,
                    "templated": True,
                    "description": "What to say.",
                },
                {"name": "channel", "type": {"kind": "union", "options": [STRING, NULL]}},
                {"name": "retries", "type": {"kind": "integer"}, "default": 3},
                {"name": "ratio", "type": {"kind": "number"}},
                {"name": "response_timeout", "type": {"kind": "timedelta"}},
                {"name": "not_before", "type": {"kind": "datetime"}},
                {"name": "mode", "type": {"kind": "literal", "literals": ["a", "b"]}, "default": "a"},
                {"name": "tags", "type": {"kind": "array", "items": STRING}},
                {"name": "extra", "type": {"kind": "object", "values": {"kind": "any"}}},
                {"name": "anything", "type": {"kind": "any"}, "templated": True},
                {"name": "class", "type": STRING},
            ],
        },
        {
            "name": "WaitSensor",
            "module": MODULE,
            "kind": "sensor",
            "template_fields": [],
            "can_skip_downstream": True,
            "template_ext": [".txt"],
            "template_fields_renderers": {"path": "bash"},
            "operator_name": "Wait",
            "omitted_params": [{"name": "callback", "annotation": "Callable"}],
            "params": [],
        },
    ],
    "skipped": [],
}


def notify() -> str:
    return generator.build_files(SPEC)[f"{PACKAGE.replace('.', '/')}/NotifyOperator.java"]


def test_build_files_writes_one_class_per_operator_in_the_package_of_its_module():
    files = generator.build_files(SPEC)

    assert sorted(files) == [
        "org/apache/airflow/providers/demo/operators/notify/NotifyOperator.java",
        "org/apache/airflow/providers/demo/operators/notify/WaitSensor.java",
    ]
    text = notify()
    assert text.startswith("/*\n * Licensed to the Apache Software Foundation (ASF)")
    assert (
        "// Generated from providers/demo/_generated/lang_sdk_operators.json by\n"
        "// scripts/ci/lang_sdk_providers/generate_java_wrappers.py. Do not edit by hand.\n"
    ) in text
    assert f"package {PACKAGE};" in text
    assert "public final class NotifyOperator {" in text
    assert (
        'public static final String CLASSPATH = "airflow.providers.demo.operators.notify.NotifyOperator";'
        in text
    )
    assert "public static final class Builder extends OperatorBuilder<Builder, Object> {" in text


def test_the_operator_info_carries_what_the_serializer_writes():
    assert (
        'OperatorInfo.of("airflow.providers.demo.operators.notify", "NotifyOperator")\n          .params(\n'
    ) in notify()
    text = notify()
    assert '.required("subject")' in text
    assert '.templated("subject")' in text
    assert '.uiColor("#e6f1f2")' in text
    sensor = generator.build_files(SPEC)[f"{PACKAGE.replace('.', '/')}/WaitSensor.java"]
    assert '.templateExt(".txt")' in sensor
    assert '.templateFieldsRenderers(Map.of("path", "bash"))' in sensor
    assert '.operatorName("Wait")' in sensor
    assert ".sensor()" in sensor
    assert ".canSkipDownstream()" in sensor
    assert "import java.util.Map;" in sensor
    assert "Not available from Java, because JSON cannot carry them: {@code callback}." in sensor


@pytest.mark.parametrize(
    "method",
    [
        "public Builder subject(String value) {",
        "public Builder subject(TaskRef<?> value) {",
        "public Builder channel(String value) {",
        "public Builder retries(long value) {",
        "public Builder ratio(double value) {",
        "public Builder responseTimeout(Duration value) {",
        "public Builder notBefore(OffsetDateTime value) {",
        "public Builder mode(String value) {",
        "public Builder tags(List<String> value) {",
        "public Builder extra(Map<String, Object> value) {",
        # An Object already takes the result of a task.
        "public Builder anything(Object value) {",
        "public Builder class_(String value) {",
    ],
)
def test_each_argument_has_a_typed_method(method):
    assert method in notify()


def test_only_a_templated_argument_takes_the_result_of_a_task():
    text = notify()

    assert text.count("TaskRef<?> value") == 1
    assert 'return param("subject", value);' in text
    assert 'return param("class", value);' in text


def test_imports_are_sorted_and_only_what_is_used():
    text = notify()

    assert (
        "import java.time.Duration;\n"
        "import java.time.OffsetDateTime;\n"
        "import java.util.List;\n"
        "import java.util.Map;\n"
        "import org.apache.airflow.sdk.TaskRef;\n"
        "import org.apache.airflow.sdk.provider.OperatorBuilder;\n"
        "import org.apache.airflow.sdk.provider.OperatorInfo;\n"
    ) in text
    assert (
        "import java.time" not in generator.build_files(SPEC)[f"{PACKAGE.replace('.', '/')}/WaitSensor.java"]
    )


def test_the_javadoc_is_escaped_and_ascii():
    text = notify()

    assert ('Notify &lt;people&gt; &amp; more, as &#92;u0041 "quoted" *&#47; text with na&#239;ve.') in text
    assert text.isascii()
    assert "Defaults to 3." in text
    assert 'One of "a", "b". Defaults to "a".' in text
    assert "Templated." in text


def test_an_argument_with_a_name_that_is_not_a_method_name_gets_an_underscore():
    assert [
        generator.to_camel_case(name) for name in ("bucket_name", "class", "build", "default", "s3_key")
    ] == [
        "bucketName",
        "class_",
        "build_",
        "default_",
        "s3Key",
    ]


def test_build_files_rejects_arguments_that_become_one_method():
    spec = json.loads(json.dumps(SPEC))
    spec["operators"][0]["params"] = [
        {"name": "a_b", "type": STRING},
        {"name": "a__b", "type": STRING},
    ]

    with pytest.raises(generator.UnsupportedSpecError, match="'a_b' and 'a__b' both become 'aB'"):
        generator.build_files(spec)


@pytest.mark.parametrize(
    ("spec_type", "expected"),
    [
        ({"kind": "tuple", "items": [STRING, {"kind": "integer"}]}, "List<Object>"),
        (
            {"kind": "struct", "fields": [{"name": "a", "type": STRING, "required": True}]},
            "Map<String, Object>",
        ),
        ({"kind": "union", "options": [{"kind": "integer"}, {"kind": "number"}, NULL]}, "double"),
        ({"kind": "union", "options": [STRING, {"kind": "boolean"}]}, "Object"),
        ({"kind": "union", "options": [{"kind": "timedelta"}, NULL]}, "Duration"),
        ({"kind": "literal", "literals": [1, 2]}, "Object"),
        ({"kind": "array", "items": {"kind": "integer"}}, "List<Long>"),
        ({"kind": "object", "values": {"kind": "boolean"}}, "Map<String, Boolean>"),
        ({"kind": "null"}, "Object"),
    ],
)
def test_render_type_maps_the_spec_kinds(spec_type, expected):
    assert generator.render_type(spec_type, used=set()) == expected


def test_render_type_rejects_a_kind_it_does_not_know():
    with pytest.raises(generator.UnsupportedSpecError, match="'mystery' is not supported"):
        generator.render_type({"kind": "mystery"}, used=set())


def test_an_operator_implemented_in_java_names_the_module_that_runs_it():
    spec = json.loads(json.dumps(SPEC))
    spec["operators"][0]["implementation_module"] = "java-demo/org.example.NotifyOperator"

    text = generator.build_files(spec)[f"{PACKAGE.replace('.', '/')}/NotifyOperator.java"]

    assert '.implementationModule("java-demo/org.example.NotifyOperator")' in text
    assert "in the Java runtime" in text
    assert "on a Python worker" in notify()


def test_build_files_rejects_a_spec_it_cannot_read():
    with pytest.raises(generator.UnsupportedSpecError, match="version 2.0"):
        generator.build_files({**SPEC, "spec_version": "2.0"})
    other = json.loads(json.dumps(SPEC))
    other["operators"][0]["module"] = "airflow.providers.other.file"
    with pytest.raises(generator.UnsupportedSpecError, match="not part of provider demo"):
        generator.build_files(other)


def write_provider(tmp_path):
    providers = tmp_path / "providers"
    spec_file = providers / "demo" / "_generated" / "lang_sdk_operators.json"
    spec_file.parent.mkdir(parents=True, exist_ok=True)
    spec_file.write_text(json.dumps(SPEC))
    return providers, tmp_path / "java-sdk"


def run_main(monkeypatch, tmp_path, *extra):
    providers, java_sdk = write_provider(tmp_path)
    monkeypatch.setattr(generator, "JAVA_SDK_PROVIDERS", {"demo": "providers/demo"})
    monkeypatch.setattr(generator, "validate_spec", lambda spec: None)
    return generator.main(
        ["--providers-dir", str(providers), "--java-sdk-dir", str(java_sdk), *extra]
    ), java_sdk


def test_main_writes_the_wrappers_and_check_finds_them_up_to_date(monkeypatch, tmp_path):
    status, java_sdk = run_main(monkeypatch, tmp_path)
    target = (
        java_sdk
        / "providers"
        / "demo"
        / "src"
        / "main"
        / "java"
        / PACKAGE.replace(".", "/")
        / "NotifyOperator.java"
    )

    assert status == 0
    assert target.read_text() == notify()
    assert run_main(monkeypatch, tmp_path, "--check")[0] == 0


def test_main_check_reports_a_wrapper_edited_by_hand_and_a_file_no_spec_generates(
    monkeypatch, tmp_path, capsys
):
    _, java_sdk = run_main(monkeypatch, tmp_path)
    package_dir = java_sdk / "providers" / "demo" / "src" / "main" / "java" / PACKAGE.replace(".", "/")
    (package_dir / "NotifyOperator.java").write_text("edited")
    (
        java_sdk
        / "providers"
        / "demo"
        / "src"
        / "main"
        / "java"
        / "org/apache/airflow/providers/demo"
        / "Stale.java"
    ).write_text("x")

    assert run_main(monkeypatch, tmp_path, "--check")[0] == 1

    err = capsys.readouterr().err
    assert "NotifyOperator.java is out of date" in err
    assert "Stale.java is not generated from demo's spec" in err
    # Writing puts it right, and removes what no spec generates.
    assert run_main(monkeypatch, tmp_path)[0] == 0
    assert (package_dir / "NotifyOperator.java").read_text() == notify()
    assert not (package_dir.parent / "Stale.java").exists()


def test_main_fails_when_a_listed_provider_has_no_spec(monkeypatch, tmp_path, capsys):
    providers, java_sdk = write_provider(tmp_path)
    monkeypatch.setattr(
        generator, "JAVA_SDK_PROVIDERS", {"demo": "providers/demo", "absent": "providers/absent"}
    )
    monkeypatch.setattr(generator, "validate_spec", lambda spec: None)

    status = generator.main(["--providers-dir", str(providers), "--java-sdk-dir", str(java_sdk), "--check"])

    assert status == 1
    assert "No spec found for absent" in capsys.readouterr().err


def test_committed_wrappers_are_what_the_committed_specs_generate(capsys):
    """Fails when a committed spec and the Java wrappers generated from it have drifted apart."""
    assert generator.main(["--check"]) == 0, capsys.readouterr().err


@pytest.mark.parametrize("provider_id", sorted(generator.JAVA_SDK_PROVIDERS))
def test_every_listed_provider_has_a_committed_spec_and_a_gradle_project(provider_id):
    spec_file = generator.PROVIDERS_DIR.joinpath(
        *provider_id.split("."), "_generated", generator.SPEC_FILE_NAME
    )
    assert spec_file.is_file()
    project = generator.JAVA_SDK_DIR / generator.JAVA_SDK_PROVIDERS[provider_id]
    assert (project / "build.gradle.kts").is_file()
    settings = (generator.JAVA_SDK_DIR / "settings.gradle.kts").read_text()
    assert f'"{generator.JAVA_SDK_PROVIDERS[provider_id].replace("/", ":")}"' in settings
