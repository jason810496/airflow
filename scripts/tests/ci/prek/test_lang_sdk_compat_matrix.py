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
from ci.prek import lang_sdk_compat_matrix as matrix, update_lang_sdk_central_matrix as central


def _entry(supported: bool, since: str | None = None, note: str = "") -> dict:
    return {"supported": supported, "since": since, "note": note}


def _doc(sdk_id: str = "go", **overrides) -> dict:
    """A fully-populated, valid capabilities document for ``sdk_id``."""
    doc = {
        "sdk": sdk_id,
        "supervisor_schema_version": "2026-06-16",
        "min_airflow_version": "3.3",
        "states": {state: _entry(True, since="3.3") for state, _ in matrix.STATE_DIMENSIONS},
        "capabilities": {cap.name: _entry(True, since="3.3") for cap in matrix.CAPABILITY_DIMENSIONS},
    }
    doc.update(overrides)
    return doc


class TestValidateCapabilities:
    def test_valid_doc_passes(self):
        matrix.validate_capabilities(_doc(), source="test")

    def test_non_dict_raises(self):
        with pytest.raises(matrix.CapabilitiesError, match="must be an object"):
            matrix.validate_capabilities([], source="test")

    @pytest.mark.parametrize(
        "key", ["sdk", "supervisor_schema_version", "min_airflow_version", "states", "capabilities"]
    )
    def test_missing_required_key_raises(self, key):
        doc = _doc()
        del doc[key]
        with pytest.raises(matrix.CapabilitiesError, match="missing required keys"):
            matrix.validate_capabilities(doc, source="test")

    def test_unknown_sdk_raises(self):
        with pytest.raises(matrix.CapabilitiesError, match="unknown sdk"):
            matrix.validate_capabilities(_doc(sdk_id="rust"), source="test")

    def test_missing_state_raises(self):
        doc = _doc()
        del doc["states"]["deferred"]
        with pytest.raises(matrix.CapabilitiesError, match="states keys mismatch"):
            matrix.validate_capabilities(doc, source="test")

    def test_unknown_capability_raises(self):
        doc = _doc()
        doc["capabilities"]["telepathy"] = _entry(True)
        with pytest.raises(matrix.CapabilitiesError, match="capabilities keys mismatch"):
            matrix.validate_capabilities(doc, source="test")

    def test_non_boolean_supported_raises(self):
        doc = _doc()
        doc["states"]["success"] = {"supported": "yes"}
        with pytest.raises(matrix.CapabilitiesError, match="boolean 'supported'"):
            matrix.validate_capabilities(doc, source="test")

    def test_sdk_mismatch_against_expected_raises(self):
        with pytest.raises(matrix.CapabilitiesError, match="belongs to 'go'"):
            matrix.validate_capabilities(_doc(sdk_id="java"), source="test", expected_sdk="go")

    def test_expected_sdk_match_passes(self):
        matrix.validate_capabilities(_doc(sdk_id="go"), source="test", expected_sdk="go")

    def test_non_string_note_raises(self):
        doc = _doc()
        doc["states"]["success"] = {"supported": True, "since": "3.3", "note": 123}
        with pytest.raises(matrix.CapabilitiesError, match="note must be a string"):
            matrix.validate_capabilities(doc, source="test")

    def test_non_string_since_raises(self):
        doc = _doc()
        doc["capabilities"]["xcom-read-write"] = {"supported": True, "since": 3, "note": ""}
        with pytest.raises(matrix.CapabilitiesError, match="since must be a string or null"):
            matrix.validate_capabilities(doc, source="test")

    def test_non_string_schema_version_raises(self):
        with pytest.raises(matrix.CapabilitiesError, match="supervisor_schema_version must be a string"):
            matrix.validate_capabilities(_doc(supervisor_schema_version=123), source="test")


class TestRenderMarkdownTable:
    def test_rows_cover_every_dimension(self):
        rendered = "".join(matrix.render_markdown_table(_doc()))
        for state, tier in matrix.STATE_DIMENSIONS:
            assert f"| state: `{state}` | {tier} |" in rendered
        for cap in matrix.CAPABILITY_DIMENSIONS:
            assert f"| capability: `{cap.name}` | {matrix._tier_label(cap)} |" in rendered
        assert "supervisor schema: 2026-06-16" in rendered

    def test_supported_and_unsupported_marks(self):
        doc = _doc()
        doc["states"]["deferred"] = _entry(False, note="no triggerer bridge")
        rendered = "".join(matrix.render_markdown_table(doc))
        assert f"| state: `success` | MUST | {matrix.SUPPORTED_MARK} | 3.3 |" in rendered
        assert f"| state: `deferred` | MAY | {matrix.UNSUPPORTED_MARK} | {matrix.ABSENT_MARK} |" in rendered

    def test_pipe_in_note_is_escaped(self):
        doc = _doc()
        doc["capabilities"]["xcom-read-write"] = _entry(True, since="3.3", note="read | write")
        rendered = "".join(matrix.render_markdown_table(doc))
        assert "read \\| write" in rendered


class TestRenderCentralTable:
    def test_header_lists_every_registered_sdk(self):
        rendered = "".join(matrix.render_central_table({sdk["id"]: None for sdk in matrix.LANG_SDKS}))
        assert ".. list-table:: Language SDK compatibility matrix" in rendered
        for sdk in matrix.LANG_SDKS:
            assert f"     - {sdk['display']}\n" in rendered

    def test_absent_sdk_renders_dash(self):
        rendered = "".join(matrix.render_central_table({sdk["id"]: None for sdk in matrix.LANG_SDKS}))
        # No data cell carries a supported mark when no SDK has published capabilities. (The
        # legend line mentions the mark glyph, so match the cell shape rather than the bare glyph.)
        assert f"     - {matrix.SUPPORTED_MARK}\n" not in rendered
        assert f"   * - Min. Airflow version\n     - {matrix.ABSENT_MARK}\n" in rendered

    def test_present_sdk_marks_and_meta_rows(self):
        doc = _doc("go")
        doc["capabilities"]["native-dag-authoring"] = _entry(False)
        docs = {sdk["id"]: (doc if sdk["id"] == "go" else None) for sdk in matrix.LANG_SDKS}
        rendered = "".join(matrix.render_central_table(docs))
        assert "``success`` (MUST)" in rendered
        assert "``mixed-lang-stub-target``" in rendered
        # Go supports success (✓) and does not author native Dags (✗), so every gated native-Dag
        # capability is n/a rather than unsupported, while ungated object-store stays ✓.
        assert f"   * - ``success`` (MUST)\n     - {matrix.SUPPORTED_MARK}\n" in rendered
        assert f"   * - ``native-dag-authoring`` (SHOULD)\n     - {matrix.UNSUPPORTED_MARK}\n" in rendered
        assert f"   * - ``task-args`` (MUST †)\n     - {matrix.NA_MARK}\n" in rendered
        assert f"   * - ``object-store`` (MAY)\n     - {matrix.SUPPORTED_MARK}\n" in rendered
        assert "3.3" in rendered and "2026-06-16" in rendered


class TestCentralHookMain:
    @pytest.fixture
    def wired(self, tmp_path, monkeypatch):
        """Point the hook at a temp index.rst and a temp single-SDK registry."""
        index = tmp_path / "index.rst"
        index.write_text(
            f"intro\n\n{matrix.CENTRAL_MATRIX_HEADER}\n{matrix.CENTRAL_MATRIX_FOOTER}\n\noutro\n"
        )
        go_json = tmp_path / "capabilities.json"
        registry = [
            {
                "id": "go",
                "display": "Go",
                "capabilities_json": go_json,
                "readme": tmp_path / "README.md",
            }
        ]
        monkeypatch.setattr(central, "INDEX_RST", index)
        monkeypatch.setattr(central, "LANG_SDKS", registry)
        monkeypatch.setattr(matrix, "LANG_SDKS", registry)
        return index, go_json

    def test_generate_then_idempotent(self, wired):
        index, go_json = wired
        go_json.write_text(json.dumps(_doc("go")))

        assert central.main() == 1
        content = index.read_text()
        assert ".. list-table:: Language SDK compatibility matrix" in content
        assert "intro\n" in content and "outro\n" in content
        assert matrix.SUPPORTED_MARK in content

        # Running again with no source change is a no-op.
        assert central.main() == 0

    def test_absent_capabilities_json_renders_dash(self, wired):
        index, go_json = wired
        # go_json intentionally not created -> column shows the absent mark.
        assert central.main() == 1
        content = index.read_text()
        assert f"     - {matrix.SUPPORTED_MARK}\n" not in content
        assert f"     - {matrix.ABSENT_MARK}\n" in content
