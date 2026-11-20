// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The committed wrappers are what the committed specs generate.
func TestTheCommittedWrappersAreUpToDate(t *testing.T) {
	changed, err := generate("../../../providers", "../../providers", wrapped, true)

	require.NoError(t, err)
	assert.Empty(t, changed, "run `just generate-providers` in go-sdk")
}

func TestGoFieldName(t *testing.T) {
	tests := map[string]string{
		"s3_bucket":   "S3Bucket",
		"aws_conn_id": "AwsConnID",
		"acl_policy":  "ACLPolicy",
		"kms_key_arn": "KMSKeyARN",
		"url":         "URL",
		"_private":    "Private",
		"botocore":    "Botocore",
	}
	for in, want := range tests {
		assert.Equal(t, want, goFieldName(in), in)
	}
}

func parseType(t *testing.T, text string) specType {
	t.Helper()
	var parsed specType
	require.NoError(t, json.Unmarshal([]byte(text), &parsed))
	return parsed
}

func TestMapType(t *testing.T) {
	tests := map[string]struct {
		spec       string
		want       string
		scalar     bool
		needsTime  bool
		wantsError bool
	}{
		"string":  {spec: `{"kind":"string"}`, want: "string", scalar: true},
		"boolean": {spec: `{"kind":"boolean"}`, want: "bool", scalar: true},
		"integer": {spec: `{"kind":"integer"}`, want: "int", scalar: true},
		"number":  {spec: `{"kind":"number"}`, want: "float64", scalar: true},
		"timedelta": {
			spec:      `{"kind":"timedelta"}`,
			want:      "time.Duration",
			scalar:    true,
			needsTime: true,
		},
		"datetime": {
			spec:      `{"kind":"datetime"}`,
			want:      "time.Time",
			scalar:    true,
			needsTime: true,
		},
		"any":    {spec: `{"kind":"any"}`, want: "any"},
		"struct": {spec: `{"kind":"struct","fields":[]}`, want: "any"},
		"strings": {
			spec:   `{"kind":"literal","literals":["a","b"]}`,
			want:   "string",
			scalar: true,
		},
		"numbers": {spec: `{"kind":"literal","literals":[1,2]}`, want: "int", scalar: true},
		"mixed":   {spec: `{"kind":"literal","literals":["a",1]}`, want: "any"},
		"array":   {spec: `{"kind":"array","items":{"kind":"string"}}`, want: "[]string"},
		"object": {
			spec: `{"kind":"object","values":{"kind":"integer"}}`,
			want: "map[string]int",
		},
		"durations": {
			spec:      `{"kind":"array","items":{"kind":"timedelta"}}`,
			want:      "[]time.Duration",
			needsTime: true,
		},
		"same tuple": {
			spec: `{"kind":"tuple","items":[{"kind":"string"},{"kind":"string"}]}`,
			want: "[]string",
		},
		"mixed tuple": {
			spec: `{"kind":"tuple","items":[{"kind":"string"},{"kind":"integer"}]}`, want: "[]any",
		},
		"optional": {
			spec:   `{"kind":"union","options":[{"kind":"string"},{"kind":"null"}]}`,
			want:   "string",
			scalar: true,
		},
		"union": {
			spec: `{"kind":"union","options":[{"kind":"string"},{"kind":"integer"}]}`,
			want: "any",
		},
		"null only": {
			spec: `{"kind":"union","options":[{"kind":"null"},{"kind":"null"}]}`, want: "any",
		},
		"unknown": {spec: `{"kind":"quaternion"}`, wantsError: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := mapType(parseType(t, tt.spec))
			if tt.wantsError {
				assert.ErrorContains(t, err, `unknown type kind "quaternion"`)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.expr)
			assert.Equal(t, tt.scalar, got.scalar)
			assert.Equal(t, tt.needsTime, got.usesTime)
		})
	}
}

func TestPackagePath(t *testing.T) {
	path, err := packagePath("airflow.providers.amazon.aws.operators.s3", "amazon")
	require.NoError(t, err)
	assert.Equal(t, "aws/operators/s3", path)

	path, err = packagePath("airflow.providers.standard.operators.latest_only", "standard")
	require.NoError(t, err)
	assert.Equal(t, "operators/latestonly", path, "an underscore is left out of a package name")

	_, err = packagePath("airflow.providers.other.operators.x", "amazon")
	assert.ErrorContains(t, err, "is not part of the provider amazon")
	_, err = packagePath("airflow.providers.amazon.operators.select", "amazon")
	assert.ErrorContains(t, err, `the package name "select", which Go cannot use`)
}

const sampleSpec = `{
  "spec_version": "1.0",
  "provider": {"id": "sample", "package": "apache-airflow-providers-sample", "version": "2.0.0"},
  "operators": [
    {
      "name": "FetchOperator",
      "module": "airflow.providers.sample.operators.fetch_data",
      "kind": "sensor",
      "description": "Fetch data.",
      "template_fields": ["url"],
      "template_ext": [".sql"],
      "template_fields_renderers": {"url": "py"},
      "ui_color": "#fff",
      "can_skip_downstream": true,
      "params": [
        {"name": "url", "type": {"kind": "string"}, "required": true, "templated": true, "description": "Where to fetch from"},
        {"name": "retries", "type": {"kind": "integer"}, "default": 3},
        {"name": "timeout", "type": {"kind": "timedelta"}, "description": "How long to wait."},
        {"name": "mode", "type": {"kind": "literal", "literals": ["a", "b"]}, "default": "a"},
        {"name": "tags", "type": {"kind": "array", "items": {"kind": "string"}}}
      ],
      "omitted_params": [{"name": "callback", "annotation": "Callable"}]
    }
  ],
  "skipped": [{"module": "m", "name": "Heavy", "reason": "needs a | pipe"}, {"module": "m", "name": "Base", "reason": "base class"}]
}`

func writeSpec(t *testing.T, providersDir, id, content string) {
	t.Helper()
	path := filepath.Join(providersDir, id, "_generated", "lang_sdk_operators.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func TestGenerateWritesATypedWrapperAndRemovesWhatIsStale(t *testing.T) {
	providers, out := t.TempDir(), t.TempDir()
	writeSpec(t, providers, "sample", sampleSpec)
	stale := filepath.Join(out, "sample", "operators", "gone", "gone.go")
	require.NoError(t, os.MkdirAll(filepath.Dir(stale), 0o755))
	require.NoError(t, os.WriteFile(stale, []byte("package gone\n"), 0o644))

	changed, err := generate(providers, out, []string{"sample"}, true)
	require.NoError(t, err)
	assert.Len(t, changed, 3, "the wrapper, the README and the stale file")
	assert.FileExists(t, stale, "check mode writes and removes nothing")

	_, err = generate(providers, out, []string{"sample"}, false)
	require.NoError(t, err)
	assert.NoFileExists(t, stale)
	changed, err = generate(providers, out, []string{"sample"}, true)
	require.NoError(t, err)
	assert.Empty(t, changed)

	wrapper, err := os.ReadFile(
		filepath.Join(out, "sample", "operators", "fetchdata", "fetchdata.go"),
	)
	require.NoError(t, err)
	source := string(wrapper)
	// gofmt aligns the fields of a literal, so compare with the whitespace collapsed.
	flat := strings.Join(strings.Fields(source), " ")
	assert.Contains(t, source, "// Code generated by go-sdk/internal/genproviders from "+
		"providers/sample/_generated/lang_sdk_operators.json. DO NOT EDIT.")
	assert.Contains(t, source, "package fetchdata")
	assert.Contains(t, flat, "URL airflow.Templated[string]")
	assert.Contains(t, flat, "Retries *int")
	assert.Contains(t, flat, "Timeout *time.Duration")
	assert.Contains(t, flat, "Mode *string")
	assert.Contains(t, flat, "Tags []string")
	assert.Contains(t, source, `airflow.RequiredArg("url", args.URL),`)
	assert.Contains(t, source, `airflow.Arg("retries", args.Retries),`)
	assert.Contains(t, source, `"airflow.providers.sample.operators.fetch_data.FetchOperator"`)
	assert.Contains(t, flat, `TemplateFields: []string{"url"},`)
	assert.Contains(t, flat, `TemplateExt: []string{".sql"},`)
	assert.Contains(t, flat, `TemplateFieldsRenderers: map[string]string{"url": "py"},`)
	assert.Contains(t, flat, "IsSensor: true,")
	assert.Contains(t, flat, "CanSkipDownstream: true,")
	assert.Contains(t, flat, "Retries defaults to `3`.")
	assert.Contains(t, flat, "Timeout how long to wait. A duration.")
	assert.Contains(t, flat, "Mode one of `\"a\"`, `\"b\"`. Defaults to `\"a\"`.")
	assert.Contains(t, flat, "Not available from Go, because JSON cannot carry them: `callback`.")

	readme, err := os.ReadFile(filepath.Join(out, "sample", "README.md"))
	require.NoError(t, err)
	assert.Contains(
		t,
		string(readme),
		"| `FetchOperator` | `github.com/apache/airflow/go-sdk/providers/sample/operators/fetchdata` |",
	)
	assert.Contains(t, string(readme), "| `Heavy` | needs a \\| pipe |")
	assert.NotContains(t, string(readme), "`Base`")
}

func TestGenerateRejectsSpecsItCannotRead(t *testing.T) {
	providers, out := t.TempDir(), t.TempDir()

	_, err := generate(providers, out, []string{"absent"}, true)
	assert.ErrorContains(t, err, "no operator spec for absent")

	writeSpec(
		t,
		providers,
		"newer",
		`{"spec_version": "2.0", "provider": {"id": "newer"}, "operators": []}`,
	)
	_, err = generate(providers, out, []string{"newer"}, true)
	assert.ErrorContains(t, err, "has version 2.0; this generator reads version 1.x")

	writeSpec(
		t,
		providers,
		"native",
		`{"spec_version": "1.0", "provider": {"id": "native"}, "implemented_in": ["Go"], "operators": []}`,
	)
	_, err = generate(providers, out, []string{"native"}, true)
	assert.ErrorContains(t, err, "native is implemented in [Go], so it has no generated Go wrapper")

	writeSpec(
		t,
		providers,
		"twice",
		`{"spec_version": "1.0", "provider": {"id": "twice", "package": "p", "version": "1"}, "operators": [
		{"name": "A", "module": "airflow.providers.twice.x.some_thing", "kind": "operator", "template_fields": [], "params": []},
		{"name": "B", "module": "airflow.providers.twice.x.something", "kind": "operator", "template_fields": [], "params": []}
	], "skipped": []}`,
	)
	_, err = generate(providers, out, []string{"twice"}, true)
	assert.ErrorContains(t, err, "both become")

	writeSpec(
		t,
		providers,
		"clash",
		`{"spec_version": "1.0", "provider": {"id": "clash", "package": "p", "version": "1"}, "operators": [
		{"name": "A", "module": "airflow.providers.clash.ops.a", "kind": "operator", "template_fields": [],
		 "params": [{"name": "foo_id", "type": {"kind": "string"}}, {"name": "foo_ID", "type": {"kind": "string"}}]}
	], "skipped": []}`,
	)
	_, err = generate(providers, out, []string{"clash"}, true)
	assert.ErrorContains(t, err, `both become "FooID"`)
}
