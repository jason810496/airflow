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

package provider

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/airflow"
)

type sampleArgs struct {
	Path    airflow.Templated[string] `airflow:"path,required"`
	Retries *int                      `airflow:"max_retries"   default:"3"`
	Mode    *string                   `airflow:"mode"          default:"fast" enum:"fast,slow"`
	Timeout *time.Duration            `airflow:"timeout"`
	Since   *time.Time                `airflow:"since"`
	Tags    []string                  `airflow:"tags"`
	Options map[string]any            `airflow:"options"`
	Flag    airflow.Templated[bool]   `airflow:"flag"`
	skipped int
}

func decodeSample(t *testing.T, args map[string]any) (sampleArgs, error) {
	t.Helper()
	params, err := readParams(reflect.TypeFor[sampleArgs]())
	require.NoError(t, err)
	var decoded sampleArgs
	return decoded, decodeArgs(params, args, reflect.ValueOf(&decoded).Elem())
}

func TestReadParamsKeepsTheOrderOfTheFields(t *testing.T) {
	params, err := readParams(reflect.TypeFor[sampleArgs]())

	require.NoError(t, err)
	names := make([]string, len(params))
	for i, p := range params {
		names[i] = p.name
	}
	assert.Equal(
		t,
		[]string{"path", "max_retries", "mode", "timeout", "since", "tags", "options", "flag"},
		names,
	)
	assert.True(t, params[0].required)
	assert.True(t, params[0].templated)
	assert.Equal(t, reflect.TypeFor[string](), params[0].valueType)
	assert.False(t, params[1].templated)
	assert.Equal(t, reflect.TypeFor[int](), params[1].valueType)
	assert.Equal(t, []string{"fast", "slow"}, params[2].enum)
}

func TestReadParamsRejectsAnInvalidStruct(t *testing.T) {
	type noTag struct{ Path string }
	type emptyName struct {
		Path string `airflow:""`
	}
	type badOption struct {
		Path string `airflow:"path,optional"`
	}
	type twice struct {
		A string `airflow:"path"`
		B string `airflow:"path"`
	}
	type templatedDuration struct {
		D airflow.Templated[time.Duration] `airflow:"d"`
	}
	tests := map[string]struct {
		args reflect.Type
		want string
	}{
		"not a struct": {reflect.TypeFor[string](), "are a struct"},
		"no tag": {
			reflect.TypeFor[noTag](),
			"field Path has no `airflow:\"name\"` tag",
		},
		"an empty name":     {reflect.TypeFor[emptyName](), "empty Python argument name"},
		"an unknown option": {reflect.TypeFor[badOption](), `unknown option "optional"`},
		"a repeated name":   {reflect.TypeFor[twice](), `both name the argument "path"`},
		"templated duration": {
			reflect.TypeFor[templatedDuration](),
			"a duration is not a template field",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := readParams(tt.args)
			assert.ErrorContains(t, err, tt.want)
		})
	}
}

func TestDecodeArgsFillsTheStruct(t *testing.T) {
	decoded, err := decodeSample(t, map[string]any{
		"path":        "/tmp/x",
		"max_retries": int64(5),
		"mode":        "slow",
		"timeout":     1.5,
		"since":       "2026-11-20T08:30:00+00:00",
		"tags":        []any{"a", "b"},
		"options":     map[string]any{"n": int64(1)},
		"flag":        true,
	})

	require.NoError(t, err)
	assert.Equal(t, "/tmp/x", decoded.Path.Value())
	assert.True(t, decoded.Path.IsSet())
	assert.Equal(t, 5, *decoded.Retries)
	assert.Equal(t, "slow", *decoded.Mode)
	assert.Equal(t, 1500*time.Millisecond, *decoded.Timeout)
	assert.True(t, decoded.Since.Equal(time.Date(2026, 11, 20, 8, 30, 0, 0, time.UTC)))
	assert.Equal(t, []string{"a", "b"}, decoded.Tags)
	assert.Equal(t, map[string]any{"n": float64(1)}, decoded.Options)
	assert.True(t, decoded.Flag.Value())
}

func TestDecodeArgsAppliesTheDefaultsOfTheArgumentsTheTaskLeftOut(t *testing.T) {
	decoded, err := decodeSample(t, map[string]any{"path": "p", "mode": nil})

	require.NoError(t, err)
	assert.Equal(t, 3, *decoded.Retries)
	assert.Equal(t, "fast", *decoded.Mode)
	assert.Nil(t, decoded.Timeout)
	assert.Nil(t, decoded.Tags)
	assert.False(t, decoded.Flag.IsSet())
}

func TestDecodeArgsRejectsAValueOfTheWrongShape(t *testing.T) {
	tests := map[string]struct {
		args map[string]any
		want string
	}{
		"a type": {map[string]any{"max_retries": "many"}, `argument "max_retries"`},
		"an enum": {
			map[string]any{"mode": "medium"},
			`argument "mode" is medium, and it takes one of ["fast" "slow"]`,
		},
		"a duration": {map[string]any{"timeout": "soon"}, `soon is not a number of seconds`},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := decodeSample(t, tt.args)
			assert.ErrorContains(t, err, tt.want)
		})
	}
}

func TestParseDefaultReadsAStringAsItIsAndAnythingElseAsJSON(t *testing.T) {
	text, err := parseDefault(param{valueType: reflect.TypeFor[string](), defaultText: "utf-8"})
	require.NoError(t, err)
	assert.Equal(t, "utf-8", text)

	flag, err := parseDefault(param{valueType: reflect.TypeFor[bool](), defaultText: "true"})
	require.NoError(t, err)
	assert.Equal(t, true, flag)

	_, err = parseDefault(param{field: "F", valueType: reflect.TypeFor[bool](), defaultText: "yes"})
	assert.ErrorContains(t, err, `field F: the default tag "yes" is not JSON`)
}

func TestIsUnset(t *testing.T) {
	var args sampleArgs
	value := reflect.ValueOf(args)

	assert.True(t, isUnset(value.FieldByName("Path")))
	assert.True(t, isUnset(value.FieldByName("Retries")))
	assert.True(t, isUnset(value.FieldByName("Tags")))
	assert.True(t, isUnset(value.FieldByName("Options")))

	args = sampleArgs{Path: airflow.Of(""), Retries: airflow.Ptr(0), Tags: []string{}}
	value = reflect.ValueOf(args)
	assert.False(t, isUnset(value.FieldByName("Path")), "an empty string is a value")
	assert.False(t, isUnset(value.FieldByName("Retries")), "a pointer to zero is a value")
	assert.False(t, isUnset(value.FieldByName("Tags")), "an empty slice is a value")
}

func TestDescribeGoType(t *testing.T) {
	tests := map[string]struct {
		in   reflect.Type
		want map[string]any
	}{
		"string":   {reflect.TypeFor[string](), map[string]any{"kind": "string"}},
		"pointer":  {reflect.TypeFor[*bool](), map[string]any{"kind": "boolean"}},
		"int":      {reflect.TypeFor[int32](), map[string]any{"kind": "integer"}},
		"float":    {reflect.TypeFor[float64](), map[string]any{"kind": "number"}},
		"duration": {reflect.TypeFor[time.Duration](), map[string]any{"kind": "timedelta"}},
		"time":     {reflect.TypeFor[time.Time](), map[string]any{"kind": "datetime"}},
		"any":      {reflect.TypeFor[any](), map[string]any{"kind": "any"}},
		"slice": {
			reflect.TypeFor[[]string](),
			map[string]any{"kind": "array", "items": map[string]any{"kind": "string"}},
		},
		"map": {
			reflect.TypeFor[map[string]int](),
			map[string]any{"kind": "object", "values": map[string]any{"kind": "integer"}},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := describeGoType(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	_, err := describeGoType(reflect.TypeFor[struct{ A int }]())
	assert.ErrorContains(t, err, "has no language-neutral type")
	_, err = describeGoType(reflect.TypeFor[map[int]string]())
	assert.ErrorContains(t, err, "a map needs string keys")
}
