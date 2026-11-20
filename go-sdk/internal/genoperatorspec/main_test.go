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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/providers/localfile"
)

// The committed spec of every provider implemented in Go is what its operators generate.
func TestTheCommittedSpecsAreUpToDate(t *testing.T) {
	err := run("../../../providers", true, os.Stdout)

	assert.NoError(t, err, "run `just generate-operator-spec` in go-sdk")
}

func TestRunWritesTheSpecWhereTheProvidersAre(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, run(dir, false, os.Stdout))

	spec, err := os.ReadFile(
		filepath.Join(dir, "go", "localfile", "_generated", "lang_sdk_operators.json"),
	)
	require.NoError(t, err)
	want, err := localfile.Info.RenderSpec()
	require.NoError(t, err)
	assert.Equal(t, string(want), string(spec))
	assert.NoError(t, run(dir, true, os.Stdout), "a written spec is up to date")
}

func TestRunInCheckModeReportsAMissingOrStaleSpecAndWritesNothing(t *testing.T) {
	dir := t.TempDir()

	assert.ErrorContains(t, run(dir, true, os.Stdout), "out of date")
	assert.NoFileExists(
		t,
		filepath.Join(dir, "go", "localfile", "_generated", "lang_sdk_operators.json"),
	)

	path := filepath.Join(dir, "go", "localfile", "_generated", "lang_sdk_operators.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("{}\n"), 0o644))
	assert.ErrorContains(t, run(dir, true, os.Stdout), "out of date")
}
