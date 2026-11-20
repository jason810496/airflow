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

package localfile_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/internal/clienttest"
	"github.com/apache/airflow/go-sdk/provider"
	"github.com/apache/airflow/go-sdk/providers/localfile"
	"github.com/apache/airflow/go-sdk/sdk"
)

const classpathPrefix = "airflow.providers.go.localfile.operators.file."

// runner runs an operator in the test's own process, or in the plugin executable.
type runner struct {
	name string
	run  func(t *testing.T, operator string, args map[string]any, client sdk.Client, skip func()) (any, error)
}

func newCall(args map[string]any, client sdk.Client, skip func()) bundle.ProviderCall {
	return bundle.ProviderCall{
		Args:         args,
		TaskInstance: sdk.TaskInstance{DagID: "files", RunID: "run1", TaskID: "task"},
		Client:       client,
		Logger:       slog.New(slog.DiscardHandler),
		SkipDownstream: func(context.Context) error {
			if skip != nil {
				skip()
			}
			return nil
		},
	}
}

var (
	pluginOnce  sync.Once
	pluginsDir  string
	pluginError string
)

func buildPlugin(t *testing.T) string {
	t.Helper()
	pluginOnce.Do(func() {
		dir, err := os.MkdirTemp("", "localfile-plugins-")
		if err != nil {
			pluginError = err.Error()
			return
		}
		cmd := exec.Command(
			"go",
			"build",
			"-o",
			filepath.Join(
				dir,
				"airflow-provider-go-localfile",
			),
			"./cmd/airflow-provider-go-localfile",
		)
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			pluginError = err.Error() + "\n" + string(out)
			return
		}
		pluginsDir = dir
	})
	require.Empty(t, pluginError)
	return pluginsDir
}

// runners returns both ways the operators run: linked into the process, as in a Go Dag executable, and
// in the plugin that the generic runner starts.
func runners(t *testing.T) []runner {
	t.Helper()
	runners := []runner{
		{
			"linked",
			func(t *testing.T, operator string, args map[string]any, client sdk.Client, skip func()) (any, error) {
				impl, ok := bundle.LookupProvider(classpathPrefix + operator)
				require.True(t, ok)
				return impl.Execute(t.Context(), newCall(args, client, skip))
			},
		},
	}
	if testing.Short() {
		return runners
	}
	dir := buildPlugin(t)
	provider.UsePlugins(func() string { return dir })
	t.Cleanup(func() { bundle.SetProviderLoader(nil) })
	return append(
		runners,
		runner{
			"plugin",
			func(t *testing.T, operator string, args map[string]any, client sdk.Client, skip func()) (any, error) {
				impl, closer, err := bundle.GetProviderLoader().Load(
					t.Context(),
					classpathPrefix+operator, "airflow-provider-go-localfile", slog.New(slog.DiscardHandler),
				)
				require.NoError(t, err)
				defer closer.Close()
				return impl.Execute(t.Context(), newCall(args, client, skip))
			},
		},
	)
}

func forEachRunner(t *testing.T, test func(t *testing.T, r runner)) {
	t.Helper()
	for _, r := range runners(t) {
		t.Run(r.name, func(t *testing.T) { test(t, r) })
	}
}

func sha(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

func TestCreateFileOperator(t *testing.T) {
	forEachRunner(t, func(t *testing.T, r runner) {
		path := filepath.Join(t.TempDir(), "a", "b", "file.txt")

		created, err := r.run(
			t,
			"CreateFileOperator",
			map[string]any{"path": path},
			clienttest.New(),
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"path": path, "created": true}, asMap(t, created))
		assert.FileExists(t, path)

		existed, err := r.run(
			t,
			"CreateFileOperator",
			map[string]any{"path": path},
			clienttest.New(),
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"path": path, "created": false}, asMap(t, existed))

		_, err = r.run(
			t,
			"CreateFileOperator",
			map[string]any{"path": path, "exist_ok": false},
			clienttest.New(),
			nil,
		)
		assert.ErrorContains(t, err, path+" exists already")
	})
}

func TestWriteFileOperator(t *testing.T) {
	forEachRunner(t, func(t *testing.T, r runner) {
		path := filepath.Join(t.TempDir(), "report.txt")
		content := "naïve café\n"

		written, err := r.run(t, "WriteFileOperator",
			map[string]any{"path": path, "content": content}, clienttest.New(), nil)
		require.NoError(t, err)
		assert.Equal(
			t,
			map[string]any{
				"path":   path,
				"bytes":  int64(len(content)),
				"sha256": sha(content),
			},
			asMap(t, written),
		)

		appended, err := r.run(
			t,
			"WriteFileOperator",
			map[string]any{
				"path":    path,
				"content": "more",
				"mode":    "append",
			},
			clienttest.New(),
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{
			"path": path, "bytes": int64(len(content) + 4), "sha256": sha(content + "more"),
		}, asMap(t, appended))

		_, err = r.run(
			t,
			"WriteFileOperator",
			map[string]any{
				"path":     path,
				"content":  "é",
				"encoding": "latin1",
			},
			clienttest.New(),
			nil,
		)
		require.NoError(t, err)
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, []byte{0xe9}, data)
	})
}

func TestWriteFileOperatorRejectsWhatItCannotEncode(t *testing.T) {
	forEachRunner(t, func(t *testing.T, r runner) {
		path := filepath.Join(t.TempDir(), "report.txt")

		_, err := r.run(
			t,
			"WriteFileOperator",
			map[string]any{
				"path":     path,
				"content":  "café",
				"encoding": "ascii",
			},
			clienttest.New(),
			nil,
		)
		assert.ErrorContains(t, err, `the character 'é' cannot be encoded as ascii`)
		_, err = r.run(
			t,
			"WriteFileOperator",
			map[string]any{
				"path":     path,
				"content":  "x",
				"encoding": "ebcdic",
			},
			clienttest.New(),
			nil,
		)
		assert.ErrorContains(t, err, `argument "encoding" is ebcdic`)
		_, err = r.run(t, "WriteFileOperator",
			map[string]any{"path": path, "content": "x", "mode": "truncate"}, clienttest.New(), nil)
		assert.ErrorContains(t, err, `argument "mode" is truncate`)
		assert.NoFileExists(t, path)
	})
}

func TestHasFileOperator(t *testing.T) {
	forEachRunner(t, func(t *testing.T, r runner) {
		dir := t.TempDir()
		present := filepath.Join(dir, "present.txt")
		require.NoError(t, os.WriteFile(present, nil, 0o644))
		missing := filepath.Join(dir, "missing.txt")
		skipped := 0
		skip := func() { skipped++ }

		found, err := r.run(
			t,
			"HasFileOperator",
			map[string]any{"path": present, "skip_downstream_if_missing": true},
			clienttest.New(),
			skip,
		)
		require.NoError(t, err)
		assert.Equal(t, true, found)

		absent, err := r.run(
			t,
			"HasFileOperator",
			map[string]any{"path": missing},
			clienttest.New(),
			skip,
		)
		require.NoError(t, err)
		assert.Equal(t, false, absent)
		assert.Zero(t, skipped, "a missing file skips nothing unless the task asks")

		absent, err = r.run(
			t,
			"HasFileOperator",
			map[string]any{"path": missing, "skip_downstream_if_missing": true},
			clienttest.New(),
			skip,
		)
		require.NoError(t, err)
		assert.Equal(t, false, absent)
		assert.Equal(t, 1, skipped)
	})
}

func TestDeleteFileOperator(t *testing.T) {
	forEachRunner(t, func(t *testing.T, r runner) {
		path := filepath.Join(t.TempDir(), "gone.txt")
		require.NoError(t, os.WriteFile(path, nil, 0o644))

		deleted, err := r.run(
			t,
			"DeleteFileOperator",
			map[string]any{"path": path},
			clienttest.New(),
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"path": path, "deleted": true}, asMap(t, deleted))
		assert.NoFileExists(t, path)

		again, err := r.run(
			t,
			"DeleteFileOperator",
			map[string]any{"path": path},
			clienttest.New(),
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"path": path, "deleted": false}, asMap(t, again))

		_, err = r.run(
			t,
			"DeleteFileOperator",
			map[string]any{"path": path, "missing_ok": false},
			clienttest.New(),
			nil,
		)
		assert.ErrorContains(t, err, path+" is missing")
	})
}

func TestWriteVariableOperatorReadsTheVariableThroughTheClient(t *testing.T) {
	forEachRunner(t, func(t *testing.T, r runner) {
		path := filepath.Join(t.TempDir(), "variable.txt")
		client := clienttest.New()
		client.Variables["go_localfile_secret"] = "from the variable\n"

		written, err := r.run(t, "WriteVariableOperator",
			map[string]any{"path": path, "key": "go_localfile_secret"}, client, nil)

		require.NoError(t, err)
		assert.Equal(t, []string{"GetVariable:go_localfile_secret"}, client.Calls())
		assert.Equal(t, map[string]any{
			"path": path, "bytes": int64(len("from the variable\n")), "sha256": sha("from the variable\n"),
		}, asMap(t, written))
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "from the variable\n", string(data))
	})
}

func TestWriteVariableOperatorFailsWhenTheVariableIsMissing(t *testing.T) {
	forEachRunner(t, func(t *testing.T, r runner) {
		path := filepath.Join(t.TempDir(), "variable.txt")

		_, err := r.run(t, "WriteVariableOperator",
			map[string]any{"path": path, "key": "absent"}, clienttest.New(), nil)

		assert.ErrorContains(t, err, `reading the Variable "absent"`)
		assert.ErrorContains(t, err, "variable not found")
		assert.NoFileExists(t, path)
	})
}

// asMap returns a result as a map, whichever way the runner returned it: the operator's own struct
// when it runs in the process, or decoded JSON when it runs in a plugin.
func asMap(t *testing.T, value any) map[string]any {
	t.Helper()
	if m, ok := value.(map[string]any); ok {
		return m
	}
	data, err := jsonRoundTrip(value)
	require.NoError(t, err)
	return data
}

func TestTheOperatorsAddTasksToADag(t *testing.T) {
	dag := airflow.Dag("files")
	content := dag.Task(
		func(airflow.Context) (string, error) { return "x", nil },
		airflow.TaskSpec{TaskID: "content"},
	)

	assert.NotPanics(t, func() {
		dag.Task(
			localfile.CreateFileOperator(
				localfile.CreateFileOperatorArgs{Path: airflow.Of("/tmp/x")},
			),
			airflow.TaskSpec{TaskID: "create"},
		)
		dag.Task(localfile.WriteFileOperator(localfile.WriteFileOperatorArgs{
			Path:     airflow.Of("/tmp/x"),
			Content:  airflow.ResultOf[string](content),
			Mode:     airflow.Ptr(localfile.WriteModeAppend),
			Encoding: airflow.Ptr(localfile.EncodingLatin1),
		}), airflow.TaskSpec{TaskID: "write"})
		dag.Task(localfile.HasFileOperator(localfile.HasFileOperatorArgs{
			Path: airflow.Of("/tmp/x"), SkipDownstreamIfMissing: airflow.Ptr(true),
		}), airflow.TaskSpec{TaskID: "has"})
		dag.Task(
			localfile.DeleteFileOperator(
				localfile.DeleteFileOperatorArgs{Path: airflow.Of("/tmp/x")},
			),
			airflow.TaskSpec{TaskID: "delete"},
		)
		dag.Task(localfile.WriteVariableOperator(localfile.WriteVariableOperatorArgs{
			Path: airflow.Of("/tmp/x"), Key: airflow.Of("k"),
		}), airflow.TaskSpec{TaskID: "variable"})
	})
}

func jsonRoundTrip(value any) (map[string]any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var decoded map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&decoded); err != nil {
		return nil, err
	}
	for key, v := range decoded {
		if n, ok := v.(json.Number); ok {
			i, err := n.Int64()
			if err != nil {
				return nil, err
			}
			decoded[key] = i
		}
	}
	return decoded, nil
}
