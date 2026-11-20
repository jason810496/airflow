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

package provider_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/internal/clienttest"
	"github.com/apache/airflow/go-sdk/provider"
	"github.com/apache/airflow/go-sdk/sdk"
)

const callbacksClasspath = "airflow.providers.go.test.operators.callbacks.CallbacksOperator"

var (
	buildOnce   sync.Once
	pluginsDir  string
	buildErrMsg string
)

// pluginsWithCallbackPlugin builds the test plugin once and returns the directory that holds it.
func pluginsWithCallbackPlugin(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds a plugin executable")
	}
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "provider-plugins-")
		if err != nil {
			buildErrMsg = err.Error()
			return
		}
		cmd := exec.Command(
			"go",
			"build",
			"-o",
			filepath.Join(dir, "callbackplugin"),
			"./testdata/callbackplugin",
		)
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErrMsg = err.Error() + "\n" + string(out)
			return
		}
		pluginsDir = dir
	})
	require.Empty(t, buildErrMsg, "building the test plugin")
	return pluginsDir
}

// logRecords is a slog.Handler that keeps what it is given.
type logRecords struct {
	mu      *sync.Mutex
	records *[]record
	attrs   []slog.Attr
}

type record struct {
	level   slog.Level
	message string
	attrs   map[string]any
}

func newLogRecords() *logRecords { return &logRecords{mu: &sync.Mutex{}, records: &[]record{}} }

func (h *logRecords) Enabled(context.Context, slog.Level) bool { return true }

func (h *logRecords) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	attrs := map[string]any{}
	for _, attr := range h.attrs {
		attrs[attr.Key] = attr.Value.Any()
	}
	r.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	*h.records = append(*h.records, record{r.Level, r.Message, attrs})
	return nil
}

func (h *logRecords) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logRecords{
		mu:      h.mu,
		records: h.records,
		attrs:   append(h.attrs[:len(h.attrs):len(h.attrs)], attrs...),
	}
}

func (h *logRecords) WithGroup(string) slog.Handler { return h }

func (h *logRecords) all() []record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]record(nil), *h.records...)
}

func newCall(mode string, client sdk.Client, logger *slog.Logger) bundle.ProviderCall {
	runAfter := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	return bundle.ProviderCall{
		Args:         map[string]any{"mode": mode},
		TaskInstance: sdk.TaskInstance{DagID: "dag", RunID: "run1", TaskID: "cb", TryNumber: 2},
		DagRun: sdk.DagRun{
			DagID: "dag", RunID: "run1", RunAfter: runAfter, Conf: map[string]any{"n": int64(7)},
		},
		Client:         client,
		Logger:         logger,
		SkipDownstream: func(context.Context) error { return nil },
	}
}

func loadCallbacks(t *testing.T) (bundle.ProviderImpl, io.Closer) {
	t.Helper()
	provider.UsePlugins(func() string { return pluginsWithCallbackPlugin(t) })
	t.Cleanup(func() { bundle.SetProviderLoader(nil) })
	impl, closer, err := bundle.GetProviderLoader().Load(
		t.Context(), callbacksClasspath, "callbackplugin", slog.New(slog.DiscardHandler),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closer.Close() })
	return impl, closer
}

func TestAPluginTellsHowToResolveTheArgumentsOfItsOperator(t *testing.T) {
	impl, _ := loadCallbacks(t)

	assert.Equal(t, callbacksClasspath, impl.Classpath())
	assert.Equal(t, []string{"mode"}, impl.TemplateFields())
	assert.Equal(t, []bundle.ProviderParam{{Name: "mode", Required: true}}, impl.Params())
}

func TestAnOperatorInAPluginCallsBackToTheClientOfTheTask(t *testing.T) {
	impl, _ := loadCallbacks(t)
	client := clienttest.New()
	client.Variables["greeting"] = "hello"
	client.Variables["json"] = `{"N": 9}`
	login := "admin"
	client.Connections["db"] = sdk.Connection{
		ID: "db", Host: "db.example.com", Port: 5432, Login: &login, Extra: map[string]any{"ssl": true},
	}
	client.XComs["upstream:return_value"] = map[string]any{"rows": int64(12)}
	logs := newLogRecords()

	result, err := impl.Execute(t.Context(), newCall("all", client, slog.New(logs)))

	require.NoError(t, err)
	seen, ok := result.(map[string]any)
	require.True(t, ok, "%T", result)
	assert.Equal(t, "hello", seen["variable"])
	assert.Equal(t, `variable not found: "missing"`, seen["missing_variable"])
	assert.EqualValues(t, 9, seen["json_variable"])
	assert.Equal(t, map[string]any{
		"host": "db.example.com", "port": int64(5432), "login": "admin", "extra": map[string]any{"ssl": true},
	}, seen["connection"])
	assert.Equal(t, `connection not found: "missing"`, seen["missing_connection"])
	assert.Equal(t, map[string]any{"rows": int64(12)}, seen["upstream"])
	assert.Equal(t, `xcom not found: "missing"`, seen["missing_xcom"])
	assert.Equal(t, map[string]any{"page": int64(4)}, seen["checkpoint"])
	assert.EqualValues(t, 4, seen["typed_checkpoint"])
	assert.Equal(t, `task state not found: "absent"`, seen["missing_state"])

	assert.Equal(t, map[string]any{"count": int64(3), "ratio": 0.5}, client.XComs["cb:note"])
	assert.NotContains(t, client.Variables, "written", "the Variable was deleted again")
	assert.Equal(t, []string{
		"GetVariable:greeting", "GetVariable:missing", "SetVariable:written", "GetVariable:json",
		"DeleteVariable:written", "GetConnection:db", "GetConnection:missing", "PushXCom:cb:note",
		"GetXCom:upstream:return_value", "GetXCom:upstream:missing", "StateSet:checkpoint:1h0m0s",
		"StateSet:plain", "StateGet:checkpoint", "StateGet:checkpoint", "StateGet:absent",
		"StateDelete:plain", "StateClear",
	}, client.Calls())
}

func TestTheLogOfAnOperatorInAPluginReachesTheTaskLog(t *testing.T) {
	impl, _ := loadCallbacks(t)
	logs := newLogRecords()

	_, err := impl.Execute(t.Context(), newCall("all", clienttest.New(), slog.New(logs)))
	require.Error(t, err, "the first Variable is missing, but the operator logged before that")

	records := logs.all()
	require.GreaterOrEqual(t, len(records), 1)
	assert.Equal(t, slog.LevelWarn, records[0].level)
	assert.Equal(t, "calling back", records[0].message)
	assert.Equal(t, "all", records[0].attrs["mode"])
	assert.EqualValues(t, 2, records[0].attrs["n"])
	assert.Equal(t, "oops", records[0].attrs["err"], "an error is logged as its message")
	require.GreaterOrEqual(t, len(records), 2)
	assert.Equal(t, "grouped", records[1].message)
	assert.Equal(t, "callbacks", records[1].attrs["scope"])
	assert.Equal(t, "v", records[1].attrs["g.k"])
}

func TestAnOperatorInAPluginSeesTheTaskInstanceAndTheDagRun(t *testing.T) {
	impl, _ := loadCallbacks(t)

	result, err := impl.Execute(
		t.Context(),
		newCall("context", clienttest.New(), slog.New(slog.DiscardHandler)),
	)

	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"dag_id": "dag", "run_id": "run1", "try": int64(2),
		"conf": map[string]any{"n": int64(7)}, "run_after": "2026-11-20T09:00:00Z",
	}, result)
}

func TestAnOperatorInAPluginSkipsDownstreamTasksThroughTheRunner(t *testing.T) {
	impl, _ := loadCallbacks(t)
	skipped := false
	call := newCall("skip", clienttest.New(), slog.New(slog.DiscardHandler))
	call.SkipDownstream = func(context.Context) error { skipped = true; return nil }

	result, err := impl.Execute(t.Context(), call)

	require.NoError(t, err)
	assert.Nil(t, result, "an operator that returns nothing pushes nothing")
	assert.True(t, skipped)
}

func TestTheFailureOfAnOperatorInAPluginReachesTheRunner(t *testing.T) {
	impl, _ := loadCallbacks(t)

	for mode, want := range map[string]string{
		"fail":  "the operator failed",
		"panic": "panic: the operator panicked",
	} {
		_, err := impl.Execute(
			t.Context(),
			newCall(mode, clienttest.New(), slog.New(slog.DiscardHandler)),
		)
		assert.ErrorContains(t, err, want, mode)
	}

	// The plugin survives a panic, so the runner can run another operator in it.
	result, err := impl.Execute(
		t.Context(),
		newCall("context", clienttest.New(), slog.New(slog.DiscardHandler)),
	)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestLoadingAPluginFailsWithAMessageThatSaysWhy(t *testing.T) {
	dir := pluginsWithCallbackPlugin(t)
	notExecutable := filepath.Join(t.TempDir(), "plugins")
	require.NoError(t, os.MkdirAll(filepath.Join(notExecutable, "subdir"), 0o755))
	require.NoError(
		t,
		os.WriteFile(filepath.Join(notExecutable, "junk"), []byte("not a plugin"), 0o755),
	)

	tests := []struct {
		name, dir, module, classpath, want string
	}{
		{
			"no plugin directory",
			"",
			"callbackplugin",
			callbacksClasspath,
			"no plugin directory is set",
		},
		{
			"a plugin that is not installed",
			dir,
			"absent",
			callbacksClasspath,
			"the plugin absent is not in",
		},
		{
			"a path out of the directory",
			dir,
			"../callbackplugin",
			callbacksClasspath,
			"is not a plugin file name",
		},
		{
			"a nested path",
			dir,
			"sub/callbackplugin",
			callbacksClasspath,
			"is not a plugin file name",
		},
		{"a directory", notExecutable, "subdir", callbacksClasspath, "is not a file"},
		{
			"a file that is no plugin",
			notExecutable,
			"junk",
			callbacksClasspath,
			"cannot start the plugin junk",
		},
		{
			"an operator the plugin lacks",
			dir,
			"callbackplugin",
			"airflow.providers.go.test.Other",
			"does not implement",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider.UsePlugins(func() string { return tt.dir })
			t.Cleanup(func() { bundle.SetProviderLoader(nil) })

			_, closer, err := bundle.GetProviderLoader().Load(
				t.Context(), tt.classpath, tt.module, slog.New(slog.DiscardHandler),
			)

			assert.ErrorContains(t, err, tt.want)
			assert.Nil(t, closer)
		})
	}
}
