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

package execution

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/pkg/binding"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// providerSupervisor answers the requests of a provider operator task and keeps them in order.
type providerSupervisor struct {
	mu       sync.Mutex
	requests []map[string]any
	// xcoms maps the task_id of an upstream task to its return_value.
	xcoms map[string]any
	// rendered maps a template field to what the supervisor renders it as.
	rendered map[string]any
}

func (s *providerSupervisor) answer(req map[string]any) (body, errBody map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req)
	switch req["type"] {
	case "GetXCom":
		value, ok := s.xcoms[req["task_id"].(string)]
		if !ok {
			return nil, map[string]any{
				"type":   "ErrorResponse",
				"error":  "XCOM_NOT_FOUND",
				"detail": map[string]any{},
			}
		}
		return map[string]any{"type": "XComResult", "key": req["key"], "value": value}, nil
	case "RenderTemplateFields":
		return map[string]any{"type": "RenderedTemplateFieldsResult", "values": s.rendered}, nil
	}
	return nil, nil
}

func (s *providerSupervisor) sent(kind string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var found []map[string]any
	for _, req := range s.requests {
		if req["type"] == kind {
			found = append(found, req)
		}
	}
	return found
}

func (s *providerSupervisor) types() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	types := make([]string, len(s.requests))
	for i, req := range s.requests {
		types[i] = req["type"].(string)
		if types[i] == "SetXCom" {
			types[i] += ":" + req["key"].(string)
		}
	}
	return types
}

func (s *providerSupervisor) comm(t *testing.T) *CoordinatorComm {
	t.Helper()
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	t.Cleanup(func() {
		reqR.Close()
		reqW.Close()
		respR.Close()
		respW.Close()
	})
	go func() {
		for {
			frame, err := readFrame(reqR)
			if err != nil {
				return
			}
			body, errBody := s.answer(rawToMap(t, frame.Body))
			if err := writeFrame(respW, encodeResponseFrame(t, frame.ID, body, errBody)); err != nil {
				return
			}
		}
	}()
	return NewCoordinatorComm(respR, reqW, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// fakeOperator is a provider operator that a test registers.
type fakeOperator struct {
	classpath      string
	templateFields []string
	params         []bundle.ProviderParam
	execute        func(ctx context.Context, call bundle.ProviderCall) (any, error)
}

func (f *fakeOperator) Classpath() string              { return f.classpath }
func (f *fakeOperator) TemplateFields() []string       { return f.templateFields }
func (f *fakeOperator) Params() []bundle.ProviderParam { return f.params }
func (f *fakeOperator) Execute(ctx context.Context, call bundle.ProviderCall) (any, error) {
	return f.execute(ctx, call)
}

// registerFakeOperator registers an operator under a class path that no other test uses.
func registerFakeOperator(
	t *testing.T,
	execute func(context.Context, bundle.ProviderCall) (any, error),
) *fakeOperator {
	t.Helper()
	op := &fakeOperator{
		classpath: "airflow.providers.test.operators." + strings.ReplaceAll(
			t.Name(),
			"/",
			"_",
		),
		templateFields: []string{"path"},
		params: []bundle.ProviderParam{
			{Name: "path", Required: true},
			{Name: "content"},
		},
		execute: execute,
	}
	bundle.RegisterProvider(op)
	return op
}

func providerDetails(
	classpath, module string,
	kwargs ...genmodels.TaskArgBinding,
) *genmodels.StartupDetails {
	details := newStartupDetails("write")
	provider := &genmodels.TIProviderOperator{
		Classpath:         classpath,
		Kwargs:            kwargs,
		DownstreamTaskIds: genmodels.DownstreamTaskIds{"write": {"after", "later"}},
	}
	if module != "" {
		provider.Module = module
	}
	details.TIContext.ProviderOperator = provider
	return details
}

func literalBinding(name string, value any) genmodels.TaskArgBinding {
	return map[string]any{"name": name, "kind": "literal", "value": value}
}

func xcomBinding(name, taskID string) genmodels.TaskArgBinding {
	return map[string]any{"name": name, "kind": "xcom", "task_id": taskID}
}

func runProvider(
	t *testing.T,
	supervisor *providerSupervisor,
	details *genmodels.StartupDetails,
) any {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// The bundle knows nothing of the task: the run context names the operator.
	return RunTask(context.Background(), testBundle{}, details, supervisor.comm(t), logger)
}

func TestProviderOperatorRunsWithRenderedAndPulledArguments(t *testing.T) {
	var got bundle.ProviderCall
	op := registerFakeOperator(t, func(_ context.Context, call bundle.ProviderCall) (any, error) {
		got = call
		return map[string]any{"bytes": 7}, nil
	})
	supervisor := &providerSupervisor{
		xcoms:    map[string]any{"build": "the report"},
		rendered: map[string]any{"path": "/tmp/run1/report.txt"},
	}

	result := runProvider(t, supervisor, providerDetails(
		op.classpath, "",
		literalBinding("path", "/tmp/{{ run_id }}/report.txt"),
		xcomBinding("content", "build"),
		literalBinding("unknown", "ignored"),
	))

	assertSucceedTask(t, result)
	assert.Equal(
		t,
		map[string]any{"path": "/tmp/run1/report.txt", "content": "the report"},
		got.Args,
	)
	assert.Equal(t, "write", got.TaskInstance.TaskID)
	assert.Equal(
		t,
		[]string{"GetXCom", "RenderTemplateFields", "SetXCom:return_value"},
		supervisor.types(),
	)
	// Only the template field goes to the supervisor, and the result of the upstream task does not.
	assert.Equal(t, map[string]any{"path": "/tmp/{{ run_id }}/report.txt"},
		supervisor.sent("RenderTemplateFields")[0]["values"])
	assert.Equal(t, "build", supervisor.sent("GetXCom")[0]["task_id"])
	assert.Equal(t, "return_value", supervisor.sent("GetXCom")[0]["key"])
	pushed, ok := supervisor.sent("SetXCom")[0]["value"].(map[string]any)
	require.True(t, ok)
	assert.EqualValues(t, 7, pushed["bytes"])
}

func TestProviderOperatorDoesNotRenderAResultOfAnotherTask(t *testing.T) {
	var got map[string]any
	op := registerFakeOperator(t, func(_ context.Context, call bundle.ProviderCall) (any, error) {
		got = call.Args
		return true, nil
	})
	supervisor := &providerSupervisor{xcoms: map[string]any{"build": "{{ not a template }}"}}

	result := runProvider(
		t,
		supervisor,
		providerDetails(op.classpath, "", xcomBinding("path", "build")),
	)

	// path is a template field, but a result is pulled and is never rendered.
	assertSucceedTask(t, result)
	assert.Equal(t, []string{"GetXCom", "SetXCom:return_value"}, supervisor.types())
	assert.Equal(t, map[string]any{"path": "{{ not a template }}"}, got)
}

func TestProviderOperatorPushesNothingForANilResult(t *testing.T) {
	var nilResult *struct{}
	for name, value := range map[string]any{"nil": nil, "a nil pointer": nilResult} {
		t.Run(name, func(t *testing.T) {
			op := registerFakeOperator(
				t,
				func(context.Context, bundle.ProviderCall) (any, error) { return value, nil },
			)
			supervisor := &providerSupervisor{rendered: map[string]any{"path": "p"}}

			result := runProvider(
				t,
				supervisor,
				providerDetails(op.classpath, "", literalBinding("path", "p")),
			)

			assertSucceedTask(t, result)
			assert.Empty(t, supervisor.sent("SetXCom"))
		})
	}
}

func TestProviderOperatorPushesAFalseResult(t *testing.T) {
	op := registerFakeOperator(
		t,
		func(context.Context, bundle.ProviderCall) (any, error) { return false, nil },
	)
	supervisor := &providerSupervisor{rendered: map[string]any{"path": "p"}}

	assertSucceedTask(t, runProvider(t, supervisor,
		providerDetails(op.classpath, "", literalBinding("path", "p"))))

	assert.Equal(t, false, supervisor.sent("SetXCom")[0]["value"])
}

func TestProviderOperatorSkipsItsDirectDownstreamTasks(t *testing.T) {
	op := registerFakeOperator(t, func(ctx context.Context, call bundle.ProviderCall) (any, error) {
		return false, call.SkipDownstream(ctx)
	})
	supervisor := &providerSupervisor{rendered: map[string]any{"path": "p"}}

	assertSucceedTask(t, runProvider(t, supervisor,
		providerDetails(op.classpath, "", literalBinding("path", "p"))))

	assert.Equal(
		t,
		[]string{
			"RenderTemplateFields",
			"SetXCom:skipmixin_key",
			"SkipDownstreamTasks",
			"SetXCom:return_value",
		},
		supervisor.types(),
	)
	assert.Equal(t, []any{"after", "later"}, supervisor.sent("SkipDownstreamTasks")[0]["tasks"])
	assert.Equal(
		t,
		map[string]any{"skipped": []any{"after", "later"}},
		supervisor.sent("SetXCom")[0]["value"],
	)
}

func TestProviderOperatorFailureHonorsShouldRetry(t *testing.T) {
	op := registerFakeOperator(t, func(context.Context, bundle.ProviderCall) (any, error) {
		return nil, errors.New("disk full")
	})
	details := providerDetails(op.classpath, "", literalBinding("path", "p"))
	supervisor := &providerSupervisor{rendered: map[string]any{"path": "p"}}

	assertTaskState(t, runProvider(t, supervisor, details), genmodels.TaskStateStateFailed)

	details.TIContext.ShouldRetry = true
	assertRetryTask(t, runProvider(t, supervisor, details), "disk full")
}

func TestProviderOperatorPanicFailsTheTask(t *testing.T) {
	op := registerFakeOperator(
		t,
		func(context.Context, bundle.ProviderCall) (any, error) { panic("boom") },
	)
	details := providerDetails(op.classpath, "", literalBinding("path", "p"))
	details.TIContext.ShouldRetry = true

	assertRetryTask(
		t,
		runProvider(t, &providerSupervisor{rendered: map[string]any{"path": "p"}}, details),
		"panic: boom",
	)
}

func TestProviderOperatorFailsBeforeRunningWhenTheArgumentsAreWrong(t *testing.T) {
	ran := false
	op := registerFakeOperator(t, func(context.Context, bundle.ProviderCall) (any, error) {
		ran = true
		return nil, nil
	})
	tests := map[string]struct {
		kwargs []genmodels.TaskArgBinding
		want   string
	}{
		"a missing required argument": {
			[]genmodels.TaskArgBinding{literalBinding("content", "x")}, `needs the argument "path"`,
		},
		"a result nobody pushed": {
			[]genmodels.TaskArgBinding{xcomBinding("path", "ghost")}, `pushed no return_value XCom`,
		},
		"a malformed binding": {
			[]genmodels.TaskArgBinding{
				map[string]any{"name": "path", "kind": "mystery"},
			}, `unknown kind "mystery"`,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			details := providerDetails(op.classpath, "", tt.kwargs...)
			details.TIContext.ShouldRetry = true

			assertRetryTask(t, runProvider(t, &providerSupervisor{}, details), tt.want)
			assert.False(t, ran)
		})
	}
}

func TestProviderOperatorOfPythonFailsNamingTheQueueSetting(t *testing.T) {
	details := providerDetails(
		"airflow.providers.amazon.aws.operators.s3.S3CreateBucketOperator",
		"",
	)
	details.TIContext.ShouldRetry = true

	assertRetryTask(t, runProvider(t, &providerSupervisor{}, details),
		"[sdk] queue_to_coordinator does not route to the Go coordinator")
}

func TestProviderOperatorOfAnotherLanguageNeedsALoader(t *testing.T) {
	details := providerDetails("airflow.providers.test.operators.Unlinked", "airflow-provider-test")
	details.TIContext.ShouldRetry = true

	assertRetryTask(t, runProvider(t, &providerSupervisor{}, details), "loads no plugins")
}

type fakeLoader struct {
	impl      bundle.ProviderImpl
	err       error
	closed    bool
	classpath string
	module    string
}

func (l *fakeLoader) Load(
	_ context.Context, classpath, module string, _ *slog.Logger,
) (bundle.ProviderImpl, io.Closer, error) {
	l.classpath, l.module = classpath, module
	if l.err != nil {
		return nil, nil, l.err
	}
	return l.impl, closerFunc(func() error { l.closed = true; return nil }), nil
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

func TestProviderOperatorLoadsFromAPluginAndStopsIt(t *testing.T) {
	loader := &fakeLoader{impl: &fakeOperator{
		classpath:      "airflow.providers.test.operators.Remote",
		templateFields: []string{"path"},
		params:         []bundle.ProviderParam{{Name: "path", Required: true}},
		execute:        func(context.Context, bundle.ProviderCall) (any, error) { return "done", nil },
	}}
	bundle.SetProviderLoader(loader)
	t.Cleanup(func() { bundle.SetProviderLoader(nil) })
	supervisor := &providerSupervisor{rendered: map[string]any{"path": "p"}}

	result := runProvider(t, supervisor, providerDetails(
		"airflow.providers.test.operators.Remote",
		"airflow-provider-test",
		literalBinding("path", "p"),
	))

	assertSucceedTask(t, result)
	assert.Equal(t, "airflow-provider-test", loader.module)
	assert.Equal(t, "airflow.providers.test.operators.Remote", loader.classpath)
	assert.True(t, loader.closed, "the plugin stops when the task ends")
	assert.Equal(t, "done", supervisor.sent("SetXCom")[0]["value"])
}

func TestProviderOperatorFailsWhenThePluginCannotLoad(t *testing.T) {
	bundle.SetProviderLoader(&fakeLoader{err: errors.New("the plugin is not in /plugins")})
	t.Cleanup(func() { bundle.SetProviderLoader(nil) })
	details := providerDetails("airflow.providers.test.operators.Remote2", "airflow-provider-test")
	details.TIContext.ShouldRetry = true

	assertRetryTask(
		t,
		runProvider(t, &providerSupervisor{}, details),
		"the plugin is not in /plugins",
	)
}

func TestATaskOfTheBundleStillRunsWithoutAProviderOperator(t *testing.T) {
	b := buildBundle(
		t,
		func(r testBundle) { r.AddDag("test_dag").AddTaskWithName("write", simpleTask) },
	)

	result := RunTask(context.Background(), b, newStartupDetails("write"),
		(&providerSupervisor{}).comm(t), slog.New(slog.NewTextHandler(io.Discard, nil)))

	assertSucceedTask(t, result)
}

// The supervisor sends the provider operator in the run context, and the runtime reads the argument
// bindings out of the generic maps that msgpack decodes.
func TestDecodeStartupDetailsWithAProviderOperator(t *testing.T) {
	raw := marshalBody(t, map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id": "550e8400-e29b-41d4-a716-446655440000", "task_id": "write", "dag_id": "d", "run_id": "r",
			"try_number": 1,
		},
		"ti_context": map[string]any{
			"provider_operator": map[string]any{
				"classpath": "airflow.providers.go.localfile.operators.file.WriteFileOperator",
				"module":    "airflow-provider-go-localfile",
				"kwargs": []any{
					map[string]any{"name": "path", "kind": "literal", "value": "/tmp/{{ run_id }}"},
					map[string]any{"name": "content", "kind": "xcom", "task_id": "build"},
					map[string]any{
						"name": "since", "kind": "literal", "value": "2026-11-20T09:00:00+00:00",
						"value_schema": map[string]any{"type": "string", "format": "date-time"},
					},
				},
				"downstream_task_ids": map[string]any{"write": []any{"read"}, "read": []any{}},
			},
		},
	})

	body, err := decodeIncomingBody(raw)

	require.NoError(t, err)
	provider := body.(*genmodels.StartupDetails).TIContext.ProviderOperator
	require.NotNil(t, provider)
	assert.Equal(t, "airflow-provider-go-localfile", provider.Module)
	assert.Equal(t, []string{"read"}, provider.DownstreamTaskIds["write"])
	args, err := convertArgBindings((*genmodels.ArgBindings)(&provider.Kwargs))
	require.NoError(t, err)
	require.Len(t, args, 3)
	assert.Equal(t, "path", args[0].ArgName())
	assert.Equal(t, "build", args[1].(binding.XComArg).TaskID)
	assert.Equal(t, "date-time", (*args[2].Schema())["format"])
}
