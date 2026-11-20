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
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// startupFrameID is the id of the frame that carries the StartupDetails. The runner answers it with the
// terminal frame of the task. The ids of the runner's own requests count up from 1, so it is far from them.
const startupFrameID = 9999

func build(t *testing.T, output, pkg string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", output, pkg)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}

func writeFrame(t *testing.T, w io.Writer, elements ...any) {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)
	require.NoError(t, enc.EncodeArrayLen(len(elements)))
	for _, element := range elements {
		require.NoError(t, enc.Encode(element))
	}
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, uint32(buf.Len()))
	_, err := w.Write(append(prefix, buf.Bytes()...))
	require.NoError(t, err)
}

// readFrame reads one frame from the runner, which is [id, body].
func readFrame(r io.Reader) (int64, map[string]any, error) {
	prefix := make([]byte, 4)
	if _, err := io.ReadFull(r, prefix); err != nil {
		return 0, nil, err
	}
	payload := make([]byte, binary.BigEndian.Uint32(prefix))
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	dec := msgpack.NewDecoder(bytes.NewReader(payload))
	if _, err := dec.DecodeArrayLen(); err != nil {
		return 0, nil, err
	}
	id, err := dec.DecodeInt64()
	if err != nil {
		return 0, nil, err
	}
	var body map[string]any
	return id, body, dec.Decode(&body)
}

type supervisor struct {
	t        *testing.T
	requests []map[string]any
	// variables answers GetVariable.
	variables map[string]string
}

func (s *supervisor) answer(conn net.Conn, id int64, req map[string]any) {
	s.requests = append(s.requests, req)
	switch req["type"] {
	case "RenderTemplateFields":
		// The supervisor renders what it is sent. This one renders {{ run_id }} only.
		values := map[string]any{}
		for name, value := range req["values"].(map[string]any) {
			if text, ok := value.(string); ok {
				values[name] = replaceRunID(text)
			} else {
				values[name] = value
			}
		}
		writeFrame(
			s.t,
			conn,
			id,
			map[string]any{"type": "RenderedTemplateFieldsResult", "values": values},
			nil,
		)
	case "GetVariable":
		key := req["key"].(string)
		value, ok := s.variables[key]
		if !ok {
			writeFrame(
				s.t,
				conn,
				id,
				nil,
				map[string]any{
					"type":   "ErrorResponse",
					"error":  "VARIABLE_NOT_FOUND",
					"detail": map[string]any{},
				},
			)
			return
		}
		writeFrame(
			s.t,
			conn,
			id,
			map[string]any{"type": "VariableResult", "key": key, "value": value},
			nil,
		)
	default:
		writeFrame(s.t, conn, id, nil, nil)
	}
}

func replaceRunID(text string) string {
	return string(bytes.ReplaceAll([]byte(text), []byte("{{ run_id }}"), []byte("run1")))
}

func (s *supervisor) request(kind string) map[string]any {
	for _, req := range s.requests {
		if req["type"] == kind {
			return req
		}
	}
	return nil
}

// runRunner starts the runner on the given listeners, sends it the start-up details of a provider
// operator task, answers its requests, and returns the terminal body of the task.
func runRunner(
	t *testing.T, runner, pluginsDir string, sup *supervisor, provider map[string]any,
) map[string]any {
	t.Helper()
	comm, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer comm.Close()
	logs, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer logs.Close()

	cmd := exec.Command(runner, "--plugins-dir="+pluginsDir,
		"--comm="+comm.Addr().String(), "--logs="+logs.Addr().String())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	accept := func(l net.Listener) net.Conn {
		_ = l.(*net.TCPListener).SetDeadline(time.Now().Add(30 * time.Second))
		conn, err := l.Accept()
		require.NoError(t, err, stderr.String())
		return conn
	}
	commConn, logsConn := accept(comm), accept(logs)
	defer commConn.Close()
	go func() { _, _ = io.Copy(io.Discard, logsConn) }()

	now := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	writeFrame(t, commConn, startupFrameID, map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id": "550e8400-e29b-41d4-a716-446655440000", "task_id": "write_variable",
			"dag_id": "python_localfile", "run_id": "run1", "try_number": 1, "map_index": -1,
		},
		"dag_rel_path": "dags/python_localfile.py",
		"bundle_info":  map[string]any{"name": "dags-folder", "version": "v1"},
		"start_date":   now,
		"ti_context": map[string]any{
			"dag_run": map[string]any{
				"dag_id": "python_localfile", "run_id": "run1", "run_after": now, "run_type": "manual",
			},
			"max_tries":         0,
			"provider_operator": provider,
		},
	})

	_ = commConn.SetReadDeadline(time.Now().Add(60 * time.Second))
	for {
		id, body, err := readFrame(commConn)
		require.NoError(t, err, stderr.String())
		if id == startupFrameID {
			require.NoError(t, <-exited, stderr.String())
			return body
		}
		sup.answer(commConn, id, body)
	}
}

func TestTheRunnerRunsAnOperatorInAPluginAndCallsBackToAirflow(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the runner and a plugin")
	}
	dir := t.TempDir()
	plugins := filepath.Join(dir, "plugins")
	require.NoError(t, os.Mkdir(plugins, 0o755))
	runner := filepath.Join(dir, "airflow-provider-runner")
	build(t, runner, ".")
	build(t, filepath.Join(plugins, "airflow-provider-go-localfile"),
		"../../providers/localfile/cmd/airflow-provider-go-localfile")
	target := filepath.Join(dir, "out", "{{ run_id }}", "variable.txt")
	sup := &supervisor{
		t:         t,
		variables: map[string]string{"go_localfile_variable": "from airflow\n"},
	}

	terminal := runRunner(t, runner, plugins, sup, map[string]any{
		"classpath": "airflow.providers.go.localfile.operators.file.WriteVariableOperator",
		"module":    "airflow-provider-go-localfile",
		"kwargs": []any{
			map[string]any{"name": "path", "kind": "literal", "value": target},
			map[string]any{"name": "key", "kind": "literal", "value": "go_localfile_variable"},
			map[string]any{"name": "queue", "kind": "literal", "value": "golang"},
		},
		"downstream_task_ids": map[string]any{"write_variable": []any{}},
	})

	assert.Equal(t, "SucceedTask", terminal["type"], "%v", terminal)
	written, err := os.ReadFile(filepath.Join(dir, "out", "run1", "variable.txt"))
	require.NoError(t, err, "the plugin wrote the rendered path")
	assert.Equal(
		t,
		"from airflow\n",
		string(written),
		"the plugin read the Variable through the runner",
	)
	assert.Equal(t, "go_localfile_variable", sup.request("GetVariable")["key"])
	render := sup.request("RenderTemplateFields")
	require.NotNil(t, render)
	assert.Equal(
		t,
		map[string]any{"path": target, "key": "go_localfile_variable"},
		render["values"],
	)
	pushed := sup.request("SetXCom")
	require.NotNil(t, pushed)
	assert.Equal(t, "return_value", pushed["key"])
	assert.Equal(t, "write_variable", pushed["task_id"])
}

func TestTheRunnerFailsATaskWhoseOperatorIsImplementedInPython(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the runner")
	}
	runner := filepath.Join(t.TempDir(), "airflow-provider-runner")
	build(t, runner, ".")

	terminal := runRunner(t, runner, t.TempDir(), &supervisor{t: t}, map[string]any{
		"classpath":           "airflow.providers.amazon.aws.operators.s3.S3CreateBucketOperator",
		"kwargs":              []any{},
		"downstream_task_ids": map[string]any{"write_variable": []any{}},
	})

	assert.Equal(t, "TaskState", terminal["type"], "%v", terminal)
	assert.Equal(t, "failed", terminal["state"])
}

func TestTheRunnerPrintsItsSupervisorSchemaVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the runner")
	}
	runner := filepath.Join(t.TempDir(), "airflow-provider-runner")
	build(t, runner, ".")

	out, err := exec.Command(runner, "--plugins-dir=/unused", "--airflow-metadata", "--format=json").
		Output()

	require.NoError(t, err)
	assert.Contains(t, string(out), `"supervisor_schema_version": "20`)
}
