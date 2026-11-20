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

package providerrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/rpc"
	"time"

	"github.com/apache/airflow/go-sdk/sdk"
)

// Callbacks is the callback service that the runner serves to a plugin for the length of one task. It
// forwards each call to the SDK client of the task, so an operator in a plugin reads and writes what an
// operator linked into a Dag executable does.
type Callbacks struct {
	Ctx    context.Context
	Client sdk.Client
	Logger *slog.Logger
	Skip   func(ctx context.Context) error
}

// GetVariable reads a Variable.
func (c *Callbacks) GetVariable(req KeyRequest, reply *StringReply) error {
	value, err := c.Client.GetVariable(c.Ctx, req.Key)
	switch {
	case errors.Is(err, sdk.VariableNotFound):
		*reply = StringReply{}
		return nil
	case err != nil:
		return err
	}
	*reply = StringReply{Found: true, Value: value}
	return nil
}

// SetVariable stores a Variable.
func (c *Callbacks) SetVariable(req SetVariableRequest, reply *bool) error {
	*reply = true
	return c.Client.SetVariable(c.Ctx, req.Key, req.Value, req.Description)
}

// DeleteVariable removes a Variable.
func (c *Callbacks) DeleteVariable(req KeyRequest, reply *bool) error {
	*reply = true
	return c.Client.DeleteVariable(c.Ctx, req.Key)
}

// GetConnection reads a Connection.
func (c *Callbacks) GetConnection(req KeyRequest, reply *JSONReply) error {
	conn, err := c.Client.GetConnection(c.Ctx, req.Key)
	switch {
	case errors.Is(err, sdk.ConnectionNotFound):
		*reply = JSONReply{}
		return nil
	case err != nil:
		return err
	}
	data, err := json.Marshal(conn)
	if err != nil {
		return err
	}
	*reply = JSONReply{Found: true, Data: data}
	return nil
}

// GetXCom reads an XCom.
func (c *Callbacks) GetXCom(req GetXComRequest, reply *JSONReply) error {
	value, err := c.Client.GetXCom(
		c.Ctx,
		req.DagID,
		req.RunID,
		req.TaskID,
		req.MapIndex,
		req.Key,
		nil,
	)
	switch {
	case errors.Is(err, sdk.XComNotFound):
		*reply = JSONReply{}
		return nil
	case err != nil:
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	*reply = JSONReply{Found: true, Data: data}
	return nil
}

// PushXCom stores an XCom.
func (c *Callbacks) PushXCom(req PushXComRequest, reply *bool) error {
	value, err := decodeJSON(req.Value)
	if err != nil {
		return err
	}
	ti := sdk.TaskInstance{
		DagID:    req.DagID,
		RunID:    req.RunID,
		TaskID:   req.TaskID,
		MapIndex: req.MapIndex,
	}
	*reply = true
	return c.Client.PushXCom(c.Ctx, ti, req.Key, value)
}

// StateGet reads a value of the task state store.
func (c *Callbacks) StateGet(req KeyRequest, reply *JSONReply) error {
	value, err := c.Client.TaskStateStore().Get(c.Ctx, req.Key)
	switch {
	case errors.Is(err, sdk.TaskStateNotFound):
		*reply = JSONReply{}
		return nil
	case err != nil:
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	*reply = JSONReply{Found: true, Data: data}
	return nil
}

// StateSet stores a value in the task state store.
func (c *Callbacks) StateSet(req StateSetRequest, reply *bool) error {
	value, err := decodeJSON(req.Value)
	if err != nil {
		return err
	}
	var opts []sdk.SetOption
	if req.HasRetention {
		opts = append(opts, sdk.WithRetention(time.Duration(req.Retention)))
	}
	*reply = true
	return c.Client.TaskStateStore().Set(c.Ctx, req.Key, value, opts...)
}

// StateDelete removes a value of the task state store.
func (c *Callbacks) StateDelete(req KeyRequest, reply *bool) error {
	*reply = true
	return c.Client.TaskStateStore().Delete(c.Ctx, req.Key)
}

// StateClear removes every value of the task state store.
func (c *Callbacks) StateClear(_ bool, reply *bool) error {
	*reply = true
	return c.Client.TaskStateStore().Clear(c.Ctx)
}

// SkipDownstream skips the direct downstream tasks of the running task.
func (c *Callbacks) SkipDownstream(_ bool, reply *bool) error {
	*reply = true
	return c.Skip(c.Ctx)
}

// Log writes a record of an operator to the task's log.
func (c *Callbacks) Log(req LogRequest, reply *bool) error {
	var attrs map[string]any
	if len(req.Attrs) > 0 {
		decoded, err := decodeJSON(req.Attrs)
		if err != nil {
			return err
		}
		attrs, _ = decoded.(map[string]any)
	}
	args := make([]any, 0, 2*len(attrs))
	for key, value := range attrs {
		args = append(args, key, value)
	}
	c.Logger.Log(c.Ctx, slog.Level(req.Level), req.Message, args...)
	*reply = true
	return nil
}

// callbackClient is the sdk.Client of an operator that runs in a plugin. It calls the runner's
// Callbacks.
type callbackClient struct {
	rpc *rpc.Client
}

var _ sdk.Client = (*callbackClient)(nil)

func (c *callbackClient) call(method string, args, reply any) error {
	return c.rpc.Call("Plugin."+method, args, reply)
}

func (c *callbackClient) GetVariable(_ context.Context, key string) (string, error) {
	var reply StringReply
	if err := c.call("GetVariable", KeyRequest{Key: key}, &reply); err != nil {
		return "", err
	}
	if !reply.Found {
		return "", fmt.Errorf("%w: %q", sdk.VariableNotFound, key)
	}
	return reply.Value, nil
}

func (c *callbackClient) UnmarshalJSONVariable(ctx context.Context, key string, pointer any) error {
	value, err := c.GetVariable(ctx, key)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(value), pointer)
}

func (c *callbackClient) SetVariable(_ context.Context, key, value, description string) error {
	var reply bool
	return c.call(
		"SetVariable",
		SetVariableRequest{Key: key, Value: value, Description: description},
		&reply,
	)
}

func (c *callbackClient) DeleteVariable(_ context.Context, key string) error {
	var reply bool
	return c.call("DeleteVariable", KeyRequest{Key: key}, &reply)
}

func (c *callbackClient) GetConnection(_ context.Context, connID string) (sdk.Connection, error) {
	var reply JSONReply
	if err := c.call("GetConnection", KeyRequest{Key: connID}, &reply); err != nil {
		return sdk.Connection{}, err
	}
	if !reply.Found {
		return sdk.Connection{}, fmt.Errorf("%w: %q", sdk.ConnectionNotFound, connID)
	}
	var conn sdk.Connection
	if err := json.Unmarshal(reply.Data, &conn); err != nil {
		return sdk.Connection{}, fmt.Errorf("decoding connection: %w", err)
	}
	return conn, nil
}

func (c *callbackClient) GetXCom(
	_ context.Context,
	dagID, runID, taskID string,
	mapIndex *int,
	key string,
	_ any,
) (any, error) {
	var reply JSONReply
	req := GetXComRequest{DagID: dagID, RunID: runID, TaskID: taskID, MapIndex: mapIndex, Key: key}
	if err := c.call("GetXCom", req, &reply); err != nil {
		return nil, err
	}
	if !reply.Found {
		return nil, fmt.Errorf("%w: %q", sdk.XComNotFound, key)
	}
	return decodeJSON(reply.Data)
}

func (c *callbackClient) PushXCom(
	_ context.Context,
	ti sdk.TaskInstance,
	key string,
	value any,
) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("cannot push XCom %q: %w", key, err)
	}
	var reply bool
	req := PushXComRequest{
		DagID: ti.DagID, RunID: ti.RunID, TaskID: ti.TaskID, MapIndex: ti.MapIndex, Key: key, Value: data,
	}
	return c.call("PushXCom", req, &reply)
}

func (c *callbackClient) TaskStateStore() sdk.TaskStateStore { return callbackStateStore{c} }

func (c *callbackClient) skipDownstream(context.Context) error {
	var reply bool
	return c.call("SkipDownstream", true, &reply)
}

func (c *callbackClient) log(level slog.Level, message string, attrs map[string]any) {
	data, err := json.Marshal(attrs)
	if err != nil {
		data = nil
	}
	var reply bool
	_ = c.call("Log", LogRequest{Level: int(level), Message: message, Attrs: data}, &reply)
}

type callbackStateStore struct{ client *callbackClient }

var _ sdk.TaskStateStore = callbackStateStore{}

func (s callbackStateStore) Get(_ context.Context, key string) (any, error) {
	var reply JSONReply
	if err := s.client.call("StateGet", KeyRequest{Key: key}, &reply); err != nil {
		return nil, err
	}
	if !reply.Found {
		return nil, fmt.Errorf("%w: %q", sdk.TaskStateNotFound, key)
	}
	return decodeJSON(reply.Data)
}

func (s callbackStateStore) UnmarshalJSONValue(ctx context.Context, key string, pointer any) error {
	value, err := s.Get(ctx, key)
	if err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, pointer)
}

func (s callbackStateStore) Set(
	_ context.Context,
	key string,
	value any,
	opts ...sdk.SetOption,
) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("cannot set task state key %q: %w", key, err)
	}
	var options sdk.SetOptions
	for _, opt := range opts {
		opt(&options)
	}
	req := StateSetRequest{Key: key, Value: data}
	if options.Retention != nil {
		req.HasRetention, req.Retention = true, int64(*options.Retention)
	}
	var reply bool
	return s.client.call("StateSet", req, &reply)
}

func (s callbackStateStore) Delete(_ context.Context, key string) error {
	var reply bool
	return s.client.call("StateDelete", KeyRequest{Key: key}, &reply)
}

func (s callbackStateStore) Clear(context.Context) error {
	var reply bool
	return s.client.call("StateClear", true, &reply)
}

// logHandler is the slog.Handler of an operator in a plugin. It sends each record to the runner, which
// writes it to the task's log.
type logHandler struct {
	client *callbackClient
	attrs  map[string]any
	prefix string
}

func (h logHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h logHandler) Handle(_ context.Context, record slog.Record) error {
	attrs := make(map[string]any, len(h.attrs)+record.NumAttrs())
	for key, value := range h.attrs {
		attrs[key] = value
	}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[h.prefix+attr.Key] = attrValue(attr.Value)
		return true
	})
	h.client.log(record.Level, record.Message, attrs)
	return nil
}

func (h logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := make(map[string]any, len(h.attrs)+len(attrs))
	for key, value := range h.attrs {
		merged[key] = value
	}
	for _, attr := range attrs {
		merged[h.prefix+attr.Key] = attrValue(attr.Value)
	}
	h.attrs = merged
	return h
}

func (h logHandler) WithGroup(name string) slog.Handler {
	if name != "" {
		h.prefix += name + "."
	}
	return h
}

// attrValue returns the value of a log attribute in a form that JSON keeps: an error is its message.
func attrValue(value slog.Value) any {
	value = value.Resolve()
	switch value.Kind() {
	case slog.KindGroup:
		group := map[string]any{}
		for _, attr := range value.Group() {
			group[attr.Key] = attrValue(attr.Value)
		}
		return group
	case slog.KindAny:
		if err, ok := value.Any().(error); ok {
			return err.Error()
		}
	}
	return value.Any()
}
