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

// Package clienttest provides an in-memory sdk.Client for tests.
package clienttest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/airflow/go-sdk/sdk"
)

// Client is an sdk.Client that keeps everything in memory. Fill the maps before a test runs.
type Client struct {
	mu sync.Mutex
	// Variables maps a key to its value.
	Variables map[string]string
	// Connections maps a conn_id to its Connection.
	Connections map[string]sdk.Connection
	// XComs maps taskID:key to the value.
	XComs map[string]any
	// State maps a key of the task state store to its value.
	State map[string]any
	calls []string
}

var _ sdk.Client = (*Client)(nil)

// New returns an empty client.
func New() *Client {
	return &Client{
		Variables:   map[string]string{},
		Connections: map[string]sdk.Connection{},
		XComs:       map[string]any{},
		State:       map[string]any{},
	}
}

// Calls returns what was called on the client, in order, as method:argument.
func (c *Client) Calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func (c *Client) record(call string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
}

func (c *Client) GetVariable(_ context.Context, key string) (string, error) {
	c.record("GetVariable:" + key)
	value, ok := c.Variables[key]
	if !ok {
		return "", fmt.Errorf("%w: %q", sdk.VariableNotFound, key)
	}
	return value, nil
}

func (c *Client) UnmarshalJSONVariable(ctx context.Context, key string, pointer any) error {
	value, err := c.GetVariable(ctx, key)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(value), pointer)
}

func (c *Client) SetVariable(_ context.Context, key, value, _ string) error {
	c.record("SetVariable:" + key)
	c.Variables[key] = value
	return nil
}

func (c *Client) DeleteVariable(_ context.Context, key string) error {
	c.record("DeleteVariable:" + key)
	delete(c.Variables, key)
	return nil
}

func (c *Client) GetConnection(_ context.Context, connID string) (sdk.Connection, error) {
	c.record("GetConnection:" + connID)
	conn, ok := c.Connections[connID]
	if !ok {
		return sdk.Connection{}, fmt.Errorf("%w: %q", sdk.ConnectionNotFound, connID)
	}
	return conn, nil
}

func (c *Client) GetXCom(
	_ context.Context, _, _, taskID string, _ *int, key string, _ any,
) (any, error) {
	c.record("GetXCom:" + taskID + ":" + key)
	value, ok := c.XComs[taskID+":"+key]
	if !ok {
		return nil, fmt.Errorf("%w: %q", sdk.XComNotFound, key)
	}
	return value, nil
}

func (c *Client) PushXCom(_ context.Context, ti sdk.TaskInstance, key string, value any) error {
	c.record("PushXCom:" + ti.TaskID + ":" + key)
	c.XComs[ti.TaskID+":"+key] = value
	return nil
}

func (c *Client) TaskStateStore() sdk.TaskStateStore { return stateStore{c} }

type stateStore struct{ client *Client }

func (s stateStore) Get(_ context.Context, key string) (any, error) {
	s.client.record("StateGet:" + key)
	value, ok := s.client.State[key]
	if !ok {
		return nil, fmt.Errorf("%w: %q", sdk.TaskStateNotFound, key)
	}
	return value, nil
}

func (s stateStore) UnmarshalJSONValue(ctx context.Context, key string, pointer any) error {
	value, err := s.Get(ctx, key)
	if err != nil {
		return err
	}
	data, _ := json.Marshal(value)
	return json.Unmarshal(data, pointer)
}

func (s stateStore) Set(_ context.Context, key string, value any, opts ...sdk.SetOption) error {
	var options sdk.SetOptions
	for _, opt := range opts {
		opt(&options)
	}
	call := "StateSet:" + key
	if options.Retention != nil {
		call += ":" + options.Retention.String()
	}
	s.client.record(call)
	s.client.State[key] = value
	return nil
}

func (s stateStore) Delete(_ context.Context, key string) error {
	s.client.record("StateDelete:" + key)
	delete(s.client.State, key)
	return nil
}

func (s stateStore) Clear(context.Context) error {
	s.client.record("StateClear")
	clear(s.client.State)
	return nil
}
