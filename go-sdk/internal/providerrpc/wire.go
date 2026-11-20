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

// Package providerrpc is the net/rpc protocol between the generic provider runner and a provider
// plugin, which hashicorp/go-plugin carries.
//
// The runner calls Describe and Execute on the plugin. The plugin has no supervisor socket, so
// Execute passes a callback stream that the runner serves with the SDK client of the task: the plugin
// reads Variables, Connections, XComs and the task state store, writes its logs, and skips downstream
// tasks through it. Every payload that holds an arbitrary value crosses as JSON bytes, because gob,
// which net/rpc uses, cannot carry a map[string]any whose values are not registered.
package providerrpc

// PluginName is the name under which the plugin serves its operators.
const PluginName = "operators"

// ParamWire is one constructor argument of an operator.
type ParamWire struct {
	Name     string
	Required bool
}

// DescribeRequest asks a plugin about one operator.
type DescribeRequest struct{ Classpath string }

// DescribeReply tells how the runner resolves the arguments of an operator.
type DescribeReply struct {
	TemplateFields []string
	Params         []ParamWire
}

// ExecuteRequest asks a plugin to run an operator.
type ExecuteRequest struct {
	Classpath string
	// Args, TaskInstance and DagRun are JSON.
	Args         []byte
	TaskInstance []byte
	DagRun       []byte
	// CallbackID is the stream of the runner's callback service, which the plugin dials.
	CallbackID uint32
}

// ExecuteReply holds what an operator returned.
type ExecuteReply struct {
	// HasResult is false when the operator returned nothing to push.
	HasResult bool
	// Result is JSON.
	Result []byte
}

// KeyRequest names a Variable, an XCom key, a task state key or a connection.
type KeyRequest struct{ Key string }

// StringReply holds a string, or says that there is none.
type StringReply struct {
	Found bool
	Value string
}

// JSONReply holds a JSON value, or says that there is none.
type JSONReply struct {
	Found bool
	Data  []byte
}

// SetVariableRequest stores a Variable.
type SetVariableRequest struct{ Key, Value, Description string }

// GetXComRequest names an XCom.
type GetXComRequest struct {
	DagID, RunID, TaskID string
	MapIndex             *int
	Key                  string
}

// PushXComRequest stores an XCom of the running task, or of another task instance.
type PushXComRequest struct {
	DagID, RunID, TaskID string
	MapIndex             *int
	Key                  string
	// Value is JSON.
	Value []byte
}

// StateSetRequest stores a value in the task state store.
type StateSetRequest struct {
	Key string
	// Value is JSON.
	Value        []byte
	HasRetention bool
	// Retention is nanoseconds, or math.MaxInt64 for sdk.NeverExpire.
	Retention int64
}

// LogRequest is one log record of an operator.
type LogRequest struct {
	Level   int
	Message string
	// Attrs is a JSON object.
	Attrs []byte
}
