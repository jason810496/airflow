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
	"fmt"
	"log/slog"
	"net/rpc"
	"runtime/debug"

	"github.com/hashicorp/go-plugin"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// Server is the RPC object that a plugin serves. go-plugin registers it under the name Plugin.
type Server struct {
	broker *plugin.MuxBroker
	impls  map[string]bundle.ProviderImpl
}

// NewServer returns the server of a plugin that runs impls.
func NewServer(broker *plugin.MuxBroker, impls []bundle.ProviderImpl) *Server {
	byClasspath := make(map[string]bundle.ProviderImpl, len(impls))
	for _, impl := range impls {
		byClasspath[impl.Classpath()] = impl
	}
	return &Server{broker: broker, impls: byClasspath}
}

func (s *Server) find(classpath string) (bundle.ProviderImpl, error) {
	impl, ok := s.impls[classpath]
	if !ok {
		return nil, fmt.Errorf("the plugin does not implement %s", classpath)
	}
	return impl, nil
}

// Describe tells how to resolve the arguments of an operator.
func (s *Server) Describe(req DescribeRequest, reply *DescribeReply) error {
	impl, err := s.find(req.Classpath)
	if err != nil {
		return err
	}
	reply.TemplateFields = impl.TemplateFields()
	for _, param := range impl.Params() {
		reply.Params = append(reply.Params, ParamWire{Name: param.Name, Required: param.Required})
	}
	return nil
}

// Execute runs an operator.
func (s *Server) Execute(req ExecuteRequest, reply *ExecuteReply) (err error) {
	impl, err := s.find(req.Classpath)
	if err != nil {
		return err
	}
	call, err := decodeCall(req)
	if err != nil {
		return err
	}
	stream, err := s.broker.Dial(req.CallbackID)
	if err != nil {
		return fmt.Errorf("connecting to the callback service: %w", err)
	}
	callbacks := rpc.NewClient(stream)
	defer callbacks.Close()
	client := &callbackClient{rpc: callbacks}
	call.Client = client
	call.Logger = slog.New(logHandler{client: client})
	call.SkipDownstream = client.skipDownstream

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	value, err := impl.Execute(context.Background(), call)
	if err != nil {
		return err
	}
	if !hasResult(value) {
		return nil
	}
	if reply.Result, err = json.Marshal(value); err != nil {
		return fmt.Errorf("the result of %s is not JSON: %w", req.Classpath, err)
	}
	reply.HasResult = true
	return nil
}

func decodeCall(req ExecuteRequest) (bundle.ProviderCall, error) {
	var call bundle.ProviderCall
	args, err := decodeJSON(req.Args)
	if err != nil {
		return call, fmt.Errorf("decoding the arguments: %w", err)
	}
	if call.Args, _ = args.(map[string]any); call.Args == nil {
		call.Args = map[string]any{}
	}
	if err := json.Unmarshal(req.TaskInstance, &call.TaskInstance); err != nil {
		return call, fmt.Errorf("decoding the task instance: %w", err)
	}
	if err := json.Unmarshal(req.DagRun, &call.DagRun); err != nil {
		return call, fmt.Errorf("decoding the Dag run: %w", err)
	}
	if call.DagRun.Conf != nil {
		call.DagRun.Conf, _ = normalizeNumbers(call.DagRun.Conf).(map[string]any)
	}
	return call, nil
}
