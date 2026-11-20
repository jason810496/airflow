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
	"net/rpc"
	"reflect"

	"github.com/hashicorp/go-plugin"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// Client is the runner's side of a plugin.
type Client struct {
	broker *plugin.MuxBroker
	rpc    *rpc.Client
}

// NewClient returns the client of the plugin that rpcClient reaches.
func NewClient(broker *plugin.MuxBroker, rpcClient *rpc.Client) *Client {
	return &Client{broker: broker, rpc: rpcClient}
}

// Describe asks the plugin how to resolve the arguments of an operator, which also checks that the
// plugin implements it.
func (c *Client) Describe(classpath string) (DescribeReply, error) {
	var reply DescribeReply
	err := c.rpc.Call("Plugin.Describe", DescribeRequest{Classpath: classpath}, &reply)
	return reply, err
}

// Execute runs an operator in the plugin, and serves it the callbacks of the task meanwhile. It
// returns when the operator does, or when ctx ends, which leaves the plugin running: the caller stops
// the plugin.
func (c *Client) Execute(
	ctx context.Context,
	classpath string,
	call bundle.ProviderCall,
) (any, error) {
	req := ExecuteRequest{Classpath: classpath}
	var err error
	if req.Args, err = json.Marshal(call.Args); err != nil {
		return nil, fmt.Errorf("the arguments of %s are not JSON: %w", classpath, err)
	}
	if req.TaskInstance, err = json.Marshal(call.TaskInstance); err != nil {
		return nil, err
	}
	if req.DagRun, err = json.Marshal(call.DagRun); err != nil {
		return nil, err
	}

	req.CallbackID = c.broker.NextId()
	go c.broker.AcceptAndServe(req.CallbackID, &Callbacks{
		Ctx: ctx, Client: call.Client, Logger: call.Logger, Skip: call.SkipDownstream,
	})

	var reply ExecuteReply
	rpcCall := c.rpc.Go("Plugin.Execute", req, &reply, make(chan *rpc.Call, 1))
	select {
	case <-rpcCall.Done:
		if rpcCall.Error != nil {
			return nil, rpcCall.Error
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if !reply.HasResult {
		return nil, nil
	}
	return decodeJSON(reply.Result)
}

// hasResult reports whether an operator returned something to push as its return_value XCom.
func hasResult(value any) bool {
	if value == nil {
		return false
	}
	v := reflect.ValueOf(value)
	return v.Kind() != reflect.Pointer || !v.IsNil()
}

// Impl is an operator that a plugin implements, as the runtime runs it.
type Impl struct {
	client    *Client
	classpath string
	described DescribeReply
}

var _ bundle.ProviderImpl = (*Impl)(nil)

// NewImpl returns the operator that classpath names, which the plugin behind client implements.
func NewImpl(client *Client, classpath string) (*Impl, error) {
	described, err := client.Describe(classpath)
	if err != nil {
		return nil, err
	}
	return &Impl{client: client, classpath: classpath, described: described}, nil
}

func (i *Impl) Classpath() string { return i.classpath }

func (i *Impl) TemplateFields() []string { return i.described.TemplateFields }

func (i *Impl) Params() []bundle.ProviderParam {
	params := make([]bundle.ProviderParam, len(i.described.Params))
	for n, param := range i.described.Params {
		params[n] = bundle.ProviderParam{Name: param.Name, Required: param.Required}
	}
	return params
}

func (i *Impl) Execute(ctx context.Context, call bundle.ProviderCall) (any, error) {
	return i.client.Execute(ctx, i.classpath, call)
}
