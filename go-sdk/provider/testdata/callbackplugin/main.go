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

// Command callbackplugin is a provider plugin that the tests of package provider start. Its operator
// calls every method of the SDK client of its task, so that a test can check that each one reaches the
// runner, and returns what it saw.
package main

import (
	"errors"
	"log"
	"time"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/provider"
	"github.com/apache/airflow/go-sdk/sdk"
)

type callbackArgs struct {
	Mode airflow.Templated[string] `airflow:"mode,required"`
}

// Callbacks calls the client of the task and returns what came back.
var Callbacks = provider.Define(provider.Meta{
	Module:               "airflow.providers.go.test.operators.callbacks",
	ClassName:            "CallbacksOperator",
	ImplementationModule: "callbackplugin",
}, func(actx airflow.Context, args callbackArgs) (map[string]any, error) {
	client := actx.Client()
	switch args.Mode.Value() {
	case "fail":
		return nil, errors.New("the operator failed")
	case "panic":
		panic("the operator panicked")
	case "skip":
		return nil, provider.SkipDownstream(actx)
	case "context":
		return map[string]any{
			"dag_id":    actx.TaskInstance().DagID,
			"run_id":    actx.DagRun().RunID,
			"try":       actx.TaskInstance().TryNumber,
			"conf":      actx.DagRun().Conf,
			"run_after": actx.DagRun().RunAfter.UTC().Format(time.RFC3339),
		}, nil
	}

	seen := map[string]any{}
	actx.Logger().
		WarnContext(actx, "calling back", "mode", args.Mode.Value(), "n", 2, "err", errors.New("oops"))
	logger := actx.Logger().With("scope", "callbacks").WithGroup("g")
	logger.InfoContext(actx, "grouped", "k", "v")

	variable, err := client.GetVariable(actx, "greeting")
	if err != nil {
		return nil, err
	}
	seen["variable"] = variable
	if _, err := client.GetVariable(actx, "missing"); err != nil {
		seen["missing_variable"] = err.Error()
	}
	if err := client.SetVariable(actx, "written", "value", "a note"); err != nil {
		return nil, err
	}
	var decoded struct{ N int }
	if err := client.UnmarshalJSONVariable(actx, "json", &decoded); err != nil {
		return nil, err
	}
	seen["json_variable"] = decoded.N
	if err := client.DeleteVariable(actx, "written"); err != nil {
		return nil, err
	}

	conn, err := client.GetConnection(actx, "db")
	if err != nil {
		return nil, err
	}
	seen["connection"] = map[string]any{
		"host":  conn.Host,
		"port":  conn.Port,
		"login": *conn.Login,
		"extra": conn.Extra,
	}
	if _, err := client.GetConnection(actx, "missing"); err != nil {
		seen["missing_connection"] = err.Error()
	}

	ti := actx.TaskInstance()
	if err := client.PushXCom(actx, ti, "note", map[string]any{"count": 3, "ratio": 0.5}); err != nil {
		return nil, err
	}
	note, err := client.GetXCom(actx, ti.DagID, ti.RunID, "upstream", nil, "return_value", nil)
	if err != nil {
		return nil, err
	}
	seen["upstream"] = note
	if _, err := client.GetXCom(actx, ti.DagID, ti.RunID, "upstream", nil, "missing", nil); err != nil {
		seen["missing_xcom"] = err.Error()
	}

	store := client.TaskStateStore()
	if err := store.Set(actx, "checkpoint", map[string]any{"page": 4}, sdk.WithRetention(time.Hour)); err != nil {
		return nil, err
	}
	if err := store.Set(actx, "plain", "x"); err != nil {
		return nil, err
	}
	checkpoint, err := store.Get(actx, "checkpoint")
	if err != nil {
		return nil, err
	}
	seen["checkpoint"] = checkpoint
	var typed struct{ Page int }
	if err := store.UnmarshalJSONValue(actx, "checkpoint", &typed); err != nil {
		return nil, err
	}
	seen["typed_checkpoint"] = typed.Page
	if _, err := store.Get(actx, "absent"); err != nil {
		seen["missing_state"] = err.Error()
	}
	if err := store.Delete(actx, "plain"); err != nil {
		return nil, err
	}
	if err := store.Clear(actx); err != nil {
		return nil, err
	}
	return seen, nil
})

func main() {
	if err := provider.Serve(); err != nil {
		log.Fatal(err)
	}
}
