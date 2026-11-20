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

// Command airflow-provider-runner is the generic Go runtime for provider operators. The Go coordinator
// starts it for a task whose stub_kind is provider_operator, unless the task's Dag file is a Go Dag,
// which links the operators it uses.
//
// It serves an empty bundle, so it runs no Dag of its own. It learns which operator to run from
// ti_context.provider_operator in the start-up details, resolves the arguments, has the supervisor
// render the template fields, and runs the operator in the plugin executable that the task's module
// names, from --plugins-dir. The operator calls back to Airflow through the runner.
//
// The coordinator starts it from its go_providers_path setting:
//
//	<go_providers_path>/airflow-provider-runner --plugins-dir=<go_providers_path>/plugins \
//		--comm=host:port --logs=host:port
//
// Install it, and the plugins of the providers a deployment uses, with:
//
//	GOBIN=<go_providers_path> go install \
//		github.com/apache/airflow/go-sdk/cmd/airflow-provider-runner
//	GOBIN=<go_providers_path>/plugins go install \
//		github.com/apache/airflow/go-sdk/providers/localfile/cmd/airflow-provider-go-localfile
//
// With --airflow-metadata it prints the manifest that holds the supervisor schema version, which the
// coordinator reads before it starts a task.
package main

import (
	"log"

	flag "github.com/spf13/pflag"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/provider"
)

func main() {
	pluginsDir := flag.String(
		"plugins-dir",
		"",
		"directory of the provider plugin executables, which tasks name by file name",
	)
	provider.UsePlugins(func() string { return *pluginsDir })

	if err := airflow.Bundle().Serve(); err != nil {
		log.Fatal(err)
	}
}
