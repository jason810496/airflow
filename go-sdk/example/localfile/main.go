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

// Package main is an example of a native Go Dag that uses the operators of the go.localfile provider,
// implemented in Go, and runs them from its own executable:
//
//	build_content -> create -> write -> has_file -> delete -> has_after_delete -> after_skip
//
// has_after_delete finds the file gone and skips its direct downstream task, after_skip. A second
// root task, write_variable, reads an Airflow Variable through the SDK client of its task and writes
// it to a file.
//
// The executable links the provider package, so the Go runtime finds each operator by its class path
// in the process. A Python Dag that uses the generated wrapper airflow.providers.go.localfile has no
// executable of the provider, so the generic provider runner loads the plugin instead.
//
// Pack it with `go tool airflow-go-pack ./example/localfile` and put the result in a Dag bundle.
package main

import (
	"log"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/providers/localfile"
)

const (
	// reportPath is rendered by the supervisor, as it renders the templates of a Python Dag.
	reportPath   = "/tmp/airflow-localfile/go_localfile/{{ run_id }}/report.txt"
	variablePath = "/tmp/airflow-localfile/go_localfile/{{ run_id }}/variable.txt"
	// variableKey is the Airflow Variable that write_variable reads.
	variableKey = "go_localfile_variable"
)

func main() {
	bundle := airflow.Bundle()
	bundle.Register(newLocalfileDag())

	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}

func newLocalfileDag() *airflow.DagRef {
	// The Dag's queue is the Go coordinator's, and so is the queue of every provider task of it.
	dag := airflow.Dag("go_localfile", airflow.DagSpec{
		Queue: "golang",
		Tags:  []string{"go", "providers", "go.localfile"},
	})

	content := dag.Task(buildContent, airflow.TaskSpec{TaskID: "build_content"})
	create := dag.Task(
		localfile.CreateFileOperator(
			localfile.CreateFileOperatorArgs{Path: airflow.Of(reportPath)},
		),
		airflow.TaskSpec{TaskID: "create"},
	)
	write := dag.Task(
		localfile.WriteFileOperator(localfile.WriteFileOperatorArgs{
			Path: airflow.Of(reportPath),
			// The result of build_content, which the runtime pulls and does not render.
			Content: airflow.ResultOf[string](content),
		}),
		airflow.TaskSpec{TaskID: "write"},
	)
	hasFile := dag.Task(
		localfile.HasFileOperator(localfile.HasFileOperatorArgs{Path: airflow.Of(reportPath)}),
		airflow.TaskSpec{TaskID: "has_file"},
	)
	remove := dag.Task(
		localfile.DeleteFileOperator(
			localfile.DeleteFileOperatorArgs{Path: airflow.Of(reportPath)},
		),
		airflow.TaskSpec{TaskID: "delete"},
	)
	hasAfterDelete := dag.Task(
		localfile.HasFileOperator(localfile.HasFileOperatorArgs{
			Path:                    airflow.Of(reportPath),
			SkipDownstreamIfMissing: airflow.Ptr(true),
		}),
		airflow.TaskSpec{TaskID: "has_after_delete"},
	)
	afterSkip := dag.Task(unreachable, airflow.TaskSpec{TaskID: "after_skip"})

	create.After(content)
	write.After(create)
	hasFile.After(write)
	remove.After(hasFile)
	hasAfterDelete.After(remove)
	afterSkip.After(hasAfterDelete)

	dag.Task(
		localfile.WriteVariableOperator(localfile.WriteVariableOperatorArgs{
			Path: airflow.Of(variablePath),
			Key:  airflow.Of(variableKey),
		}),
		airflow.TaskSpec{TaskID: "write_variable"},
	)
	return dag
}

func buildContent(airflow.Context) (string, error) {
	return "localfile report\nnaïve café, 3 orders\n", nil
}

func unreachable(airflow.Context) (string, error) { return "unreachable", nil }
