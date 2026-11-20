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

// Package main is an example of a native Go Dag that mixes its own tasks with provider operators that
// Python implements:
//
//	build_report -> create_bucket -> write_report -> wait_for_report -> run_bash -> summarize
//
// build_report and summarize run in Go. The other tasks are amazon and standard operators, declared
// through the generated packages under providers/: each wrapper writes the Python operator's class and
// arguments into the serialized Dag, and a Python worker that has the provider installed builds and
// runs the operator. Values flow both ways. The report that a Go task builds is what the S3 operator
// writes, and what the Bash operator prints is what the last Go task receives.
//
// A provider task takes the Dag's queue like any other task. The Dag's golang queue routes to the Go
// coordinator, so each of the Python tasks sets a queue that a Python worker serves.
//
// Pack it with `go tool airflow-go-pack ./example/providers` and put the result in a Dag bundle.
package main

import (
	"fmt"
	"log"

	"github.com/apache/airflow/go-sdk/airflow"
	s3operators "github.com/apache/airflow/go-sdk/providers/amazon/aws/operators/s3"
	s3sensors "github.com/apache/airflow/go-sdk/providers/amazon/aws/sensors/s3"
	bashoperators "github.com/apache/airflow/go-sdk/providers/standard/operators/bash"
)

const (
	// pythonQueue is a queue that Python workers serve, for the tasks that a Python worker runs.
	pythonQueue = "default"
	bucket      = "go-provider-operators"
	// reportKey is rendered by the Python worker, as it renders the templates of a Python Dag.
	reportKey = "runs/{{ run_id }}/report.json"
	awsConnID = "aws_localstack"
)

func main() {
	bundle := airflow.Bundle()
	bundle.Register(newProvidersDag())

	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}

func newProvidersDag() *airflow.DagRef {
	dag := airflow.Dag("go_provider_operators", airflow.DagSpec{
		Queue: "golang",
		Tags:  []string{"go", "providers", "aws"},
	})

	// 1. Go builds the report.
	report := dag.Task(buildReport, airflow.TaskSpec{TaskID: "build_report"})

	// 2. Python stores it in S3 and waits until it is there.
	createBucket := dag.Task(
		s3operators.S3CreateBucketOperator(s3operators.S3CreateBucketOperatorArgs{
			BucketName: airflow.Of(bucket),
			AwsConnID:  airflow.Of(awsConnID),
		}),
		airflow.TaskSpec{TaskID: "create_bucket", Queue: pythonQueue},
	)
	writeReport := dag.Task(
		s3operators.S3CreateObjectOperator(s3operators.S3CreateObjectOperatorArgs{
			S3Bucket: airflow.Of(bucket),
			S3Key:    airflow.Of(reportKey),
			// The result of build_report, which the Python worker resolves when it renders the templates.
			Data:      airflow.ResultOf[string](report),
			Replace:   airflow.Ptr(true),
			AwsConnID: airflow.Of(awsConnID),
		}),
		airflow.TaskSpec{TaskID: "write_report", Queue: pythonQueue},
	)
	waitForReport := dag.Task(
		s3sensors.S3KeySensor(s3sensors.S3KeySensorArgs{
			BucketName:   airflow.Of(bucket),
			BucketKey:    airflow.Of[any](reportKey),
			AwsConnID:    airflow.Of(awsConnID),
			PokeInterval: airflow.Ptr(5.0),
			Timeout:      airflow.Ptr(120.0),
		}),
		airflow.TaskSpec{TaskID: "wait_for_report", Queue: pythonQueue},
	)

	// 3. A Bash operator prints a line, which is its result.
	runBash := dag.Task(
		bashoperators.BashOperator(bashoperators.BashOperatorArgs{
			BashCommand: airflow.Of(`echo "the report of run {{ run_id }} is stored"`),
		}),
		airflow.TaskSpec{TaskID: "run_bash", Queue: pythonQueue},
	)

	// 4. Go receives the line.
	dag.Task(summarize, airflow.TaskSpec{TaskID: "summarize"}, airflow.Inputs(runBash))

	writeReport.After(createBucket)
	waitForReport.After(writeReport)
	runBash.After(waitForReport)
	return dag
}

func buildReport(airflow.Context) (string, error) {
	return `{"orders":3,"total":120}`, nil
}

// summarize receives what the Bash operator printed.
func summarize(_ airflow.Context, printed string) (string, error) {
	return fmt.Sprintf("bash printed: %s", printed), nil
}
