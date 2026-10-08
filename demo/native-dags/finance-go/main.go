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

// Package main is the finance team's Dag bundle. The Dag processor runs the packed binary to parse
// its Dags, and the worker runs it for each task. Each Dag lives in its own file, so the Code view
// of Airflow shows that file and not this one.
//
// Pack it with `go tool airflow-go-pack .` and put the result in a Dag bundle.
package main

import (
	"log"

	"github.com/apache/airflow/go-sdk/airflow"
)

func main() {
	bundle := airflow.Bundle()
	bundle.Register(newRevenueCloseDag())

	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}
