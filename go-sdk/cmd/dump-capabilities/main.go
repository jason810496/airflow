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

// Command dump-capabilities prints the Go SDK capability manifest as JSON on stdout. The
// compatibility-matrix prek hook uses it to regenerate go-sdk/capabilities.json:
//
//	go run ./cmd/dump-capabilities > capabilities.json
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/apache/airflow/go-sdk/pkg/conformance"
)

func main() {
	data, err := json.MarshalIndent(conformance.Capabilities, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := os.Stdout.Write(append(data, '\n')); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
