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

// Command genproviders writes the Go wrappers of the Python providers. A wrapper has no task body: it
// records the Python operator class and its constructor arguments in the Dag with airflow.UseOperator,
// and a Python worker that has the provider installed builds and runs the operator. See ADR 0018 of
// the Lang SDK ADRs.
//
// It reads the operator spec that scripts/ci/lang_sdk_providers/generate_operator_specs.py writes to
// providers/<provider>/_generated/lang_sdk_operators.json, and writes one Go package for each Python
// module of the provider under go-sdk/providers/<provider>/. The path of a package is the path of the
// Python module under airflow.providers.<provider>, with the underscores left out: the module
// airflow.providers.amazon.aws.operators.s3 is the package providers/amazon/aws/operators/s3.
//
// The spec leaves out an operator whose required arguments JSON cannot carry, and an optional argument
// of such a kind is left out of the operator's wrapper, so the wrappers need no more checks.
//
// A provider takes part when it is in the list below. A spec that names implemented_in is written by
// the SDK that implements the provider, and has no generated wrapper. Re-run it with
// `just generate-providers`, or with -check to fail when a committed wrapper is out of date.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

// wrapped lists the Python providers that get a Go wrapper.
var wrapped = []string{"standard", "amazon"}

func main() {
	check := flag.Bool("check", false, "fail when a wrapper is out of date, and write nothing")
	providersDir := flag.String(
		"providers-dir",
		"../providers",
		"the directory that holds the providers",
	)
	outDir := flag.String("out", "providers", "the directory that the wrapper packages go to")
	flag.Parse()

	changed, err := generate(*providersDir, *outDir, wrapped, *check)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, path := range changed {
		fmt.Println(filepath.ToSlash(path))
	}
	if *check && len(changed) > 0 {
		fmt.Fprintln(os.Stderr, "out of date, run `just generate-providers` in go-sdk")
		os.Exit(1)
	}
}
