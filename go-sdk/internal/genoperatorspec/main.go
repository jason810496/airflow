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

// Command genoperatorspec writes the operator spec of each provider that is implemented in Go, to
// providers/<provider id as a path>/_generated/lang_sdk_operators.json at the repository root, beside
// the specs that scripts/ci/lang_sdk_providers/generate_operator_specs.py writes for the Python
// providers. Every SDK generates its wrapper of such a provider from the spec, and the Python wrapper
// package is generated from it by scripts/ci/lang_sdk_providers/generate_python_wrappers.py.
//
// A provider takes part when it is in the list below. Re-run it with `just generate-operator-spec`, or
// with -check to fail when a committed spec is out of date.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/airflow/go-sdk/provider"
	"github.com/apache/airflow/go-sdk/providers/localfile"
)

// implemented lists the providers implemented in Go.
var implemented = []provider.Info{localfile.Info}

func main() {
	check := flag.Bool(
		"check",
		false,
		"fail when a committed spec is out of date, and write nothing",
	)
	providersDir := flag.String(
		"providers-dir", "../providers", "the directory that holds the providers of the repository",
	)
	flag.Parse()
	if err := run(*providersDir, *check, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run writes the specs under providersDir, or with check only reports the ones that differ.
func run(providersDir string, check bool, out *os.File) error {
	var stale []string
	for _, info := range implemented {
		spec, err := info.RenderSpec()
		if err != nil {
			return err
		}
		path := filepath.Join(providersDir, filepath.FromSlash(info.SpecPath()))
		current, err := os.ReadFile(path)
		if err == nil && bytes.Equal(current, spec) {
			continue
		}
		stale = append(stale, path)
		if check {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, spec, 0o644); err != nil {
			return err
		}
		fmt.Fprintln(out, "wrote", path)
	}
	if check && len(stale) > 0 {
		return fmt.Errorf("out of date, run `just generate-operator-spec` in go-sdk:\n%v", stale)
	}
	return nil
}
