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

// Package lake reads and writes the shared data lake. Each team writes under
// <lake>/<team>/<batch_id>/, and teams hand work to each other through Variables that name a batch
// directory.
package lake

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

const (
	Team = "finance"

	// StorefrontBatchVariable names the storefront batch directory to close.
	StorefrontBatchVariable = "handoff.storefront.latest_batch"
	// RiskDecisionsVariable names the directory that holds the risk team's decisions.json.
	RiskDecisionsVariable = "handoff.risk.latest_decisions"
	// FinanceCloseVariable names the finance batch directory that finance_revenue_close finished.
	FinanceCloseVariable = "handoff.finance.latest_close"
)

const defaultRoot = "/files/demo/lake"

// Root is the lake root, COCEU_LAKE_ROOT or /files/demo/lake.
func Root() string {
	if root := os.Getenv("COCEU_LAKE_ROOT"); root != "" {
		return root
	}
	return defaultRoot
}

// FinanceDir is where finance writes the files of one batch.
func FinanceDir(batchID string) string {
	return filepath.Join(Root(), Team, batchID)
}

// File describes a file in the lake.
type File struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// Digest reads path and returns its size and sha256.
func Digest(path string) (File, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return File{}, err
	}
	return digestOf(filepath.Base(path), content), nil
}

func digestOf(name string, content []byte) File {
	sum := sha256.Sum256(content)
	return File{Name: name, Bytes: int64(len(content)), SHA256: hex.EncodeToString(sum[:])}
}

// ReadJSON decodes the JSON file at path into v.
func ReadJSON(path string, v any) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(content, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// WriteJSON writes v as indented JSON through a temporary file, so a reader never sees half a file.
func WriteJSON(dir, name string, v any) (File, error) {
	content, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return File{}, fmt.Errorf("%s: %w", name, err)
	}
	content = append(content, '\n')
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return File{}, err
	}
	target := filepath.Join(dir, name)
	staging := target + ".tmp"
	if err := os.WriteFile(staging, content, 0o644); err != nil {
		return File{}, err
	}
	if err := os.Rename(staging, target); err != nil {
		return File{}, err
	}
	return digestOf(name, content), nil
}

// Digests lists the regular files directly inside dir, sorted by name, leaving out the names in skip
// and any temporary file.
func Digests(dir string, skip ...string) ([]File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := []File{}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.Type().IsRegular() || strings.HasSuffix(name, ".tmp") || slices.Contains(skip, name) {
			continue
		}
		digest, err := Digest(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		files = append(files, digest)
	}
	return files, nil
}

// Remove deletes the named files of dir, ignoring the ones that are not there.
func Remove(dir string, names ...string) error {
	for _, name := range names {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Exists reports whether path is a regular file.
func Exists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

// Parallel runs every function in its own goroutine and returns the first error in argument order,
// after all of them have finished.
func Parallel(fns ...func() error) error {
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn()
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
