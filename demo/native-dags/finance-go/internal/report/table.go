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

// Package report renders the plain-text tables that the tasks write to their logs.
package report

import (
	"strings"
	"unicode/utf8"
)

// Table lays out rows under headers. The columns listed in right are right-aligned.
func Table(headers []string, rows [][]string, right ...int) string {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = utf8.RuneCountInString(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			widths[i] = max(widths[i], utf8.RuneCountInString(cell))
		}
	}
	format := func(cells []string) string {
		out := make([]string, len(cells))
		for i, cell := range cells {
			pad := strings.Repeat(" ", widths[i]-utf8.RuneCountInString(cell))
			if isRight(right, i) {
				out[i] = pad + cell
			} else {
				out[i] = cell + pad
			}
		}
		return strings.TrimRight(strings.Join(out, "  "), " ")
	}
	rule := make([]string, len(widths))
	for i, w := range widths {
		rule[i] = strings.Repeat("-", w)
	}
	lines := []string{format(headers), strings.Join(rule, "  ")}
	for _, row := range rows {
		lines = append(lines, format(row))
	}
	return strings.Join(lines, "\n")
}

func isRight(right []int, column int) bool {
	for _, r := range right {
		if r == column {
			return true
		}
	}
	return false
}
