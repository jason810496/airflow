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

package provider

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLineWriterLogsEachLineAPluginPrints(t *testing.T) {
	logs := &capture{}
	w := &lineWriter{logger: slog.New(logs), level: slog.LevelWarn}

	n, err := w.Write([]byte("first\nsecond"))
	assert.NoError(t, err)
	assert.Equal(t, 12, n)
	_, _ = w.Write([]byte(" half\r\n\nthird\n"))

	assert.Equal(t, []string{"first", "second half", "third"}, logs.messages)
	assert.Equal(t, slog.LevelWarn, logs.level)
}

type capture struct {
	messages []string
	level    slog.Level
}

func (c *capture) Enabled(context.Context, slog.Level) bool { return true }

func (c *capture) Handle(_ context.Context, r slog.Record) error {
	c.messages = append(c.messages, r.Message)
	c.level = r.Level
	return nil
}

func (c *capture) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *capture) WithGroup(string) slog.Handler { return c }
