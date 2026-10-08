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

package risk

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func write(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "decisions.json"), []byte(body), 0o644))
	return dir
}

func TestLoad(t *testing.T) {
	dir := write(t, `{"storefront_batch_dir": "/lake/storefront/b1/", "decisions": [
		{"order_id": "A", "action": "block", "extra": 1}, {"order_id": "B", "action": "review"}, {"order_id": "C", "action": "approve"}]}`)

	s, err := Load(dir, "/lake/storefront/b1")
	assert.NoError(t, err)
	assert.True(t, s.Screened)
	assert.Equal(t, [3]int{1, 1, 1}, [3]int{s.Approved, s.Review, s.Blocked})
	assert.Equal(t, Block, s.ActionFor("A"))
	assert.Equal(t, Approve, s.ActionFor("not listed"))

	for _, other := range []string{"/lake/storefront/b2", ""} {
		s, err = Load(dir, other)
		assert.NoError(t, err)
		assert.False(t, s.Screened)
		assert.Equal(t, NoScreening, s.Message)
	}
	s, err = Load("", "/lake/storefront/b1")
	assert.NoError(t, err)
	assert.Equal(t, NoScreening, s.Message)
	s, err = Load(t.TempDir(), "/lake/storefront/b1")
	assert.NoError(t, err)
	assert.False(t, s.Screened)
}

func TestLoadRejectsBadDecisions(t *testing.T) {
	_, err := Load(write(t, `{"storefront_batch_dir": "/b", "decisions": [{"order_id": "A", "action": "hold"}]}`), "/b")
	assert.ErrorContains(t, err, `action "hold"`)
	_, err = Load(write(t, `{"storefront_batch_dir": "/b", "decisions": [{"order_id": "A", "action": "block"}, {"order_id": "A", "action": "review"}]}`), "/b")
	assert.ErrorContains(t, err, "twice")
}
