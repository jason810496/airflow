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

package period

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		date  string
		force bool
		want  Kind
	}{
		{"2026-10-07", false, Daily},
		{"2026-10-11", false, WeekEnd},
		{"2026-10-31", false, MonthEnd},
		{"2026-05-31", false, MonthEnd},
		{"2028-02-29", false, MonthEnd},
		{"2026-02-28", false, MonthEnd},
		{"2026-10-07", true, MonthEnd},
	} {
		got, err := Classify(tc.date, tc.force)
		assert.NoError(t, err)
		assert.Equal(t, tc.want, got, tc.date)
	}
	_, err := Classify("10/07/2026", false)
	assert.Error(t, err)
}
