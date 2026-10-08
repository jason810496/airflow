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

package ledger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllocateKeepsTheTotal(t *testing.T) {
	for _, tc := range []struct {
		total   int64
		weights []int64
	}{
		{100, []int64{1, 1, 1}},
		{1, []int64{5, 5}},
		{9999, []int64{2400, 5900, 499}},
		{0, []int64{3, 4}},
		{50, []int64{0, 0}},
	} {
		var sum int64
		for _, part := range allocate(tc.total, tc.weights) {
			sum += part
		}
		if tc.weights[0]+tc.weights[1] > 0 {
			assert.Equal(t, tc.total, sum, "%v", tc)
		}
	}
}

func TestAccountForSKU(t *testing.T) {
	assert.Equal(t, "4010", AccountForSKU("TEE-001"))
	assert.Equal(t, "4040", AccountForSKU("LAP-001"))
	assert.Equal(t, AccountGiftCards, AccountForSKU("GFT-050"))
	assert.Equal(t, AccountOtherRevenue, AccountForSKU("NEW-001"))
	assert.True(t, IsRevenue("4010"))
	assert.False(t, IsRevenue(AccountRefunds))
}

func TestTrialBalanceBalances(t *testing.T) {
	entries := []Entry{{Lines: []Line{
		debit(AccountClearing, 700, ""), credit("4010", 1000, ""), debit(AccountRefunds, 300, ""),
	}}}
	tb := NewTrialBalance(entries)
	assert.Equal(t, int64(1000), tb.DebitsUSDCents)
	assert.Equal(t, tb.DebitsUSDCents, tb.CreditsUSDCents)
}
