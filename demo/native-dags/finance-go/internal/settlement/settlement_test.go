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

package settlement

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"coceuretail.example/finance/internal/money"
	"coceuretail.example/finance/internal/storefront"
)

func batch() *storefront.Batch {
	b := &storefront.Batch{ID: "b1", BusinessDate: "2026-10-07"}
	for i := 0; i < 40; i++ {
		currency := []string{"USD", "EUR", "JPY"}[i%3]
		b.Orders = append(b.Orders, storefront.Order{
			OrderID: fmt.Sprintf("O-%03d", i), Currency: currency,
			CardFingerprint: fmt.Sprintf("card_%d", i%7), TotalMinor: int64(2000 + i*137),
		})
	}
	b.Refunds = []storefront.Refund{{RefundID: "R1", OrderID: "O-001", Currency: "EUR", AmountMinor: 500}}
	return b
}

func TestDeriveWithoutMismatchMatchesExpected(t *testing.T) {
	meta := Meta{Processor: "p", Account: "a"}
	expected, err := Expected(batch(), meta)
	require.NoError(t, err)
	reported, err := Derive(batch(), meta, false)
	require.NoError(t, err)
	assert.Equal(t, expected, reported)

	result := Reconcile(expected, reported, money.DefaultRates, 100)
	assert.True(t, result.Matched)
	assert.Empty(t, result.Differences)
	for _, c := range expected.Currencies {
		assert.Equal(t, c.GrossMinor-c.RefundsMinor-c.FeesMinor, c.NetMinor)
	}
}

func TestInjectedMismatchIsFoundAndStable(t *testing.T) {
	meta := Meta{}
	expected, _ := Expected(batch(), meta)
	reported, err := Derive(batch(), meta, true)
	require.NoError(t, err)
	again, _ := Derive(batch(), meta, true)
	assert.Equal(t, reported, again)

	result := Reconcile(expected, reported, money.DefaultRates, 100)
	assert.False(t, result.Matched)
	items := map[string]bool{}
	for _, d := range result.Differences {
		items[d.Item] = true
	}
	assert.True(t, items["captures"] && items["gross"] && items["net payout"], "%v", items)
	assert.Greater(t, result.VarianceUSDCents, int64(100))
}

func TestPayoutDateSkipsTheWeekend(t *testing.T) {
	for date, want := range map[string]string{
		"2026-10-07": "2026-10-09", "2026-10-08": "2026-10-12", "2026-10-09": "2026-10-13", "2026-10-11": "2026-10-13",
	} {
		got, err := PayoutDate(date)
		assert.NoError(t, err)
		assert.Equal(t, want, got, date)
	}
}

func TestMethodForIsStable(t *testing.T) {
	assert.Equal(t, MethodFor("card_abc"), MethodFor("card_abc"))
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		seen[MethodFor(fmt.Sprint("card_", i))] = true
	}
	assert.Len(t, seen, len(Methods))
}
