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

package money

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToUSDCents(t *testing.T) {
	rates, err := ParseRates(`{"EUR": 1.085, "JPY": 0.00668}`)
	assert.NoError(t, err)
	assert.Equal(t, int64(1085), rates.ToUSDCents(1000, "EUR"))
	assert.Equal(t, int64(6680), rates.ToUSDCents(10000, "JPY"))
	assert.Equal(t, int64(-1085), rates.ToUSDCents(-1000, "EUR"))
	assert.Equal(t, int64(127), rates.ToUSDCents(100, "GBP"), "GBP keeps its default rate")
	assert.False(t, rates.Supports("CHF"))
}

func TestParseRatesRejectsBadInput(t *testing.T) {
	_, err := ParseRates(`[1]`)
	assert.Error(t, err)
	_, err = ParseRates(`{"EUR": 0}`)
	assert.ErrorContains(t, err, "invalid EUR rate")
	rates, err := ParseRates("")
	assert.NoError(t, err)
	assert.Equal(t, DefaultRates, rates)
}

func TestFormat(t *testing.T) {
	assert.Equal(t, "$1,234,567.05", FormatUSD(123456705))
	assert.Equal(t, "-$0.99", FormatUSD(-99))
	assert.Equal(t, "1,234.50 EUR", FormatMinor(123450, "EUR"))
	assert.Equal(t, "-952,950 JPY", FormatMinor(-952950, "JPY"))
}
