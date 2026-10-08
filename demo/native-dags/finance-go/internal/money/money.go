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

// Package money keeps amounts as integers: US cents, or the minor unit of a currency.
package money

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"
)

// Rates holds US dollars per one major unit of each currency.
type Rates map[string]float64

// DefaultRates are used when the Variable finance.fx_rates is empty.
var DefaultRates = Rates{"USD": 1, "EUR": 1.085, "GBP": 1.265, "JPY": 0.00668}

var minorDigits = map[string]int{"USD": 2, "EUR": 2, "GBP": 2, "JPY": 0}

const rateScale = 1_000_000

// ParseRates reads the JSON of the Variable finance.fx_rates. A currency it leaves out keeps the
// default rate.
func ParseRates(raw string) (Rates, error) {
	rates := Rates{}
	for currency, rate := range DefaultRates {
		rates[currency] = rate
	}
	if strings.TrimSpace(raw) == "" {
		return rates, nil
	}
	var parsed map[string]float64
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf(`finance.fx_rates must be a JSON object such as {"EUR": 1.085}: %w`, err)
	}
	for currency, rate := range parsed {
		if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
			return nil, fmt.Errorf("finance.fx_rates has an invalid %s rate: %v", currency, rate)
		}
		rates[currency] = rate
	}
	return rates, nil
}

// Supports reports whether the currency has a minor unit and a rate.
func (r Rates) Supports(currency string) bool {
	_, hasDigits := minorDigits[currency]
	return hasDigits && r[currency] > 0
}

// ToUSDCents converts an amount in the minor unit of currency, rounding half away from zero.
// Check Supports first: an unknown currency converts to zero.
func (r Rates) ToUSDCents(minor int64, currency string) int64 {
	if !r.Supports(currency) {
		return 0
	}
	rate := int64(math.Round(r[currency] * rateScale))
	denominator := new(big.Int).Mul(big.NewInt(rateScale), pow10(minorDigits[currency]))
	numerator := new(big.Int).Mul(big.NewInt(minor), big.NewInt(rate*100))
	return divRound(numerator, denominator)
}

func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

func divRound(numerator, denominator *big.Int) int64 {
	sign := int64(numerator.Sign())
	n := new(big.Int).Abs(numerator)
	n.Add(n, new(big.Int).Rsh(denominator, 1))
	return sign * n.Div(n, denominator).Int64()
}

// FormatUSD writes cents as $1,234.56.
func FormatUSD(cents int64) string {
	sign := ""
	if cents < 0 {
		sign, cents = "-", -cents
	}
	return fmt.Sprintf("%s$%s.%02d", sign, group(cents/100), cents%100)
}

// FormatMinor writes an amount in the minor unit of currency, such as 1,234.56 EUR.
func FormatMinor(minor int64, currency string) string {
	sign := ""
	if minor < 0 {
		sign, minor = "-", -minor
	}
	digits := minorDigits[currency]
	if digits == 0 {
		return fmt.Sprintf("%s%s %s", sign, group(minor), currency)
	}
	unit := pow10(digits).Int64()
	return fmt.Sprintf("%s%s.%0*d %s", sign, group(minor/unit), digits, minor%unit, currency)
}

func group(n int64) string {
	digits := fmt.Sprint(n)
	var out []byte
	for i := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, digits[i])
	}
	return string(out)
}
