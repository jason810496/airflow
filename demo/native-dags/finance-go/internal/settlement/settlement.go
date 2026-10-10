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

// Package settlement derives the settlement report of the payment processor and reconciles it with
// what finance expects to be paid.
//
// The processor reports in the currency of each charge, so every figure here is in a minor unit and
// no FX rate is involved until a difference is priced in US cents.
package settlement

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"coceuretail.example/finance/internal/money"
	"coceuretail.example/finance/internal/storefront"
)

// Methods are the payment methods in the order they are reported.
var Methods = []string{"visa", "mastercard", "amex", "wallet"}

var methodShare = []struct {
	method string
	upTo   uint32
}{{"visa", 45}, {"mastercard", 75}, {"amex", 85}, {"wallet", 100}}

type tariff struct {
	bps   int64
	fixed map[string]int64
}

// Tariffs is the fee schedule of the processor contract: basis points of the charge plus a fixed
// amount in the minor unit of the currency.
var Tariffs = map[string]tariff{
	"visa":       {290, map[string]int64{"USD": 30, "EUR": 25, "GBP": 20, "JPY": 30}},
	"mastercard": {275, map[string]int64{"USD": 30, "EUR": 25, "GBP": 20, "JPY": 30}},
	"amex":       {350, map[string]int64{"USD": 30, "EUR": 25, "GBP": 20, "JPY": 30}},
	"wallet":     {249, map[string]int64{"USD": 30, "EUR": 25, "GBP": 20, "JPY": 30}},
}

// MethodFor infers the payment method from the card fingerprint, as the processor sees it.
func MethodFor(cardFingerprint string) string {
	sum := sha256.Sum256([]byte(cardFingerprint))
	roll := binary.BigEndian.Uint32(sum[:4]) % 100
	for _, share := range methodShare {
		if roll < share.upTo {
			return share.method
		}
	}
	return Methods[len(Methods)-1]
}

// Meta is what the Connection payments_gateway says about the processor account.
type Meta struct {
	Processor string `json:"processor"`
	Account   string `json:"account"`
}

type FeeLine struct {
	Method     string `json:"method"`
	Captures   int    `json:"captures"`
	GrossMinor int64  `json:"gross_minor"`
	FeeMinor   int64  `json:"fee_minor"`
}

type CurrencyLine struct {
	Currency     string    `json:"currency"`
	Captures     int       `json:"captures"`
	GrossMinor   int64     `json:"gross_minor"`
	Refunds      int       `json:"refunds"`
	RefundsMinor int64     `json:"refunds_minor"`
	Fees         []FeeLine `json:"fees"`
	FeesMinor    int64     `json:"fees_minor"`
	NetMinor     int64     `json:"net_minor"`
}

// Report is the settlement report of one batch.
type Report struct {
	Meta
	BatchID      string         `json:"batch_id"`
	BusinessDate string         `json:"business_date"`
	PayoutDate   string         `json:"payout_date"`
	Currencies   []CurrencyLine `json:"currencies"`
}

// Currency returns the line of currency, or a zero line.
func (r Report) Currency(currency string) CurrencyLine {
	for _, line := range r.Currencies {
		if line.Currency == currency {
			return line
		}
	}
	return CurrencyLine{Currency: currency}
}

// PayoutDate is two business days after the business date.
func PayoutDate(businessDate string) (string, error) {
	day, err := time.Parse(time.DateOnly, businessDate)
	if err != nil {
		return "", fmt.Errorf("business date %q: %w", businessDate, err)
	}
	for added := 0; added < 2; {
		day = day.AddDate(0, 0, 1)
		if wd := day.Weekday(); wd != time.Saturday && wd != time.Sunday {
			added++
		}
	}
	return day.Format(time.DateOnly), nil
}

func fee(method, currency string, grossMinor int64, captures int) int64 {
	t := Tariffs[method]
	return (grossMinor*t.bps+5000)/10000 + int64(captures)*t.fixed[currency]
}

// Expected is the report that finance expects from its own orders, refunds and the contract tariff.
func Expected(batch *storefront.Batch, meta Meta) (Report, error) {
	payout, err := PayoutDate(batch.BusinessDate)
	if err != nil {
		return Report{}, err
	}
	type key struct{ currency, method string }
	gross := map[key]int64{}
	captures := map[key]int{}
	lines := map[string]*CurrencyLine{}
	line := func(currency string) *CurrencyLine {
		if lines[currency] == nil {
			lines[currency] = &CurrencyLine{Currency: currency}
		}
		return lines[currency]
	}
	for _, order := range batch.Orders {
		k := key{order.Currency, MethodFor(order.CardFingerprint)}
		gross[k] += order.TotalMinor
		captures[k]++
		l := line(order.Currency)
		l.Captures++
		l.GrossMinor += order.TotalMinor
	}
	for _, refund := range batch.Refunds {
		l := line(refund.Currency)
		l.Refunds++
		l.RefundsMinor += refund.AmountMinor
	}
	for k, g := range gross {
		l := line(k.currency)
		f := FeeLine{k.method, captures[k], g, fee(k.method, k.currency, g, captures[k])}
		l.Fees = append(l.Fees, f)
		l.FeesMinor += f.FeeMinor
	}
	report := Report{
		Meta: meta, BatchID: batch.ID, BusinessDate: batch.BusinessDate, PayoutDate: payout,
	}
	for _, l := range lines {
		sort.Slice(l.Fees, func(i, j int) bool { return methodRank(l.Fees[i].Method) < methodRank(l.Fees[j].Method) })
		l.NetMinor = l.GrossMinor - l.RefundsMinor - l.FeesMinor
		report.Currencies = append(report.Currencies, *l)
	}
	sort.Slice(report.Currencies, func(i, j int) bool {
		return report.Currencies[i].Currency < report.Currencies[j].Currency
	})
	return report, nil
}

func methodRank(method string) int {
	for i, m := range Methods {
		if m == method {
			return i
		}
	}
	return len(Methods)
}

// Derive is the report the processor sends. Without injectMismatch it equals Expected. With it, the
// processor misses one capture and overcharges one method, which are the two small discrepancies
// that finance has to catch. The choice depends on the batch only, so a retry reports the same.
func Derive(batch *storefront.Batch, meta Meta, injectMismatch bool) (Report, error) {
	if !injectMismatch {
		return Expected(batch, meta)
	}
	missed, ok := missedCapture(batch)
	if !ok {
		return Expected(batch, meta)
	}
	trimmed := *batch
	trimmed.Orders = make([]storefront.Order, 0, len(batch.Orders)-1)
	for _, order := range batch.Orders {
		if order.OrderID != missed.OrderID {
			trimmed.Orders = append(trimmed.Orders, order)
		}
	}
	report, err := Expected(&trimmed, meta)
	if err != nil {
		return Report{}, err
	}
	for i := range report.Currencies {
		l := &report.Currencies[i]
		if l.Currency != missed.Currency || len(l.Fees) == 0 {
			continue
		}
		biggest := 0
		for j, f := range l.Fees {
			if f.GrossMinor > l.Fees[biggest].GrossMinor {
				biggest = j
			}
		}
		// Twenty-five basis points above the contract.
		l.Fees[biggest].FeeMinor += (l.Fees[biggest].GrossMinor*25 + 5000) / 10000
		l.FeesMinor = 0
		for _, f := range l.Fees {
			l.FeesMinor += f.FeeMinor
		}
		l.NetMinor = l.GrossMinor - l.RefundsMinor - l.FeesMinor
	}
	return report, nil
}

// missedCapture picks the order the processor leaves out: one of the orders in the currency with
// the highest volume, chosen by a hash of the batch id.
func missedCapture(batch *storefront.Batch) (storefront.Order, bool) {
	volume := map[string]int64{}
	for _, order := range batch.Orders {
		volume[order.Currency] += order.TotalMinor
	}
	best := ""
	for currency, v := range volume {
		if best == "" || v > volume[best] || (v == volume[best] && currency < best) {
			best = currency
		}
	}
	var candidates []storefront.Order
	for _, order := range batch.Orders {
		if order.Currency == best && order.TotalMinor > 0 {
			candidates = append(candidates, order)
		}
	}
	if len(candidates) < 2 {
		return storefront.Order{}, false
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].OrderID < candidates[j].OrderID })
	sum := sha256.Sum256([]byte(batch.ID))
	return candidates[int(binary.BigEndian.Uint32(sum[:4]))%len(candidates)], true
}

// Difference is one line where the processor and finance disagree.
type Difference struct {
	Currency      string `json:"currency"`
	Item          string `json:"item"`
	ExpectedMinor int64  `json:"expected_minor"`
	ReportedMinor int64  `json:"reported_minor"`
	// DiffMinor is a count for the captures item, and a minor unit amount for every other item.
	DiffMinor    int64 `json:"diff_minor"`
	DiffUSDCents int64 `json:"diff_usd_cents"`
}

// Result is the outcome of comparing a reported settlement with the expected one.
type Result struct {
	Matched bool `json:"matched"`
	// VarianceUSDCents is the sum of the absolute net payout differences of all currencies.
	VarianceUSDCents  int64        `json:"variance_usd_cents"`
	ToleranceUSDCents int64        `json:"tolerance_usd_cents"`
	Differences       []Difference `json:"differences"`
}

// Reconcile compares the reported settlement with the expected one. The settlement matches when the
// net payout of every currency agrees within toleranceUSDCents in total.
func Reconcile(expected, reported Report, rates money.Rates, toleranceUSDCents int64) Result {
	result := Result{ToleranceUSDCents: toleranceUSDCents, Differences: []Difference{}}
	currencies := map[string]bool{}
	for _, l := range expected.Currencies {
		currencies[l.Currency] = true
	}
	for _, l := range reported.Currencies {
		currencies[l.Currency] = true
	}
	names := make([]string, 0, len(currencies))
	for c := range currencies {
		names = append(names, c)
	}
	sort.Strings(names)

	for _, currency := range names {
		want, got := expected.Currency(currency), reported.Currency(currency)
		add := func(item string, w, g int64) {
			if w == g {
				return
			}
			result.Differences = append(result.Differences, Difference{
				Currency: currency, Item: item, ExpectedMinor: w, ReportedMinor: g,
				DiffMinor: g - w, DiffUSDCents: rates.ToUSDCents(g-w, currency),
			})
		}
		if want.Captures != got.Captures {
			result.Differences = append(result.Differences, Difference{
				Currency: currency, Item: "captures", ExpectedMinor: int64(want.Captures),
				ReportedMinor: int64(got.Captures), DiffMinor: int64(got.Captures - want.Captures),
			})
		}
		add("gross", want.GrossMinor, got.GrossMinor)
		add("refunds", want.RefundsMinor, got.RefundsMinor)
		for _, method := range Methods {
			add("fee "+method, feeOf(want, method), feeOf(got, method))
		}
		add("net payout", want.NetMinor, got.NetMinor)
		net := rates.ToUSDCents(got.NetMinor-want.NetMinor, currency)
		result.VarianceUSDCents += max(net, -net)
	}
	result.Matched = result.VarianceUSDCents <= toleranceUSDCents
	return result
}

func feeOf(line CurrencyLine, method string) int64 {
	for _, f := range line.Fees {
		if f.Method == method {
			return f.FeeMinor
		}
	}
	return 0
}
