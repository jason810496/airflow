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

// Package ledger builds the double-entry journal of a batch in integer US cents.
package ledger

import (
	"fmt"
	"sort"
	"strings"

	"coceuretail.example/finance/internal/money"
	"coceuretail.example/finance/internal/report"
	"coceuretail.example/finance/internal/risk"
	"coceuretail.example/finance/internal/settlement"
	"coceuretail.example/finance/internal/storefront"
)

const (
	AccountClearing     = "1100"
	AccountAccruedFees  = "2200"
	AccountHeldRevenue  = "2300"
	AccountRefundsDue   = "2400"
	AccountGiftCards    = "2500"
	AccountOtherRevenue = "4090"
	AccountRefunds      = "4900"
	AccountFees         = "5100"
	AccountPlatformFee  = "5110"
	AccountAssessments  = "5120"
)

var accountNames = map[string]string{
	AccountClearing:     "Cash clearing, payment processor",
	AccountAccruedFees:  "Accrued processor fees",
	AccountHeldRevenue:  "Held revenue, orders under review",
	AccountRefundsDue:   "Refunds payable, blocked orders",
	AccountGiftCards:    "Gift card liability",
	"4010":              "Revenue, apparel",
	"4020":              "Revenue, footwear",
	"4030":              "Revenue, home",
	"4040":              "Revenue, electronics",
	"4050":              "Revenue, beauty",
	AccountOtherRevenue: "Revenue, shipping and other",
	AccountRefunds:      "Refunds and returns, contra revenue",
	AccountFees:         "Processor fees",
	AccountPlatformFee:  "Processor platform fee",
	AccountAssessments:  "Card network assessments",
}

var skuAccounts = map[string]string{
	"TEE": "4010", "HDY": "4010", "JKT": "4010", "JNS": "4010", "SOC": "4010", "CAP": "4010",
	"SNK": "4020", "BOT": "4020", "SND": "4020",
	"MUG": "4030", "CND": "4030", "BLK": "4030", "LMP": "4030", "PLW": "4030", "KTL": "4030",
	"EAR": "4040", "WCH": "4040", "SPK": "4040", "CHG": "4040", "PWR": "4040", "TAB": "4040",
	"LAP": "4040", "CAM": "4040",
	"SRM": "4050", "MOI": "4050", "SUN": "4050", "LIP": "4050", "SHM": "4050", "PRF": "4050",
	"GFT": AccountGiftCards,
}

// AccountName is the name of an account code.
func AccountName(code string) string {
	if name, ok := accountNames[code]; ok {
		return name
	}
	return "Account " + code
}

// IsRevenue reports whether the account holds recognized revenue.
func IsRevenue(code string) bool { return strings.HasPrefix(code, "40") && code != AccountRefunds }

// AccountForSKU maps a product to its account. A gift card is a liability until it is redeemed.
func AccountForSKU(sku string) string {
	prefix, _, _ := strings.Cut(sku, "-")
	if code, ok := skuAccounts[prefix]; ok {
		return code
	}
	return AccountOtherRevenue
}

type Line struct {
	Account        string `json:"account"`
	Name           string `json:"name"`
	DebitUSDCents  int64  `json:"debit_usd_cents"`
	CreditUSDCents int64  `json:"credit_usd_cents"`
	Memo           string `json:"memo,omitempty"`
}

type Entry struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	Lines       []Line `json:"lines"`
}

// Summary is what the rest of the close needs from the journal.
type Summary struct {
	GrossSalesUSDCents        int64            `json:"gross_sales_usd_cents"`
	RefundsUSDCents           int64            `json:"refunds_usd_cents"`
	FeesUSDCents              int64            `json:"fees_usd_cents"`
	RecognizedRevenueUSDCents int64            `json:"recognized_revenue_usd_cents"`
	NetRevenueUSDCents        int64            `json:"net_revenue_usd_cents"`
	HeldRevenueUSDCents       int64            `json:"held_revenue_usd_cents"`
	HeldOrders                int              `json:"held_orders"`
	ReversedUSDCents          int64            `json:"reversed_usd_cents"`
	ReversedOrders            int              `json:"reversed_orders"`
	GiftCardsUSDCents         int64            `json:"gift_cards_usd_cents"`
	ClearingUSDCents          int64            `json:"clearing_usd_cents"`
	RevenueByAccount          map[string]int64 `json:"revenue_by_account_usd_cents"`
}

type Journal struct {
	BatchID         string             `json:"batch_id"`
	BusinessDate    string             `json:"business_date"`
	FxRates         map[string]float64 `json:"fx_rates"`
	Entries         []Entry            `json:"entries"`
	DebitsUSDCents  int64              `json:"debits_usd_cents"`
	CreditsUSDCents int64              `json:"credits_usd_cents"`
	Summary         Summary            `json:"summary"`
}

// Balanced reports whether debits equal credits.
func (j *Journal) Balanced() bool { return j.DebitsUSDCents == j.CreditsUSDCents }

type Input struct {
	Batch      *storefront.Batch
	Screening  risk.Screening
	Settlement settlement.Report
	Rates      money.Rates
}

// Build posts the journal of a batch:
//
//	JE-01 sales captured: Dr clearing, Cr revenue by product group (gift cards go to a liability)
//	JE-02 refunds: Dr refunds and returns, Cr clearing
//	JE-03 processor fees as reported by the processor: Dr fees, Cr clearing
//	JE-04 revenue of orders that risk wants reviewed: Dr revenue, Cr held revenue
//	JE-05 orders that risk blocked: Dr revenue, Cr refunds payable
//
// Every figure is converted at the closing rates, order by order, and spread over the lines of the
// order, so the entries add up to the cent.
func Build(in Input) (*Journal, error) {
	sales := map[string]int64{}
	held := map[string]int64{}
	reversed := map[string]int64{}
	var grossSales, heldTotal, reversedTotal int64
	var heldOrders, reversedOrders int

	actions := make(map[string]risk.Action, len(in.Screening.Decisions))
	for _, d := range in.Screening.Decisions {
		actions[d.OrderID] = d.Action
	}

	for _, order := range in.Batch.Orders {
		if !in.Rates.Supports(order.Currency) {
			return nil, fmt.Errorf("order %s is in %s, which has no rate in finance.fx_rates", order.OrderID, order.Currency)
		}
		spread, err := spreadOrder(order, in.Rates)
		if err != nil {
			return nil, err
		}
		action := actions[order.OrderID]
		var orderTotal int64
		for account, cents := range spread {
			sales[account] += cents
			orderTotal += cents
			switch {
			case action == risk.Block:
				reversed[account] += cents
			case action == risk.Review && IsRevenue(account):
				held[account] += cents
			}
		}
		grossSales += orderTotal
		switch action {
		case risk.Block:
			reversedTotal += orderTotal
			reversedOrders++
		case risk.Review:
			var heldHere int64
			for account, cents := range spread {
				if IsRevenue(account) {
					heldHere += cents
				}
			}
			if heldHere > 0 {
				heldTotal += heldHere
				heldOrders++
			}
		}
	}

	var refundsTotal int64
	for _, refund := range in.Batch.Refunds {
		if !in.Rates.Supports(refund.Currency) {
			return nil, fmt.Errorf("refund %s is in %s, which has no rate in finance.fx_rates", refund.RefundID, refund.Currency)
		}
		refundsTotal += in.Rates.ToUSDCents(refund.AmountMinor, refund.Currency)
	}

	feesByMethod := map[string]int64{}
	var feesTotal int64
	for _, currency := range in.Settlement.Currencies {
		if !in.Rates.Supports(currency.Currency) {
			return nil, fmt.Errorf("the settlement report has %s, which has no rate in finance.fx_rates", currency.Currency)
		}
		for _, f := range currency.Fees {
			cents := in.Rates.ToUSDCents(f.FeeMinor, currency.Currency)
			feesByMethod[f.Method] += cents
			feesTotal += cents
		}
	}

	j := &Journal{BatchID: in.Batch.ID, BusinessDate: in.Batch.BusinessDate, FxRates: in.Rates}
	salesEntry := Entry{ID: "JE-01", Description: "Sales captured by the processor"}
	salesEntry.Lines = append(salesEntry.Lines, debit(AccountClearing, grossSales, fmt.Sprintf("%d orders", len(in.Batch.Orders))))
	salesEntry.Lines = append(salesEntry.Lines, credits(sales, "")...)
	j.Entries = append(j.Entries, salesEntry)

	if refundsTotal > 0 {
		j.Entries = append(j.Entries, Entry{ID: "JE-02", Description: "Refunds paid out", Lines: []Line{
			debit(AccountRefunds, refundsTotal, fmt.Sprintf("%d refunds", len(in.Batch.Refunds))),
			credit(AccountClearing, refundsTotal, ""),
		}})
	}
	if feesTotal > 0 {
		fees := Entry{ID: "JE-03", Description: "Processor fees withheld"}
		for _, method := range settlement.Methods {
			if cents := feesByMethod[method]; cents > 0 {
				fees.Lines = append(fees.Lines, debit(AccountFees, cents, method))
			}
		}
		fees.Lines = append(fees.Lines, credit(AccountClearing, feesTotal, ""))
		j.Entries = append(j.Entries, fees)
	}
	if heldTotal > 0 {
		e := Entry{ID: "JE-04", Description: "Revenue held while risk reviews the orders"}
		e.Lines = append(e.Lines, debits(held)...)
		e.Lines = append(e.Lines, credit(AccountHeldRevenue, heldTotal, fmt.Sprintf("%d orders", heldOrders)))
		j.Entries = append(j.Entries, e)
	}
	if reversedTotal > 0 {
		e := Entry{ID: "JE-05", Description: "Reversal of the orders that risk blocked"}
		e.Lines = append(e.Lines, debits(reversed)...)
		e.Lines = append(e.Lines, credit(AccountRefundsDue, reversedTotal, fmt.Sprintf("%d orders", reversedOrders)))
		j.Entries = append(j.Entries, e)
	}

	for _, e := range j.Entries {
		for _, l := range e.Lines {
			j.DebitsUSDCents += l.DebitUSDCents
			j.CreditsUSDCents += l.CreditUSDCents
		}
	}
	j.Summary = summarize(j, grossSales, heldOrders, reversedOrders)
	return j, nil
}

func summarize(j *Journal, grossSales int64, heldOrders, reversedOrders int) Summary {
	balances := Balances(j.Entries)
	s := Summary{
		GrossSalesUSDCents: grossSales,
		HeldOrders:         heldOrders,
		ReversedOrders:     reversedOrders,
		RevenueByAccount:   map[string]int64{},
	}
	for account, balance := range balances {
		switch {
		case IsRevenue(account):
			s.RecognizedRevenueUSDCents -= balance
			s.RevenueByAccount[account] = -balance
		case account == AccountRefunds:
			s.RefundsUSDCents = balance
		case account == AccountFees:
			s.FeesUSDCents = balance
		case account == AccountHeldRevenue:
			s.HeldRevenueUSDCents = -balance
		case account == AccountRefundsDue:
			s.ReversedUSDCents = -balance
		case account == AccountGiftCards:
			s.GiftCardsUSDCents = -balance
		case account == AccountClearing:
			s.ClearingUSDCents = balance
		}
	}
	s.NetRevenueUSDCents = s.RecognizedRevenueUSDCents - s.RefundsUSDCents
	return s
}

// spreadOrder converts an order to US cents and splits it over the accounts of its lines.
func spreadOrder(order storefront.Order, rates money.Rates) (map[string]int64, error) {
	weights := make([]int64, 0, len(order.Items)+1)
	accounts := make([]string, 0, len(order.Items)+1)
	var lines int64
	for _, item := range order.Items {
		weights = append(weights, item.Qty*item.UnitPriceMinor)
		accounts = append(accounts, AccountForSKU(item.SKU))
		lines += item.Qty * item.UnitPriceMinor
	}
	switch rest := order.TotalMinor - lines; {
	case rest < 0:
		return nil, fmt.Errorf("order %s totals %d, less than its lines at %d", order.OrderID, order.TotalMinor, lines)
	case rest > 0:
		weights = append(weights, rest)
		accounts = append(accounts, AccountOtherRevenue)
	}
	spread := map[string]int64{}
	for i, cents := range allocate(rates.ToUSDCents(order.TotalMinor, order.Currency), weights) {
		spread[accounts[i]] += cents
	}
	return spread, nil
}

// allocate splits total in proportion to weights, so the parts add up to total.
func allocate(total int64, weights []int64) []int64 {
	var sum int64
	for _, w := range weights {
		sum += w
	}
	parts := make([]int64, len(weights))
	if sum == 0 {
		return parts
	}
	type remainder struct {
		index int
		rest  int64
	}
	rests := make([]remainder, len(weights))
	var given int64
	for i, w := range weights {
		parts[i] = total * w / sum
		rests[i] = remainder{i, total * w % sum}
		given += parts[i]
	}
	sort.SliceStable(rests, func(a, b int) bool { return rests[a].rest > rests[b].rest })
	for i := int64(0); i < total-given; i++ {
		parts[rests[i].index]++
	}
	return parts
}

func debit(account string, cents int64, memo string) Line {
	return Line{Account: account, Name: AccountName(account), DebitUSDCents: cents, Memo: memo}
}

func credit(account string, cents int64, memo string) Line {
	return Line{Account: account, Name: AccountName(account), CreditUSDCents: cents, Memo: memo}
}

func sortedAccounts(amounts map[string]int64) []string {
	accounts := make([]string, 0, len(amounts))
	for account, cents := range amounts {
		if cents != 0 {
			accounts = append(accounts, account)
		}
	}
	sort.Strings(accounts)
	return accounts
}

func debits(amounts map[string]int64) []Line {
	var lines []Line
	for _, account := range sortedAccounts(amounts) {
		lines = append(lines, debit(account, amounts[account], ""))
	}
	return lines
}

func credits(amounts map[string]int64, memo string) []Line {
	var lines []Line
	for _, account := range sortedAccounts(amounts) {
		lines = append(lines, credit(account, amounts[account], memo))
	}
	return lines
}

// Balances is the net debit balance of each account, so a credit balance is negative.
func Balances(entries []Entry) map[string]int64 {
	balances := map[string]int64{}
	for _, e := range entries {
		for _, l := range e.Lines {
			balances[l.Account] += l.DebitUSDCents - l.CreditUSDCents
		}
	}
	return balances
}

type TrialBalanceLine struct {
	Account        string `json:"account"`
	Name           string `json:"name"`
	DebitUSDCents  int64  `json:"debit_usd_cents"`
	CreditUSDCents int64  `json:"credit_usd_cents"`
}

type TrialBalance struct {
	Lines           []TrialBalanceLine `json:"lines"`
	DebitsUSDCents  int64              `json:"debits_usd_cents"`
	CreditsUSDCents int64              `json:"credits_usd_cents"`
}

// NewTrialBalance lists the balance of every account that the entries touch.
func NewTrialBalance(entries []Entry) TrialBalance {
	balances := Balances(entries)
	accounts := make([]string, 0, len(balances))
	for account := range balances {
		accounts = append(accounts, account)
	}
	sort.Strings(accounts)
	tb := TrialBalance{Lines: []TrialBalanceLine{}}
	for _, account := range accounts {
		line := TrialBalanceLine{Account: account, Name: AccountName(account)}
		if balance := balances[account]; balance >= 0 {
			line.DebitUSDCents = balance
		} else {
			line.CreditUSDCents = -balance
		}
		tb.DebitsUSDCents += line.DebitUSDCents
		tb.CreditsUSDCents += line.CreditUSDCents
		tb.Lines = append(tb.Lines, line)
	}
	return tb
}

// Table renders the trial balance for a log.
func (tb TrialBalance) Table() string {
	rows := make([][]string, 0, len(tb.Lines)+1)
	for _, l := range tb.Lines {
		rows = append(rows, []string{l.Account, l.Name, cell(l.DebitUSDCents), cell(l.CreditUSDCents)})
	}
	rows = append(rows, []string{"", "Total", money.FormatUSD(tb.DebitsUSDCents), money.FormatUSD(tb.CreditsUSDCents)})
	return report.Table([]string{"account", "name", "debit", "credit"}, rows, 2, 3)
}

// Table renders every line of the entries for a log.
func Table(entries []Entry) string {
	var rows [][]string
	for _, e := range entries {
		for _, l := range e.Lines {
			rows = append(rows, []string{e.ID, l.Account, l.Name, cell(l.DebitUSDCents), cell(l.CreditUSDCents), l.Memo})
		}
	}
	return report.Table([]string{"entry", "account", "name", "debit", "credit", "memo"}, rows, 3, 4)
}

func cell(cents int64) string {
	if cents == 0 {
		return ""
	}
	return money.FormatUSD(cents)
}
