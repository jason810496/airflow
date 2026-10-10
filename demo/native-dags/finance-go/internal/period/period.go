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

// Package period holds the period close steps that follow a balanced ledger: the daily close, the
// week-end roll-up and the month-end accruals.
package period

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"coceuretail.example/finance/internal/lake"
	"coceuretail.example/finance/internal/ledger"
)

type Kind string

const (
	Daily    Kind = "daily_close"
	WeekEnd  Kind = "week_end_close"
	MonthEnd Kind = "month_end_close"
)

const (
	// PlatformFeeUSDCents is the monthly fee of the processor platform, invoiced in arrears.
	PlatformFeeUSDCents = 49_900
	// AssessmentBps is what the card networks assess on the month's gross volume, invoiced in arrears.
	AssessmentBps = 13
)

// Classify decides which close a business date gets. The last day of the month is a month end, any
// other Sunday is a week end, and everything else is a daily close. forceMonthEnd makes any date a
// month end.
func Classify(businessDate string, forceMonthEnd bool) (Kind, error) {
	day, err := time.Parse(time.DateOnly, businessDate)
	if err != nil {
		return "", fmt.Errorf("business date %q: %w", businessDate, err)
	}
	switch {
	case forceMonthEnd || day.AddDate(0, 0, 1).Day() == 1:
		return MonthEnd, nil
	case day.Weekday() == time.Sunday:
		return WeekEnd, nil
	default:
		return Daily, nil
	}
}

type Control struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

type Day struct {
	Date            string `json:"date"`
	Closed          bool   `json:"closed"`
	GrossSalesCents int64  `json:"gross_sales_usd_cents"`
	NetRevenueCents int64  `json:"net_revenue_usd_cents"`
	RefundsCents    int64  `json:"refunds_usd_cents"`
	FeesCents       int64  `json:"fees_usd_cents"`
	HeldCents       int64  `json:"held_revenue_usd_cents"`
}

type Totals struct {
	DaysClosed      int      `json:"days_closed"`
	MissingDates    []string `json:"missing_dates"`
	GrossSalesCents int64    `json:"gross_sales_usd_cents"`
	NetRevenueCents int64    `json:"net_revenue_usd_cents"`
	RefundsCents    int64    `json:"refunds_usd_cents"`
	FeesCents       int64    `json:"fees_usd_cents"`
	HeldCents       int64    `json:"held_revenue_usd_cents"`
}

type Week struct {
	Start  string `json:"start"`
	End    string `json:"end"`
	Days   []Day  `json:"days"`
	Totals Totals `json:"totals"`
}

type Month struct {
	Month                string         `json:"month"`
	DaysInMonth          int            `json:"days_in_month"`
	Totals               Totals         `json:"month_to_date"`
	Accruals             []ledger.Entry `json:"accruals"`
	AccrualUSDCents      int64          `json:"accrual_usd_cents"`
	NetAfterAccrualCents int64          `json:"net_revenue_after_accruals_usd_cents"`
}

// Record is period_close.json, the file that every close writes and later closes roll up.
type Record struct {
	CloseType    Kind           `json:"close_type"`
	BatchID      string         `json:"batch_id"`
	BusinessDate string         `json:"business_date"`
	Summary      ledger.Summary `json:"summary"`
	Controls     []Control      `json:"controls"`
	Week         *Week          `json:"week,omitempty"`
	Month        *Month         `json:"month,omitempty"`
}

// Controls checks the day against what finance normally sees.
func Controls(s ledger.Summary) []Control {
	rate := func(part, whole int64) float64 {
		if whole == 0 {
			return 0
		}
		return float64(part) / float64(whole) * 100
	}
	check := func(name string, value, low, high float64) Control {
		status := "pass"
		if value < low || value > high {
			status = "warn"
		}
		return Control{name, status, fmt.Sprintf("%.2f%%, expected %.1f%% to %.1f%%", value, low, high)}
	}
	return []Control{
		check("refund_rate", rate(s.RefundsUSDCents, s.GrossSalesUSDCents), 0, 8),
		check("processor_fee_rate", rate(s.FeesUSDCents, s.GrossSalesUSDCents), 2, 4),
		check("held_revenue_share", rate(s.HeldRevenueUSDCents, s.GrossSalesUSDCents), 0, 2),
	}
}

// Prior reads the period_close.json of every finance batch except skipBatch, keeping the latest
// batch of each business date.
func Prior(skipBatch string) (map[string]Record, error) {
	root := filepath.Join(lake.Root(), lake.Team)
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return map[string]Record{}, nil
	}
	if err != nil {
		return nil, err
	}
	byDate := map[string]Record{}
	for _, entry := range entries {
		path := filepath.Join(root, entry.Name(), "period_close.json")
		if !entry.IsDir() || entry.Name() == skipBatch || !lake.Exists(path) {
			continue
		}
		var record Record
		if err := lake.ReadJSON(path, &record); err != nil {
			return nil, err
		}
		if known, ok := byDate[record.BusinessDate]; !ok || record.BatchID > known.BatchID {
			byDate[record.BusinessDate] = record
		}
	}
	return byDate, nil
}

func day(date string, record Record, closed bool) Day {
	if !closed {
		return Day{Date: date}
	}
	s := record.Summary
	return Day{
		Date: date, Closed: true, GrossSalesCents: s.GrossSalesUSDCents, NetRevenueCents: s.NetRevenueUSDCents,
		RefundsCents: s.RefundsUSDCents, FeesCents: s.FeesUSDCents, HeldCents: s.HeldRevenueUSDCents,
	}
}

func total(days []Day) Totals {
	t := Totals{MissingDates: []string{}}
	for _, d := range days {
		if !d.Closed {
			t.MissingDates = append(t.MissingDates, d.Date)
			continue
		}
		t.DaysClosed++
		t.GrossSalesCents += d.GrossSalesCents
		t.NetRevenueCents += d.NetRevenueCents
		t.RefundsCents += d.RefundsCents
		t.FeesCents += d.FeesCents
		t.HeldCents += d.HeldCents
	}
	return t
}

// BuildWeek rolls up the seven days that end on current's business date from the closes before it.
func BuildWeek(current Record, prior map[string]Record) (*Week, error) {
	end, err := time.Parse(time.DateOnly, current.BusinessDate)
	if err != nil {
		return nil, err
	}
	week := &Week{Start: end.AddDate(0, 0, -6).Format(time.DateOnly), End: current.BusinessDate}
	for i := 6; i >= 0; i-- {
		date := end.AddDate(0, 0, -i).Format(time.DateOnly)
		record, closed := prior[date]
		if date == current.BusinessDate {
			record, closed = current, true
		}
		week.Days = append(week.Days, day(date, record, closed))
	}
	week.Totals = total(week.Days)
	return week, nil
}

// BuildMonth accrues the processor fees that are invoiced after the month and sums the month so far.
func BuildMonth(current Record, prior map[string]Record) (*Month, error) {
	end, err := time.Parse(time.DateOnly, current.BusinessDate)
	if err != nil {
		return nil, err
	}
	first := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC)
	month := &Month{Month: first.Format("2006-01"), DaysInMonth: first.AddDate(0, 1, -1).Day()}
	var days []Day
	for d := first; !d.After(end); d = d.AddDate(0, 0, 1) {
		date := d.Format(time.DateOnly)
		record, closed := prior[date]
		if date == current.BusinessDate {
			record, closed = current, true
		}
		days = append(days, day(date, record, closed))
	}
	month.Totals = total(days)

	assessments := (month.Totals.GrossSalesCents*AssessmentBps + 5000) / 10000
	month.Accruals = []ledger.Entry{
		{ID: "JE-ME-01", Description: "Accrue the processor platform fee", Lines: []ledger.Line{
			{Account: ledger.AccountPlatformFee, Name: ledger.AccountName(ledger.AccountPlatformFee), DebitUSDCents: PlatformFeeUSDCents},
			{Account: ledger.AccountAccruedFees, Name: ledger.AccountName(ledger.AccountAccruedFees), CreditUSDCents: PlatformFeeUSDCents},
		}},
		{ID: "JE-ME-02", Description: "Accrue card network assessments on month-to-date volume", Lines: []ledger.Line{
			{Account: ledger.AccountAssessments, Name: ledger.AccountName(ledger.AccountAssessments), DebitUSDCents: assessments,
				Memo: fmt.Sprintf("%d bps of %d days", AssessmentBps, month.Totals.DaysClosed)},
			{Account: ledger.AccountAccruedFees, Name: ledger.AccountName(ledger.AccountAccruedFees), CreditUSDCents: assessments},
		}},
	}
	month.AccrualUSDCents = PlatformFeeUSDCents + assessments
	month.NetAfterAccrualCents = month.Totals.NetRevenueCents - month.AccrualUSDCents
	return month, nil
}
