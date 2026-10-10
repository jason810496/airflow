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

package closing

import (
	"fmt"
	"path/filepath"

	"coceuretail.example/finance/internal/lake"
	"coceuretail.example/finance/internal/ledger"
	"coceuretail.example/finance/internal/money"
	"coceuretail.example/finance/internal/period"
	"coceuretail.example/finance/internal/report"
)

func writePeriod(posted Posted, record period.Record) (PeriodResult, error) {
	file, err := lake.WriteJSON(filepath.Dir(posted.JournalPath), "period_close.json", record)
	if err != nil {
		return PeriodResult{}, err
	}
	return PeriodResult{
		CloseType: record.CloseType, Path: filepath.Join(filepath.Dir(posted.JournalPath), file.Name),
		NetRevenueUSDCents: posted.Summary.NetRevenueUSDCents,
	}, nil
}

func newRecord(posted Posted, kind period.Kind) period.Record {
	return period.Record{
		CloseType: kind, BatchID: posted.BatchID, BusinessDate: posted.BusinessDate,
		Summary: posted.Summary, Controls: period.Controls(posted.Summary),
	}
}

func controlRows(controls []period.Control) [][]string {
	rows := make([][]string, len(controls))
	for i, c := range controls {
		rows[i] = []string{c.Name, c.Status, c.Detail}
	}
	return rows
}

// DailyClose records the day's controls.
func DailyClose(posted Posted, log Logf) (PeriodResult, error) {
	record := newRecord(posted, period.Daily)
	result, err := writePeriod(posted, record)
	if err != nil {
		return PeriodResult{}, err
	}
	log("Daily close for %s, net revenue %s\n%s", posted.BusinessDate, money.FormatUSD(posted.Summary.NetRevenueUSDCents),
		report.Table([]string{"control", "status", "detail"}, controlRows(record.Controls)))
	return result, nil
}

func dayRows(days []period.Day) [][]string {
	rows := make([][]string, len(days))
	for i, d := range days {
		if !d.Closed {
			rows[i] = []string{d.Date, "no close", "", "", "", ""}
			continue
		}
		rows[i] = []string{
			d.Date, "closed", money.FormatUSD(d.GrossSalesCents), money.FormatUSD(d.RefundsCents),
			money.FormatUSD(d.FeesCents), money.FormatUSD(d.NetRevenueCents),
		}
	}
	return rows
}

// WeekEndClose rolls up the week from the daily closes that earlier runs left in the lake.
func WeekEndClose(posted Posted, log Logf) (PeriodResult, error) {
	record := newRecord(posted, period.WeekEnd)
	prior, err := period.Prior(posted.BatchID)
	if err != nil {
		return PeriodResult{}, err
	}
	if record.Week, err = period.BuildWeek(record, prior); err != nil {
		return PeriodResult{}, err
	}
	result, err := writePeriod(posted, record)
	if err != nil {
		return PeriodResult{}, err
	}
	w := record.Week
	log("Week %s to %s, %d of 7 days closed\n%s\nweek net revenue %s",
		w.Start, w.End, w.Totals.DaysClosed,
		report.Table([]string{"date", "state", "gross", "refunds", "fees", "net revenue"}, dayRows(w.Days), 2, 3, 4, 5),
		money.FormatUSD(w.Totals.NetRevenueCents))
	return result, nil
}

// MonthEndClose rolls up the month to date and accrues the processor fees that are invoiced in
// arrears.
func MonthEndClose(posted Posted, log Logf) (PeriodResult, error) {
	record := newRecord(posted, period.MonthEnd)
	prior, err := period.Prior(posted.BatchID)
	if err != nil {
		return PeriodResult{}, err
	}
	if record.Month, err = period.BuildMonth(record, prior); err != nil {
		return PeriodResult{}, err
	}
	result, err := writePeriod(posted, record)
	if err != nil {
		return PeriodResult{}, err
	}
	m := record.Month
	log("Month %s, %d of %d days closed, month-to-date net revenue %s\nAccrued processor fees\n%s\nnet revenue after accruals %s",
		m.Month, m.Totals.DaysClosed, m.DaysInMonth, money.FormatUSD(m.Totals.NetRevenueCents),
		ledger.Table(m.Accruals), money.FormatUSD(m.NetAfterAccrualCents))
	return result, nil
}

// WriteCloseSummary writes close_summary.json, the file the storefront reads to invoice the day.
func WriteCloseSummary(posted Posted, dagID, runID string, log Logf) (CloseSummary, error) {
	dir := filepath.Dir(posted.JournalPath)
	status, closeType := "closed", "none"
	if posted.Reconciled() {
		var record period.Record
		if err := lake.ReadJSON(filepath.Join(dir, "period_close.json"), &record); err != nil {
			return CloseSummary{}, fmt.Errorf("no period close ran: %w", err)
		}
		closeType = string(record.CloseType)
	} else {
		status = "closed_with_incident"
	}
	files, err := lake.Digests(dir, "close_summary.json")
	if err != nil {
		return CloseSummary{}, err
	}
	if _, err := lake.WriteJSON(dir, "close_summary.json", map[string]any{
		"team":                 lake.Team,
		"batch_id":             posted.BatchID,
		"business_date":        posted.BusinessDate,
		"storefront_batch_dir": posted.StorefrontBatch,
		"status":               status,
		"close_type":           closeType,
		"ledger_balanced":      posted.Balanced,
		"settlement_matched":   posted.Reconciliation.Matched,
		"risk_screened":        posted.RiskScreened,
		"risk_message":         posted.RiskMessage,
		"summary":              posted.Summary,
		"produced_by":          dagID,
		"run_id":               runID,
		"files":                files,
	}); err != nil {
		return CloseSummary{}, err
	}
	rows := make([][]string, len(files))
	for i, f := range files {
		rows[i] = []string{f.Name, fmt.Sprint(f.Bytes), f.SHA256[:16]}
	}
	log("Closed %s as %s (%s)\n%s", posted.BatchID, status, closeType,
		report.Table([]string{"file", "bytes", "sha256"}, rows, 1))
	return CloseSummary{Status: status, CloseDir: dir}, nil
}
