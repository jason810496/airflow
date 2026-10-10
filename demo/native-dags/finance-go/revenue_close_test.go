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

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"coceuretail.example/finance/internal/closing"
	"coceuretail.example/finance/internal/lake"
	"coceuretail.example/finance/internal/ledger"
	"coceuretail.example/finance/internal/period"
	"coceuretail.example/finance/internal/risk"
	"coceuretail.example/finance/internal/storefront"
)

// outcome is what one run of the Dag leaves behind.
type outcome struct {
	posted     closing.Posted
	balanced   bool
	kind       period.Kind
	financeDir string
	summary    map[string]any
}

// runClose runs the tasks in the order of the Dag, taking the branch that the If and the Switch
// choose, as the scheduler would.
func runClose(t *testing.T, e *env) outcome {
	t.Helper()
	actx := e.actx()
	require.NoError(t, snapshotFxRates(actx))
	sales, err := loadSales(actx)
	require.NoError(t, err)
	screening, err := loadRiskDecisions(actx)
	require.NoError(t, err)
	settled, err := loadSettlements(actx)
	require.NoError(t, err)
	posted, err := postJournalEntries(actx, sales, screening, settled)
	require.NoError(t, err)

	out := outcome{posted: posted, financeDir: lake.FinanceDir(posted.BatchID)}
	out.balanced, err = ledgerBalanced(actx, posted)
	require.NoError(t, err)
	if out.balanced {
		published, err := publishTrialBalance(actx, posted)
		require.NoError(t, err)
		closes := &closeTasks{daily: &airflow.TaskRef{}, week: &airflow.TaskRef{}, month: &airflow.TaskRef{}}
		chosen, err := closes.pick(actx, published)
		require.NoError(t, err)
		switch chosen {
		case closes.daily:
			out.kind = period.Daily
			_, err = dailyClose(actx, posted)
		case closes.week:
			out.kind = period.WeekEnd
			_, err = weekEndClose(actx, posted)
		case closes.month:
			out.kind = period.MonthEnd
			_, err = monthEndClose(actx, posted)
		}
		require.NoError(t, err)
	} else {
		_, err := openReconIncident(actx, posted)
		require.NoError(t, err)
	}
	_, err = finalizeClose(actx, posted)
	require.NoError(t, err)
	require.NoError(t, lake.ReadJSON(filepath.Join(out.financeDir, "close_summary.json"), &out.summary))
	return out
}

func TestBalancedDailyClose(t *testing.T) {
	e := newEnv(t)
	dir := e.storefrontBatch("2026-10-07", 60)
	out := runClose(t, e)

	assert.True(t, out.balanced)
	assert.True(t, out.posted.Balanced)
	assert.True(t, out.posted.Reconciliation.Matched)
	assert.Equal(t, period.Daily, out.kind)
	assert.False(t, out.posted.RiskScreened)
	assert.Equal(t, risk.NoScreening, out.posted.RiskMessage)

	assert.True(t, e.exists(out.financeDir, "trial_balance.json"))
	assert.False(t, e.exists(out.financeDir, "recon_incident.json"))
	assert.True(t, e.exists(out.financeDir, "period_close.json"))
	assert.Equal(t, "closed", out.summary["status"])
	assert.Equal(t, dir, out.summary["storefront_batch_dir"])
	assert.Equal(t, out.financeDir, e.client.variables[lake.FinanceCloseVariable])
	assert.Contains(t, e.logs.String(), "Trial balance for 2026-10-07")
	assert.Contains(t, e.logs.String(), "Journal for 2026-10-07")
}

func TestMismatchOpensIncident(t *testing.T) {
	e := newEnv(t)
	e.storefrontBatch("2026-10-07", 60)
	e.client.variables["finance.inject_mismatch"] = "true"
	out := runClose(t, e)

	assert.False(t, out.balanced)
	assert.True(t, out.posted.Balanced, "the ledger itself balances, the settlement does not")
	assert.False(t, out.posted.Reconciliation.Matched)
	assert.True(t, e.exists(out.financeDir, "recon_incident.json"))
	assert.False(t, e.exists(out.financeDir, "trial_balance.json"))
	assert.False(t, e.exists(out.financeDir, "period_close.json"))
	assert.Equal(t, "closed_with_incident", out.summary["status"])
	assert.Equal(t, "none", out.summary["close_type"])
	assert.Contains(t, e.logs.String(), "reconciliation incident")

	var incident struct {
		Status string           `json:"status"`
		Lines  []map[string]any `json:"discrepancy_lines"`
	}
	require.NoError(t, lake.ReadJSON(filepath.Join(out.financeDir, "recon_incident.json"), &incident))
	assert.Equal(t, "open", incident.Status)
	assert.NotEmpty(t, incident.Lines)
}

func TestRiskDecisionsChangeTheJournal(t *testing.T) {
	e := newEnv(t)
	dir := e.storefrontBatch("2026-10-07", 60)
	plain := runClose(t, e).posted.Summary

	e.writeDecisions(dir,
		map[string]any{"order_id": "ORD-20261007-00002", "action": "review"},
		map[string]any{"order_id": "ORD-20261007-00004", "action": "block"},
		map[string]any{"order_id": "ORD-20261007-00005", "action": "approve"},
		map[string]any{"order_id": "ORD-20261007-99999", "action": "block"},
	)
	out := runClose(t, e)
	s := out.posted.Summary

	assert.True(t, out.posted.RiskScreened)
	assert.True(t, out.balanced)
	assert.Equal(t, 1, s.HeldOrders)
	assert.Equal(t, 1, s.ReversedOrders)
	assert.Positive(t, s.HeldRevenueUSDCents)
	assert.Positive(t, s.ReversedUSDCents)
	assert.Equal(t, plain.GrossSalesUSDCents, s.GrossSalesUSDCents)
	assert.Equal(t, plain.RecognizedRevenueUSDCents-s.HeldRevenueUSDCents-s.ReversedUSDCents+s.GiftCardsUSDCents-plain.GiftCardsUSDCents,
		s.RecognizedRevenueUSDCents)
	assert.Equal(t, s.GrossSalesUSDCents-s.RefundsUSDCents-s.FeesUSDCents, s.ClearingUSDCents)
}

func TestRiskDecisionsForAnotherBatchAreIgnored(t *testing.T) {
	e := newEnv(t)
	e.storefrontBatch("2026-10-06", 20)
	e.writeDecisions(e.client.variables[lake.StorefrontBatchVariable],
		map[string]any{"order_id": "ORD-20261006-00002", "action": "block"})
	e.storefrontBatch("2026-10-07", 20)
	out := runClose(t, e)

	assert.False(t, out.posted.RiskScreened)
	assert.Zero(t, out.posted.Summary.ReversedOrders)
}

func TestCloseTypes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		date  string
		force string
		want  period.Kind
	}{
		{"weekday", "2026-10-07", "", period.Daily},
		{"sunday", "2026-10-11", "", period.WeekEnd},
		{"last day of month", "2026-10-31", "", period.MonthEnd},
		{"forced", "2026-10-07", "true", period.MonthEnd},
		{"month end on a sunday", "2026-05-31", "", period.MonthEnd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.storefrontBatch(tc.date, 30)
			if tc.force != "" {
				e.client.variables["finance.force_month_end"] = tc.force
			}
			out := runClose(t, e)
			assert.Equal(t, tc.want, out.kind)
			assert.Equal(t, string(tc.want), out.summary["close_type"])
		})
	}
}

func TestWeekEndRollsUpPriorCloses(t *testing.T) {
	e := newEnv(t)
	var net int64
	for _, date := range []string{"2026-10-05", "2026-10-06", "2026-10-08", "2026-10-10"} {
		e.storefrontBatch(date, 25)
		net += runClose(t, e).posted.Summary.NetRevenueUSDCents
	}
	e.storefrontBatch("2026-10-11", 25)
	out := runClose(t, e)
	require.Equal(t, period.WeekEnd, out.kind)

	var record period.Record
	require.NoError(t, lake.ReadJSON(filepath.Join(out.financeDir, "period_close.json"), &record))
	require.NotNil(t, record.Week)
	assert.Equal(t, "2026-10-05", record.Week.Start)
	assert.Equal(t, 5, record.Week.Totals.DaysClosed)
	assert.Equal(t, []string{"2026-10-07", "2026-10-09"}, record.Week.Totals.MissingDates)
	assert.Equal(t, net+out.posted.Summary.NetRevenueUSDCents, record.Week.Totals.NetRevenueCents)
}

func TestMonthEndAccruesProcessorFees(t *testing.T) {
	e := newEnv(t)
	var gross int64
	for _, date := range []string{"2026-10-29", "2026-10-30"} {
		e.storefrontBatch(date, 25)
		gross += runClose(t, e).posted.Summary.GrossSalesUSDCents
	}
	e.storefrontBatch("2026-10-31", 25)
	out := runClose(t, e)
	require.Equal(t, period.MonthEnd, out.kind)
	gross += out.posted.Summary.GrossSalesUSDCents

	var record period.Record
	require.NoError(t, lake.ReadJSON(filepath.Join(out.financeDir, "period_close.json"), &record))
	month := record.Month
	require.NotNil(t, month)
	assert.Equal(t, "2026-10", month.Month)
	assert.Equal(t, 31, month.DaysInMonth)
	assert.Equal(t, 3, month.Totals.DaysClosed)
	assessments := (gross*period.AssessmentBps + 5000) / 10000
	assert.Equal(t, period.PlatformFeeUSDCents+assessments, month.AccrualUSDCents)
	for _, entry := range month.Accruals {
		var debits, credits int64
		for _, l := range entry.Lines {
			debits += l.DebitUSDCents
			credits += l.CreditUSDCents
		}
		assert.Equal(t, debits, credits, entry.ID)
	}
	assert.Equal(t, ledger.AccountAccruedFees, month.Accruals[0].Lines[1].Account)
}

func TestPostRefusesAChangedBatch(t *testing.T) {
	e := newEnv(t)
	dir := e.storefrontBatch("2026-10-07", 20)
	actx := e.actx()
	require.NoError(t, snapshotFxRates(actx))
	sales, err := loadSales(actx)
	require.NoError(t, err)
	settled, err := loadSettlements(actx)
	require.NoError(t, err)

	path := filepath.Join(dir, "refunds.json")
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte(strings.Replace(string(content), "changed_mind", "damaged", 1)), 0o644))

	_, err = postJournalEntries(actx, sales, risk.Screening{Message: risk.NoScreening}, settled)
	assert.ErrorContains(t, err, "refunds.json changed after load_sales read it")
}

func TestPostNeedsTheFxSnapshot(t *testing.T) {
	e := newEnv(t)
	e.storefrontBatch("2026-10-07", 20)
	actx := e.actx()
	sales, err := loadSales(actx)
	require.NoError(t, err)
	settled, err := loadSettlements(actx)
	require.NoError(t, err)

	_, err = postJournalEntries(actx, sales, risk.Screening{}, settled)
	assert.ErrorContains(t, err, "snapshot_fx_rates has not pinned the rates")
}

// The runtime hands each result to the next task as JSON, so every result has to survive that.
func TestResultsSurviveJSON(t *testing.T) {
	e := newEnv(t)
	e.storefrontBatch("2026-10-07", 30)
	e.writeDecisions(e.client.variables[lake.StorefrontBatchVariable],
		map[string]any{"order_id": "ORD-20261007-00002", "action": "review", "reasons": []string{"card_velocity"}})
	actx := e.actx()
	require.NoError(t, snapshotFxRates(actx))
	sales, err := loadSales(actx)
	require.NoError(t, err)
	screening, err := loadRiskDecisions(actx)
	require.NoError(t, err)
	settled, err := loadSettlements(actx)
	require.NoError(t, err)
	posted, err := postJournalEntries(actx, sales, screening, settled)
	require.NoError(t, err)
	published, err := publishTrialBalance(actx, posted)
	require.NoError(t, err)

	roundTrip := func(in, out any) {
		raw, err := json.Marshal(in)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, out))
		assert.Equal(t, in, reflect.ValueOf(out).Elem().Interface())
	}
	roundTrip(sales, new(closing.SalesLoad))
	roundTrip(screening, new(risk.Screening))
	roundTrip(settled, new(closing.SettlementLoad))
	roundTrip(posted, new(closing.Posted))
	roundTrip(published, new(closing.TrialBalanceResult))
}

func TestDagShape(t *testing.T) {
	bundle := airflow.Bundle()
	assert.NotPanics(t, func() { bundle.Register(newRevenueCloseDag()) })
}

// TestRealStorefrontBatch closes a batch that storefront_daily_orders wrote, when the lake of such a
// batch is named in COCEU_TEST_LAKE.
func TestRealStorefrontBatch(t *testing.T) {
	root := os.Getenv("COCEU_TEST_LAKE")
	if root == "" {
		t.Skip("COCEU_TEST_LAKE is not set")
	}
	matches, err := filepath.Glob(filepath.Join(root, "storefront", "*"))
	require.NoError(t, err)
	require.NotEmpty(t, matches)

	for _, inject := range []string{"false", "true"} {
		e := newEnv(t)
		t.Setenv("COCEU_LAKE_ROOT", root)
		e.client.variables[lake.StorefrontBatchVariable] = matches[0]
		e.client.variables["finance.inject_mismatch"] = inject
		out := runClose(t, e)
		batch, err := storefront.Load(matches[0])
		require.NoError(t, err)
		assert.Equal(t, inject == "false", out.balanced, "inject_mismatch=%s", inject)
		assert.True(t, out.posted.Balanced)
		t.Logf("inject_mismatch=%s, %d orders, variance %d cents\n%s", inject, len(batch.Orders),
			out.posted.Reconciliation.VarianceUSDCents, e.logs.String())
	}
}
