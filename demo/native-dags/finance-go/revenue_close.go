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

// finance_revenue_close: the finance team's close of a storefront day.
//
// Triggered by the storefront, never scheduled. It reconciles the day's orders with the risk
// decisions and the payment processor's settlement report, posts the journal, publishes the trial
// balance or opens a reconciliation incident, runs the period close that the business date calls
// for, and then asks the storefront to invoice the closed day.

package main

import (
	"fmt"
	"time"

	"github.com/apache/airflow/go-sdk/airflow"

	"coceuretail.example/finance/internal/closing"
	"coceuretail.example/finance/internal/lake"
	"coceuretail.example/finance/internal/period"
	"coceuretail.example/finance/internal/risk"
)

const docMD = `
### Finance revenue close

Triggered by the storefront with the batch to close in the Variable
` + "`handoff.storefront.latest_batch`" + `. Everything is written to
` + "`/files/demo/lake/finance/<batch>/`" + `.

* ` + "`reconcile`" + `: loads the sales, the risk decisions (when risk screened the batch) and the
  processor's settlement report, which is derived from the orders.
* ` + "`snapshot_fx_rates`" + `: pins the closing rates before the journal is posted.
* ` + "`post_journal_entries`" + `: double-entry journal in US cents, with held revenue for orders under
  review and a reversal for blocked orders.
* ` + "`ledger_balanced`" + `: publishes the trial balance, or opens a reconciliation incident when the
  settlement does not match the expected cash.
* ` + "`close_type`" + `: daily close, week-end close (Sunday) or month-end close (last day of the month).
* ` + "`finalize_close`" + `: writes ` + "`close_summary.json`" + `, sets ` + "`handoff.finance.latest_close`" + ` and
  lets ` + "`trigger_customer_invoices`" + ` start ` + "`storefront_customer_invoices`" + `.

Knobs: Variables ` + "`finance.fx_rates`" + `, ` + "`finance.force_month_end`" + ` and
` + "`finance.inject_mismatch`" + ` (` + "`true`" + ` makes the processor misreport the settlement).
`

func newRevenueCloseDag() *airflow.DagRef {
	dag := airflow.Dag("finance_revenue_close", airflow.DagSpec{
		Description:          "Reconcile, journal and close a storefront day",
		DocMD:                docMD,
		Queue:                "golang",
		Tags:                 []string{"finance", "go"},
		StartDate:            time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Catchup:              ptr(false),
		MaxActiveRuns:        1,
		IsPausedUponCreation: ptr(false),
	})
	retry := func(taskID string) airflow.TaskSpec {
		return airflow.TaskSpec{TaskID: taskID, Retries: 2, RetryDelay: ptr(15 * time.Second)}
	}

	snapshot := dag.Task(snapshotFxRates, airflow.TaskSpec{TaskID: "snapshot_fx_rates"})

	reconcile := dag.TaskGroup("reconcile")
	sales := reconcile.Task(loadSales, retry("load_sales"))
	screening := reconcile.Task(loadRiskDecisions, retry("load_risk_decisions"))
	settled := reconcile.Task(loadSettlements, retry("load_settlements"))

	posted := dag.Task(
		postJournalEntries,
		airflow.TaskSpec{TaskID: "post_journal_entries"},
		airflow.Inputs(sales, screening, settled),
	)
	snapshot.Before(airflow.Label(posted, "rates pinned"))

	published := dag.Task(
		publishTrialBalance,
		airflow.TaskSpec{TaskID: "publish_trial_balance"},
		airflow.Inputs(posted),
	)
	incident := dag.Task(
		openReconIncident,
		airflow.TaskSpec{TaskID: "open_recon_incident"},
		airflow.Inputs(posted),
	)
	dag.If(ledgerBalanced, airflow.TaskSpec{TaskID: "ledger_balanced"}, airflow.Inputs(posted)).
		Then(published).
		Else(incident)

	closes := &closeTasks{
		daily: dag.Task(dailyClose, airflow.TaskSpec{TaskID: "daily_close"}, airflow.Inputs(posted)),
		week:  dag.Task(weekEndClose, airflow.TaskSpec{TaskID: "week_end_close"}, airflow.Inputs(posted)),
		month: dag.Task(monthEndClose, airflow.TaskSpec{TaskID: "month_end_close"}, airflow.Inputs(posted)),
	}
	dag.Switch(closes.pick, airflow.TaskSpec{TaskID: "close_type"}, airflow.Inputs(published)).
		Case(closes.daily).
		Case(closes.week).
		Case(closes.month)

	// The If and the Switch each skip a side, so the join runs when nothing failed and one task ran.
	finalized := dag.Task(
		finalizeClose,
		airflow.TaskSpec{TaskID: "finalize_close", TriggerRule: airflow.TriggerRuleNoneFailedMinOneSuccess},
		airflow.Inputs(posted),
	)
	finalized.After(published, incident, closes.daily, closes.week, closes.month)

	trigger := dag.Task(
		airflow.TriggerDagRun(airflow.TriggerDagRunSpec{
			DagID: "storefront_customer_invoices",
			Conf: map[string]any{
				"requested_by": "finance",
				"contract":     lake.FinanceCloseVariable,
			},
		}),
		airflow.TaskSpec{TaskID: "trigger_customer_invoices"},
	)
	finalized.Before(trigger)

	return dag
}

// snapshotFxRates pins the closing rates. It passes nothing on: post_journal_entries reads the file,
// so it has to run first.
func snapshotFxRates(actx airflow.Context) error {
	dir, err := storefrontBatchDir(actx)
	if err != nil {
		return err
	}
	rates, _, err := variable(actx, "finance.fx_rates")
	if err != nil {
		return err
	}
	return closing.PinRates(dir, rates, logger(actx))
}

func loadSales(actx airflow.Context) (closing.SalesLoad, error) {
	dir, err := storefrontBatchDir(actx)
	if err != nil {
		return closing.SalesLoad{}, err
	}
	return closing.LoadSales(dir, logger(actx))
}

func loadRiskDecisions(actx airflow.Context) (risk.Screening, error) {
	dir, err := storefrontBatchDir(actx)
	if err != nil {
		return risk.Screening{}, err
	}
	decisions, _, err := variable(actx, lake.RiskDecisionsVariable)
	if err != nil {
		return risk.Screening{}, err
	}
	return closing.LoadScreening(decisions, dir, logger(actx))
}

func loadSettlements(actx airflow.Context) (closing.SettlementLoad, error) {
	dir, err := storefrontBatchDir(actx)
	if err != nil {
		return closing.SettlementLoad{}, err
	}
	conn, err := actx.Client().GetConnection(actx, "payments_gateway")
	if err != nil {
		return closing.SettlementLoad{}, fmt.Errorf("read Connection payments_gateway: %w", err)
	}
	inject, err := flag(actx, "finance.inject_mismatch")
	if err != nil {
		return closing.SettlementLoad{}, err
	}
	return closing.LoadSettlements(dir, closing.ProcessorOf(conn.Host, conn.Login), inject, logger(actx))
}

// toleranceUSDCents is how far the processor's net payout may differ from the expected one.
const toleranceUSDCents = 100

func postJournalEntries(
	actx airflow.Context, sales closing.SalesLoad, screening risk.Screening, settled closing.SettlementLoad,
) (closing.Posted, error) {
	return closing.PostJournal(sales, screening, settled, toleranceUSDCents, logger(actx))
}

// ledgerBalanced decides between the trial balance and a reconciliation incident.
func ledgerBalanced(actx airflow.Context, posted closing.Posted) (bool, error) {
	ok := posted.Reconciled()
	logf(actx, "debits equal credits: %t, settlement matches expected cash: %t, so %s",
		posted.Balanced, posted.Reconciliation.Matched,
		map[bool]string{true: "publishing the trial balance", false: "opening a reconciliation incident"}[ok])
	return ok, nil
}

func publishTrialBalance(actx airflow.Context, posted closing.Posted) (closing.TrialBalanceResult, error) {
	return closing.PublishTrialBalance(posted, logger(actx))
}

func openReconIncident(actx airflow.Context, posted closing.Posted) (closing.Incident, error) {
	return closing.OpenIncident(posted, logger(actx))
}

// closeTasks holds the tasks that close_type chooses from, because a decider returns a *TaskRef.
type closeTasks struct {
	daily, week, month *airflow.TaskRef
}

// pick chooses the close for the business date. It takes the published trial balance so that it
// runs only for a day that reconciled.
func (c *closeTasks) pick(actx airflow.Context, published closing.TrialBalanceResult) (*airflow.TaskRef, error) {
	force, err := flag(actx, "finance.force_month_end")
	if err != nil {
		return nil, err
	}
	kind, err := period.Classify(published.BusinessDate, force)
	if err != nil {
		return nil, err
	}
	logf(actx, "%s is a %s, finance.force_month_end=%t", published.BusinessDate, kind, force)
	switch kind {
	case period.MonthEnd:
		return c.month, nil
	case period.WeekEnd:
		return c.week, nil
	default:
		return c.daily, nil
	}
}

func dailyClose(actx airflow.Context, posted closing.Posted) (closing.PeriodResult, error) {
	return closing.DailyClose(posted, logger(actx))
}

func weekEndClose(actx airflow.Context, posted closing.Posted) (closing.PeriodResult, error) {
	return closing.WeekEndClose(posted, logger(actx))
}

func monthEndClose(actx airflow.Context, posted closing.Posted) (closing.PeriodResult, error) {
	return closing.MonthEndClose(posted, logger(actx))
}

// finalizeClose writes the summary the storefront invoices from, then publishes where it is.
func finalizeClose(actx airflow.Context, posted closing.Posted) (closing.CloseSummary, error) {
	ti := actx.TaskInstance()
	summary, err := closing.WriteCloseSummary(posted, ti.DagID, ti.RunID, logger(actx))
	if err != nil {
		return closing.CloseSummary{}, err
	}
	if err := actx.Client().SetVariable(actx, lake.FinanceCloseVariable, summary.CloseDir,
		"Latest finance close directory in the lake. Written by finance_revenue_close."); err != nil {
		return closing.CloseSummary{}, fmt.Errorf("set Variable %s: %w", lake.FinanceCloseVariable, err)
	}
	logf(actx, "Set Variable %s to %s", lake.FinanceCloseVariable, summary.CloseDir)
	return summary, nil
}
