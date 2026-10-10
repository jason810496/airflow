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

// Package closing is the mechanics of finance_revenue_close: reading the storefront batch, building
// the journal, writing the close files to the lake and rendering the task logs. The Dag file decides
// what happens and in which order, this package does it.
package closing

import (
	"coceuretail.example/finance/internal/lake"
	"coceuretail.example/finance/internal/ledger"
	"coceuretail.example/finance/internal/money"
	"coceuretail.example/finance/internal/period"
	"coceuretail.example/finance/internal/settlement"
)

// Logf writes one line to the task log.
type Logf func(format string, args ...any)

// FxSnapshot is fx_rates.json, the closing rates pinned before the journal is posted.
type FxSnapshot struct {
	BatchID string      `json:"batch_id"`
	Source  string      `json:"source"`
	Rates   money.Rates `json:"rates"`
}

// SalesLoad is what load_sales hands on: where the batch is and what the storefront said about it.
type SalesLoad struct {
	BatchDir        string      `json:"batch_dir"`
	BatchID         string      `json:"batch_id"`
	BusinessDate    string      `json:"business_date"`
	Orders          int         `json:"orders"`
	Refunds         int         `json:"refunds"`
	StorefrontGross int64       `json:"storefront_gross_usd_cents"`
	StorefrontNet   int64       `json:"storefront_net_usd_cents"`
	Suspicious      int         `json:"suspicious_orders"`
	Files           []lake.File `json:"files"`
}

// SettlementLoad is the settlement report of the processor and where it was stored.
type SettlementLoad struct {
	BatchDir   string            `json:"batch_dir"`
	ReportPath string            `json:"report_path"`
	Report     settlement.Report `json:"report"`
}

// Posted is what post_journal_entries hands on.
type Posted struct {
	BatchID         string            `json:"batch_id"`
	BusinessDate    string            `json:"business_date"`
	StorefrontBatch string            `json:"storefront_batch_dir"`
	JournalPath     string            `json:"journal_path"`
	Balanced        bool              `json:"balanced"`
	DebitsUSDCents  int64             `json:"debits_usd_cents"`
	CreditsUSDCents int64             `json:"credits_usd_cents"`
	RiskScreened    bool              `json:"risk_screened"`
	RiskMessage     string            `json:"risk_message"`
	Reconciliation  settlement.Result `json:"reconciliation"`
	Summary         ledger.Summary    `json:"summary"`
}

// Reconciled is true when debits equal credits and the settlement matches the expected cash.
func (p Posted) Reconciled() bool { return p.Balanced && p.Reconciliation.Matched }

// TrialBalanceResult is what publish_trial_balance hands on.
type TrialBalanceResult struct {
	BatchID         string `json:"batch_id"`
	BusinessDate    string `json:"business_date"`
	Path            string `json:"path"`
	DebitsUSDCents  int64  `json:"debits_usd_cents"`
	CreditsUSDCents int64  `json:"credits_usd_cents"`
}

// Incident is what open_recon_incident hands on.
type Incident struct {
	IncidentID string `json:"incident_id"`
	Severity   string `json:"severity"`
	Path       string `json:"path"`
}

// PeriodResult is what a period close hands on.
type PeriodResult struct {
	CloseType          period.Kind `json:"close_type"`
	Path               string      `json:"path"`
	NetRevenueUSDCents int64       `json:"net_revenue_usd_cents"`
}

// CloseSummary is what finalize_close writes and hands on.
type CloseSummary struct {
	Status   string `json:"status"`
	CloseDir string `json:"close_dir"`
}
