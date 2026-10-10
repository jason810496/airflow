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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"

	"coceuretail.example/finance/internal/lake"
	"coceuretail.example/finance/internal/ledger"
	"coceuretail.example/finance/internal/money"
	"coceuretail.example/finance/internal/report"
	"coceuretail.example/finance/internal/risk"
	"coceuretail.example/finance/internal/settlement"
	"coceuretail.example/finance/internal/storefront"
)

type journalFile struct {
	ledger.Journal
	StorefrontBatch string            `json:"storefront_batch_dir"`
	Risk            risk.Screening    `json:"risk"`
	Reconciliation  settlement.Result `json:"settlement_reconciliation"`
}

// PostJournal builds the journal of the batch at the pinned closing rates, reconciles the
// settlement with the expected cash and writes journal.json. It removes the files of an earlier run
// of the same batch, so that finalize_close cannot find them.
func PostJournal(
	sales SalesLoad, screening risk.Screening, settled SettlementLoad, toleranceUSDCents int64, log Logf,
) (Posted, error) {
	if sales.BatchDir != settled.BatchDir {
		return Posted{}, fmt.Errorf("the loads read different batches: %s and %s", sales.BatchDir, settled.BatchDir)
	}
	batch, err := storefront.Load(sales.BatchDir)
	if err != nil {
		return Posted{}, err
	}
	for i, f := range batch.Files {
		if f != sales.Files[i] {
			return Posted{}, fmt.Errorf("%s changed after load_sales read it", f.Name)
		}
	}

	var snapshot FxSnapshot
	financeDir := lake.FinanceDir(batch.ID)
	if err := lake.ReadJSON(filepath.Join(financeDir, "fx_rates.json"), &snapshot); err != nil {
		return Posted{}, fmt.Errorf("snapshot_fx_rates has not pinned the rates: %w", err)
	}
	if snapshot.BatchID != batch.ID {
		return Posted{}, fmt.Errorf("the pinned rates are for batch %s, not %s", snapshot.BatchID, batch.ID)
	}

	journal, err := ledger.Build(ledger.Input{
		Batch: batch, Screening: screening, Settlement: settled.Report, Rates: snapshot.Rates,
	})
	if err != nil {
		return Posted{}, err
	}
	expected, err := settlement.Expected(batch, settled.Report.Meta)
	if err != nil {
		return Posted{}, err
	}
	reconciliation := settlement.Reconcile(expected, settled.Report, snapshot.Rates, toleranceUSDCents)

	if err := lake.Remove(financeDir, "trial_balance.json", "recon_incident.json", "period_close.json", "close_summary.json"); err != nil {
		return Posted{}, err
	}
	file, err := lake.WriteJSON(financeDir, "journal.json", journalFile{
		Journal: *journal, StorefrontBatch: batch.Dir, Risk: screening, Reconciliation: reconciliation,
	})
	if err != nil {
		return Posted{}, err
	}

	s := journal.Summary
	log("Journal for %s, closing rates applied\n%s", batch.BusinessDate, ledger.Table(journal.Entries))
	log("debits %s, credits %s, balanced=%t\nstorefront reported %s gross at its own rates, finance books %s at closing rates\n"+
		"recognized revenue %s, held %s (%d orders), reversed %s (%d orders), refunds %s, processor fees %s, net revenue %s",
		money.FormatUSD(journal.DebitsUSDCents), money.FormatUSD(journal.CreditsUSDCents), journal.Balanced(),
		money.FormatUSD(sales.StorefrontGross), money.FormatUSD(s.GrossSalesUSDCents),
		money.FormatUSD(s.RecognizedRevenueUSDCents), money.FormatUSD(s.HeldRevenueUSDCents), s.HeldOrders,
		money.FormatUSD(s.ReversedUSDCents), s.ReversedOrders, money.FormatUSD(s.RefundsUSDCents),
		money.FormatUSD(s.FeesUSDCents), money.FormatUSD(s.NetRevenueUSDCents))
	log("Settlement versus expected cash: variance %s, tolerance %s, %d differences",
		money.FormatUSD(reconciliation.VarianceUSDCents), money.FormatUSD(toleranceUSDCents), len(reconciliation.Differences))

	return Posted{
		BatchID: batch.ID, BusinessDate: batch.BusinessDate, StorefrontBatch: batch.Dir,
		JournalPath: filepath.Join(financeDir, file.Name), Balanced: journal.Balanced(),
		DebitsUSDCents: journal.DebitsUSDCents, CreditsUSDCents: journal.CreditsUSDCents,
		RiskScreened: screening.Screened, RiskMessage: screening.Message,
		Reconciliation: reconciliation, Summary: s,
	}, nil
}

// PublishTrialBalance writes the trial balance of the posted journal.
func PublishTrialBalance(posted Posted, log Logf) (TrialBalanceResult, error) {
	var journal ledger.Journal
	if err := lake.ReadJSON(posted.JournalPath, &journal); err != nil {
		return TrialBalanceResult{}, err
	}
	tb := ledger.NewTrialBalance(journal.Entries)
	file, err := lake.WriteJSON(filepath.Dir(posted.JournalPath), "trial_balance.json", struct {
		BatchID      string `json:"batch_id"`
		BusinessDate string `json:"business_date"`
		ledger.TrialBalance
	}{posted.BatchID, posted.BusinessDate, tb})
	if err != nil {
		return TrialBalanceResult{}, err
	}
	log("Trial balance for %s\n%s", posted.BusinessDate, tb.Table())
	return TrialBalanceResult{
		BatchID: posted.BatchID, BusinessDate: posted.BusinessDate,
		Path:           filepath.Join(filepath.Dir(posted.JournalPath), file.Name),
		DebitsUSDCents: tb.DebitsUSDCents, CreditsUSDCents: tb.CreditsUSDCents,
	}, nil
}

// highSeverityVarianceUSDCents is the settlement variance from which an incident is high severity.
const highSeverityVarianceUSDCents = 10_000

// OpenIncident writes recon_incident.json for a journal that is out of balance or a settlement that
// does not match. The incident id is stable for a batch.
func OpenIncident(posted Posted, log Logf) (Incident, error) {
	sum := sha256.Sum256([]byte(posted.BatchID))
	id := fmt.Sprintf("RECON-%s-%s", strings.ReplaceAll(posted.BusinessDate, "-", ""), hex.EncodeToString(sum[:])[:6])
	severity := "medium"
	if posted.Reconciliation.VarianceUSDCents >= highSeverityVarianceUSDCents {
		severity = "high"
	}
	reasons := []string{}
	if !posted.Balanced {
		reasons = append(reasons, fmt.Sprintf("debits %s do not equal credits %s",
			money.FormatUSD(posted.DebitsUSDCents), money.FormatUSD(posted.CreditsUSDCents)))
	}
	if !posted.Reconciliation.Matched {
		reasons = append(reasons, fmt.Sprintf("settlement variance %s is above the tolerance of %s",
			money.FormatUSD(posted.Reconciliation.VarianceUSDCents), money.FormatUSD(posted.Reconciliation.ToleranceUSDCents)))
	}
	file, err := lake.WriteJSON(filepath.Dir(posted.JournalPath), "recon_incident.json", map[string]any{
		"incident_id":          id,
		"status":               "open",
		"severity":             severity,
		"batch_id":             posted.BatchID,
		"business_date":        posted.BusinessDate,
		"storefront_batch_dir": posted.StorefrontBatch,
		"reasons":              reasons,
		"ledger": map[string]any{
			"debits_usd_cents": posted.DebitsUSDCents, "credits_usd_cents": posted.CreditsUSDCents,
		},
		"variance_usd_cents":  posted.Reconciliation.VarianceUSDCents,
		"tolerance_usd_cents": posted.Reconciliation.ToleranceUSDCents,
		"discrepancy_lines":   posted.Reconciliation.Differences,
		"next_steps": []string{
			"ask the processor to resend the settlement report for the business date",
			"compare the captures with the order export, and dispute the fee lines above the contract tariff",
			"rerun finance_revenue_close once the report is corrected",
		},
	})
	if err != nil {
		return Incident{}, err
	}
	rows := make([][]string, len(posted.Reconciliation.Differences))
	for i, d := range posted.Reconciliation.Differences {
		expected, reported, diff := fmt.Sprint(d.ExpectedMinor), fmt.Sprint(d.ReportedMinor), fmt.Sprint(d.DiffMinor)
		usd := ""
		if d.Item != "captures" {
			expected = money.FormatMinor(d.ExpectedMinor, d.Currency)
			reported = money.FormatMinor(d.ReportedMinor, d.Currency)
			diff = money.FormatMinor(d.DiffMinor, d.Currency)
			usd = money.FormatUSD(d.DiffUSDCents)
		}
		rows[i] = []string{d.Currency, d.Item, expected, reported, diff, usd}
	}
	log("Opened %s (%s): %s\n%s", id, severity, strings.Join(reasons, "; "),
		report.Table([]string{"currency", "item", "expected", "reported", "difference", "usd"}, rows, 2, 3, 4, 5))
	return Incident{IncidentID: id, Severity: severity, Path: filepath.Join(filepath.Dir(posted.JournalPath), file.Name)}, nil
}
