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
	"strings"

	"coceuretail.example/finance/internal/lake"
	"coceuretail.example/finance/internal/money"
	"coceuretail.example/finance/internal/report"
	"coceuretail.example/finance/internal/risk"
	"coceuretail.example/finance/internal/settlement"
	"coceuretail.example/finance/internal/storefront"
)

// PinRates writes the closing rates of the Variable finance.fx_rates next to the batch.
func PinRates(batchDir, rawRates string, log Logf) error {
	rates, err := money.ParseRates(rawRates)
	if err != nil {
		return err
	}
	batchID := filepath.Base(batchDir)
	if _, err := lake.WriteJSON(lake.FinanceDir(batchID), "fx_rates.json", FxSnapshot{
		BatchID: batchID, Source: "Variable finance.fx_rates", Rates: rates,
	}); err != nil {
		return err
	}
	rows := make([][]string, 0, len(money.DefaultRates))
	for _, currency := range []string{"USD", "EUR", "GBP", "JPY"} {
		rows = append(rows, []string{currency, fmt.Sprint(rates[currency])})
	}
	log("Closing rates for %s, US dollars per unit\n%s", batchID, report.Table([]string{"currency", "rate"}, rows, 1))
	return nil
}

// LoadSales reads the storefront batch in dir and notes what the storefront reported about it.
func LoadSales(dir string, log Logf) (SalesLoad, error) {
	batch, err := storefront.Load(dir)
	if err != nil {
		return SalesLoad{}, err
	}
	rows := make([][]string, len(batch.Files))
	for i, f := range batch.Files {
		rows[i] = []string{f.Name, fmt.Sprint(f.Bytes), f.SHA256[:16]}
	}
	log("Loaded batch %s for %s from %s\n%s", batch.ID, batch.BusinessDate, dir,
		report.Table([]string{"file", "bytes", "sha256"}, rows, 1))
	log("%d orders and %d refunds, the storefront reports %s gross and %s net at its own rates, %d orders flagged",
		len(batch.Orders), len(batch.Refunds), money.FormatUSD(batch.Summary.GrossUSDCents),
		money.FormatUSD(batch.Summary.NetUSDCents), batch.Summary.SuspiciousCount)
	return SalesLoad{
		BatchDir: dir, BatchID: batch.ID, BusinessDate: batch.BusinessDate,
		Orders: len(batch.Orders), Refunds: len(batch.Refunds),
		StorefrontGross: batch.Summary.GrossUSDCents, StorefrontNet: batch.Summary.NetUSDCents,
		Suspicious: batch.Summary.SuspiciousCount, Files: batch.Files,
	}, nil
}

// LoadScreening reads the risk decisions in decisionsDir when they screened the batch in batchDir.
func LoadScreening(decisionsDir, batchDir string, log Logf) (risk.Screening, error) {
	screening, err := risk.Load(decisionsDir, batchDir)
	if err != nil {
		return risk.Screening{}, err
	}
	log("%s", screening.Message)
	if screening.Screened {
		rows := [][]string{}
		for _, d := range screening.Decisions {
			if d.Action != risk.Approve {
				rows = append(rows, []string{d.OrderID, string(d.Action), strings.Join(d.Reasons, ",")})
			}
		}
		if len(rows) > 0 {
			log("Orders that risk did not approve\n%s", report.Table([]string{"order", "action", "reasons"}, rows))
		}
	}
	return screening, nil
}

// ProcessorOf names the payment processor and the merchant account of a Connection.
func ProcessorOf(host string, login *string) settlement.Meta {
	meta := settlement.Meta{Processor: host, Account: "unknown"}
	if login != nil {
		meta.Account = *login
	}
	return meta
}

// LoadSettlements derives the processor's settlement report for the batch in dir and stores it.
// injectMismatch makes the processor misreport.
func LoadSettlements(dir string, meta settlement.Meta, injectMismatch bool, log Logf) (SettlementLoad, error) {
	batch, err := storefront.Load(dir)
	if err != nil {
		return SettlementLoad{}, err
	}
	derived, err := settlement.Derive(batch, meta, injectMismatch)
	if err != nil {
		return SettlementLoad{}, err
	}
	file, err := lake.WriteJSON(lake.FinanceDir(batch.ID), "settlement_report.json", derived)
	if err != nil {
		return SettlementLoad{}, err
	}
	rows := [][]string{}
	for _, c := range derived.Currencies {
		rows = append(rows, []string{
			c.Currency, fmt.Sprint(c.Captures), money.FormatMinor(c.GrossMinor, c.Currency),
			money.FormatMinor(c.RefundsMinor, c.Currency), money.FormatMinor(c.FeesMinor, c.Currency),
			money.FormatMinor(c.NetMinor, c.Currency),
		})
	}
	log("Settlement report from %s (account %s), payout on %s, inject_mismatch=%t\n%s",
		meta.Processor, meta.Account, derived.PayoutDate, injectMismatch,
		report.Table([]string{"currency", "captures", "gross", "refunds", "fees", "net payout"}, rows, 1, 2, 3, 4, 5))
	return SettlementLoad{BatchDir: dir, ReportPath: filepath.Join(lake.FinanceDir(batch.ID), file.Name), Report: derived}, nil
}
