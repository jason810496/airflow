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

// Package storefront reads the batch files that the storefront team writes to the lake. The field
// names are the data contract, so they follow storefront-ts/src/lib/types.ts.
package storefront

import (
	"fmt"
	"path/filepath"

	"coceuretail.example/finance/internal/lake"
)

type LineItem struct {
	SKU            string `json:"sku"`
	Qty            int64  `json:"qty"`
	UnitPriceMinor int64  `json:"unit_price_minor"`
}

type Order struct {
	OrderID         string     `json:"order_id"`
	CustomerID      string     `json:"customer_id"`
	CardFingerprint string     `json:"card_fingerprint"`
	BillingCountry  string     `json:"billing_country"`
	ShippingCountry string     `json:"shipping_country"`
	Currency        string     `json:"currency"`
	Items           []LineItem `json:"items"`
	TotalMinor      int64      `json:"total_minor"`
	PlacedAt        string     `json:"placed_at"`
}

type Refund struct {
	RefundID    string `json:"refund_id"`
	OrderID     string `json:"order_id"`
	Currency    string `json:"currency"`
	AmountMinor int64  `json:"amount_minor"`
	Reason      string `json:"reason"`
}

type OrdersFile struct {
	BatchID      string  `json:"batch_id"`
	BusinessDate string  `json:"business_date"`
	Orders       []Order `json:"orders"`
}

type RefundsFile struct {
	BatchID      string   `json:"batch_id"`
	BusinessDate string   `json:"business_date"`
	Refunds      []Refund `json:"refunds"`
}

// Summary is sales_summary.json: what the storefront computed with its own FX rates.
type Summary struct {
	BatchID              string `json:"batch_id"`
	BusinessDate         string `json:"business_date"`
	GrossUSDCents        int64  `json:"gross_usd_cents"`
	RefundsUSDCents      int64  `json:"refunds_usd_cents"`
	NetUSDCents          int64  `json:"net_usd_cents"`
	OrderCount           int    `json:"order_count"`
	RefundCount          int    `json:"refund_count"`
	UnmatchedRefundCount int    `json:"unmatched_refund_count"`
	SuspiciousCount      int    `json:"suspicious_count"`
}

// Batch is one storefront batch as finance reads it.
type Batch struct {
	Dir          string
	ID           string
	BusinessDate string
	Orders       []Order
	Refunds      []Refund
	Summary      Summary
	// Files holds the digest of each file read, in the order sales_summary, orders, refunds.
	Files []lake.File
}

// Load reads sales_summary.json, orders.json and refunds.json of dir at the same time.
func Load(dir string) (*Batch, error) {
	var (
		summary Summary
		orders  OrdersFile
		refunds RefundsFile
		files   = make([]lake.File, 3)
	)
	read := func(i int, name string, into any) func() error {
		return func() error {
			path := filepath.Join(dir, name)
			if err := lake.ReadJSON(path, into); err != nil {
				return err
			}
			digest, err := lake.Digest(path)
			files[i] = digest
			return err
		}
	}
	err := lake.Parallel(
		read(0, "sales_summary.json", &summary),
		read(1, "orders.json", &orders),
		read(2, "refunds.json", &refunds),
	)
	if err != nil {
		return nil, err
	}
	if orders.BatchID != summary.BatchID || refunds.BatchID != summary.BatchID {
		return nil, fmt.Errorf(
			"%s mixes batches: summary %q, orders %q, refunds %q",
			dir, summary.BatchID, orders.BatchID, refunds.BatchID,
		)
	}
	return &Batch{
		Dir:          dir,
		ID:           summary.BatchID,
		BusinessDate: summary.BusinessDate,
		Orders:       orders.Orders,
		Refunds:      refunds.Refunds,
		Summary:      summary,
		Files:        files,
	}, nil
}
