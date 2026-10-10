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
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/sdk"

	"coceuretail.example/finance/internal/lake"
	"coceuretail.example/finance/internal/money"
	"coceuretail.example/finance/internal/storefront"
)

// fakeClient is the Variables and Connections of a test. The rest of sdk.Client is nil.
type fakeClient struct {
	sdk.Client
	variables map[string]string
}

func (c *fakeClient) GetVariable(_ context.Context, key string) (string, error) {
	if value, ok := c.variables[key]; ok {
		return value, nil
	}
	return "", fmt.Errorf("%w: %s", sdk.VariableNotFound, key)
}

func (c *fakeClient) SetVariable(_ context.Context, key, value, _ string) error {
	c.variables[key] = value
	return nil
}

func (c *fakeClient) GetConnection(_ context.Context, id string) (sdk.Connection, error) {
	if id != "payments_gateway" {
		return sdk.Connection{}, sdk.ConnectionNotFound
	}
	login := "coceuretail-finance"
	return sdk.Connection{ID: id, Type: "http", Host: "payments.internal.coceuretail", Login: &login}, nil
}

type env struct {
	t      *testing.T
	lake   string
	client *fakeClient
	logs   *bytes.Buffer
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("COCEU_LAKE_ROOT", dir)
	return &env{
		t: t, lake: dir, logs: &bytes.Buffer{},
		client: &fakeClient{variables: map[string]string{
			"finance.fx_rates": `{"USD": 1, "EUR": 1.085, "GBP": 1.265, "JPY": 0.00668}`,
		}},
	}
}

func (e *env) actx() airflow.Context {
	return airflow.NewContext(
		context.Background(), slog.New(slog.NewTextHandler(e.logs, nil)), e.client,
		airflow.TaskInstance{DagID: "finance_revenue_close", RunID: "manual__test", TaskID: "t", TryNumber: 1},
		airflow.DagRun{DagID: "finance_revenue_close", RunID: "manual__test"},
	)
}

// storefrontBatch writes a storefront batch like the one storefront_daily_orders writes, with orders
// in four currencies, a gift card, a shipping remainder and some refunds.
func (e *env) storefrontBatch(businessDate string, orders int) string {
	e.t.Helper()
	batchID := "scheduled__" + businessDate + "T00-00-00"
	dir := filepath.Join(e.lake, "storefront", batchID)
	currencies := []string{"USD", "USD", "USD", "EUR", "EUR", "GBP", "JPY"}
	skus := []string{"TEE-001", "HDY-001", "SNK-001", "MUG-001", "EAR-001", "LIP-001", "TAB-001", "GFT-025", "XYZ-009"}
	priceUSD := map[string]int64{
		"TEE-001": 2400, "HDY-001": 5900, "SNK-001": 9900, "MUG-001": 1800, "EAR-001": 12900,
		"LIP-001": 1200, "TAB-001": 32900, "GFT-025": 2500, "XYZ-009": 700,
	}
	var all []storefront.Order
	var refunds []storefront.Refund
	rates := money.DefaultRates
	var gross, refundsUSD int64
	for i := 0; i < orders; i++ {
		currency := currencies[i%len(currencies)]
		order := storefront.Order{
			OrderID:    fmt.Sprintf("ORD-%s-%05d", businessDate[:4]+businessDate[5:7]+businessDate[8:], i+1),
			CustomerID: fmt.Sprintf("C-%d", 100000+i), Currency: currency,
			CardFingerprint: fmt.Sprintf("card_%d", i%23),
			BillingCountry:  "US", ShippingCountry: "US", PlacedAt: businessDate + "T10:00:00Z",
		}
		for line := 0; line <= i%2; line++ {
			sku := skus[(i+line*3)%len(skus)]
			unit := priceUSD[sku]
			switch currency {
			case "JPY":
				unit = unit * 3 / 2
			case "GBP":
				unit = unit * 8 / 10
			}
			qty := int64(1 + (i+line)%3)
			order.Items = append(order.Items, storefront.LineItem{SKU: sku, Qty: qty, UnitPriceMinor: unit})
			order.TotalMinor += qty * unit
		}
		if i%9 == 0 {
			order.TotalMinor += 499
		}
		all = append(all, order)
		gross += rates.ToUSDCents(order.TotalMinor, currency)
		if i%17 == 0 {
			refund := storefront.Refund{
				RefundID: fmt.Sprintf("RF-%d", i), OrderID: order.OrderID, Currency: currency,
				AmountMinor: order.TotalMinor / 2, Reason: "changed_mind",
			}
			refunds = append(refunds, refund)
			refundsUSD += rates.ToUSDCents(refund.AmountMinor, currency)
		}
	}
	header := map[string]any{"batch_id": batchID, "business_date": businessDate, "source": "test"}
	write := func(name string, extra map[string]any) {
		doc := map[string]any{}
		for k, v := range header {
			doc[k] = v
		}
		for k, v := range extra {
			doc[k] = v
		}
		if _, err := lake.WriteJSON(dir, name, doc); err != nil {
			e.t.Fatal(err)
		}
	}
	write("orders.json", map[string]any{"orders": all})
	write("refunds.json", map[string]any{"refunds": refunds})
	write("sales_summary.json", map[string]any{
		"gross_usd_cents": gross, "refunds_usd_cents": refundsUSD, "net_usd_cents": gross - refundsUSD,
		"order_count": len(all), "refund_count": len(refunds), "suspicious_count": 0,
	})
	e.client.variables[lake.StorefrontBatchVariable] = dir
	return dir
}

func (e *env) writeDecisions(storefrontDir string, decisions ...map[string]any) {
	e.t.Helper()
	dir := filepath.Join(e.lake, "risk", "risk_batch")
	if _, err := lake.WriteJSON(dir, "decisions.json", map[string]any{
		"batch_id": "risk_batch", "storefront_batch_dir": storefrontDir, "decisions": decisions,
	}); err != nil {
		e.t.Fatal(err)
	}
	e.client.variables[lake.RiskDecisionsVariable] = dir
}

func (e *env) exists(batchDir, name string) bool {
	_, err := os.Stat(filepath.Join(batchDir, name))
	return err == nil
}
