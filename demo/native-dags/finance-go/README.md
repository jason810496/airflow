<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Finance (Go)

The bundle of the finance team. It holds one Dag, `finance_revenue_close`, which has no schedule: the storefront
triggers it. The Dag is in `revenue_close.go`, the code it uses is in `internal/`.

```text
snapshot_fx_rates ----------------------------------------.
reconcile: load_sales, load_risk_decisions, load_settlements --> post_journal_entries
post_journal_entries --> if ledger_balanced --> publish_trial_balance --> switch close_type
                                           \-> open_recon_incident        daily_close | week_end_close | month_end_close
all branches --> finalize_close --> trigger_customer_invoices
```

`finalize_close` writes `close_summary.json`, sets the Variable `handoff.finance.latest_close` and uses the trigger rule
`none_failed_min_one_success`, because each `if` and `switch` skips a side. When the settlement does not match, the
day gets an incident and no period close.

Everything is written to `<lake>/finance/<batch_id>/`: `fx_rates.json`, `settlement_report.json`, `journal.json`,
`trial_balance.json` or `recon_incident.json`, `period_close.json` and `close_summary.json`. Amounts are integer US
cents, and the settlement report is in the minor unit of each currency.

## Contracts

Finance reads the storefront batch named by the Variable `handoff.storefront.latest_batch`: `sales_summary.json`,
`orders.json` and `refunds.json`, with the shapes of `storefront-ts/src/lib/types.ts`.

Finance writes `close_summary.json`, which `storefront_customer_invoices` reads for `storefront_batch_dir`.
The Variable `handoff.finance.latest_close` holds the directory that contains it.

### Risk decisions

Finance uses the risk decisions of a batch when the Variable `handoff.risk.latest_decisions` names a directory with this
`decisions.json`, and its `storefront_batch_dir` is the batch that finance is closing. Anything else means the batch
was not screened, and finance books every order as approved.

```json
{
  "batch_id": "manual__2026-10-07T00-00-00",
  "storefront_batch_dir": "/files/demo/lake/storefront/scheduled__2026-10-07T00-00-00",
  "decisions": [
    {"order_id": "ORD-20261007-00042", "action": "block", "score": 0.8, "reasons": ["card_velocity"]},
    {"order_id": "ORD-20261007-00107", "action": "review", "score": 0.45, "reasons": ["cross_border_high_value"]}
  ]
}
```

| Field | Required | Meaning |
| --- | --- | --- |
| `storefront_batch_dir` | yes | The storefront batch that risk screened, compared as a path |
| `decisions[].order_id` | yes | An order of that batch, listed once |
| `decisions[].action` | yes | `approve`, `review` or `block`, anything else fails the task |
| `batch_id`, `score`, `reasons` | no | Ignored by finance |

An order that has no decision counts as approved, so risk lists only the orders it screened. Finance holds the revenue of a
`review` order in the held revenue account and reverses a `block` order against refunds payable.

## Knobs

| Variable | Effect |
| --- | --- |
| `finance.fx_rates` | JSON with US dollars per unit, pinned by `snapshot_fx_rates` |
| `finance.force_month_end` | `true` runs `month_end_close` whatever the business date is |
| `finance.inject_mismatch` | `true` makes the processor miss one capture and overcharge one payment method, so the settlement does not match and `open_recon_incident` runs |

The business date is the date of the storefront batch. A Sunday is a week end, the last day of the month is a month end.
The Connection `payments_gateway` names the processor of the settlement report.

## Build and test

```bash
go test ./...
go tool airflow-go-pack --output ../../../files/bundles/finance/finance . -- -trimpath
go tool airflow-go-pack inspect --source ../../../files/bundles/finance/finance
```

`demo/native-dags/setup.sh finance` does the same for Linux, which is what Breeze runs. To close a batch that
`storefront_daily_orders` wrote, run `COCEU_TEST_LAKE=<lake root> go test -run TestRealStorefrontBatch -v .`.
