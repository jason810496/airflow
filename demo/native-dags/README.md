# Acme Retail: native Dags in three languages

A demo of Dags written in TypeScript, Java and Go running side by side. Each team owns its own Dag bundle
and the teams hand work to each other only through `TriggerDagRun` plus a data contract on a shared lake.

| Team | Language | Dags |
| --- | --- | --- |
| Storefront | TypeScript | `storefront_daily_orders` (`@daily`), `storefront_customer_invoices` (triggered) |
| Risk | Java | `risk_fraud_screening` (triggered), not built yet |
| Finance | Go | `finance_revenue_close` (triggered), not built yet |

## Layout

| Path | What it is |
| --- | --- |
| `setup.sh` | Builds the bundles into `files/bundles/<team>/` and writes the Breeze config under `files/` |
| `storefront-ts/` | The storefront TypeScript project. One file per Dag in `src/dags/`, shared code in `src/lib/` |

`files/` is git-ignored and mounted by Breeze as `/files`. Besides the bundles, `setup.sh` writes
`files/airflow-breeze-config/environment_variables.env` (the Dag bundles and `[sdk]` coordinators),
`files/airflow-breeze-config/init.sh` (seeds Connections and Variables), and `files/demo/seed.sh`.
An existing `environment_variables.env` or `init.sh` is kept: the script manages only its own marked block
and backs the file up first.

## Run it

Run `setup.sh` on the host, from the worktree root. It needs Node.js 22 or later and pnpm.

```bash
demo/native-dags/setup.sh            # every team
demo/native-dags/setup.sh storefront # one team
breeze start-airflow
```

Breeze runs the Dags with `LocalExecutor`. With Celery, start the worker with
`-q typescript,java,golang,default`.

After `breeze --db-reset`, run `bash /files/demo/seed.sh` in Breeze to seed the Connections and Variables again.

## Data contract

The lake is `/files/demo/lake/<team>/<batch_id>/`, where `batch_id` is the run id of the producing run.
`storefront_daily_orders` writes `orders.json`, `refunds.json`, `inventory.json`, `suspicious_orders.json`,
`sales_summary.json` and `_manifest.json` (every file with its sha256), then sets the Variable
`handoff.storefront.latest_batch` to the batch directory. Consumers read that Variable, because a triggered
run's `conf` is static. Customer invoices go to `/files/demo/outbox/`.

## Knobs

| Variable | Effect |
| --- | --- |
| `storefront.inject_fraud` | `true` adds card testing, cross-border and new-account orders, so the day is handed to risk |
| `storefront.low_stock` | `true` leaves a few SKUs under 3 days of cover (expedited restock), `stockout` also empties a promoted SKU (campaigns paused) |
| `storefront.fx_rates` | JSON with US dollars per unit, for example `{"EUR": 1.08}` |
