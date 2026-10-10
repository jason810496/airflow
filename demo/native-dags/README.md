# CoC EU Retail: native Dags in three languages

A demo of Dags written in TypeScript, Java and Go running side by side. Each team owns its own Dag bundle
and the teams hand work to each other only through `TriggerDagRun` plus a data contract on a shared lake.

| Team | Language | Dags |
| --- | --- | --- |
| Storefront | TypeScript | `storefront_daily_orders` (`@daily`), `storefront_customer_invoices` (triggered) |
| Risk | Java | `risk_fraud_screening` (triggered, triggers `finance_revenue_close` when it is done) |
| Finance | Go | `finance_revenue_close` (triggered) |

## Layout

| Path | What it is |
| --- | --- |
| `setup.sh` | Builds the bundles into `files/bundles/<team>/` and writes the Breeze config under `files/` |
| `storefront-ts/` | The storefront TypeScript project. One file per Dag in `src/dags/`, shared code in `src/lib/` |
| `risk-java/` | The risk Java project (Gradle). The Dag is `FraudScreeningDag.java`, shared code in `lib/` |
| `finance-go/` | The finance Go module. The Dag is `revenue_close.go`, shared code in `internal/`, the risk decisions contract in its README |

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
breeze start-airflow --backend postgres
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
| `storefront.inventory_scenario` | `normal` (default) gives a standard restock, `low` leaves a few SKUs under 3 days of cover (expedited restock), `stockout` also empties a promoted SKU (campaigns paused) |
| `storefront.fx_rates` | JSON with US dollars per unit, for example `{"EUR": 1.08}` |
| `risk.chargeback_threshold_usd_cents` | Over this amount of blocked and reviewed orders, risk notifies the payments team (default `500000`) |
| `finance.inject_mismatch` | `true` makes the processor settlement disagree with the ledger, so a reconciliation incident opens |
| `finance.force_month_end` | `true` runs the month-end close on any day |

The presenter script is [RUNBOOK.md](RUNBOOK.md).

## Run without breeze

`run-local.sh` starts the same Airflow natively from this worktree, with no Docker: a `uv` venv with editable
`airflow-core`, `task-sdk` and the standard provider, SQLite, `LocalExecutor`, the Simple auth manager, and the
api-server, scheduler, dag-processor and triggerer in the background. Run `setup.sh` first.

```bash
demo/native-dags/run-local.sh start                                  # http://localhost:28080, admin / admin
run_id=$(demo/native-dags/run-local.sh trigger storefront_daily_orders)
demo/native-dags/run-local.sh wait storefront_daily_orders "$run_id" # prints the task states
demo/native-dags/run-local.sh status | seed | logs <component> | stop
```

It reads `files/airflow-breeze-config/environment_variables.env` and replaces `/files/` with `DEMO_FILES_ROOT`.
The task runtimes inherit `COCEU_LAKE_ROOT` and `COCEU_OUTBOX_ROOT`, which point into that root.

| Variable | Default |
| --- | --- |
| `DEMO_STATE_DIR` | `demo/native-dags/.local-airflow` (venv, `AIRFLOW_HOME`, logs, pids) |
| `DEMO_FILES_ROOT` | `<worktree>/files` |
| `DEMO_NODE_BIN` | unset, `node` on `PATH` must be 22 or later |
| `DEMO_JAVA_BIN` | unset, `java` on `PATH` must be 11 or later |
| `DEMO_PORT` | `28080` |
| `DEMO_ADMIN_USER`, `DEMO_ADMIN_PASSWORD` | `admin`, `admin` |
