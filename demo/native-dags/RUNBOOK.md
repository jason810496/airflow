# Presenter runbook (about 10 minutes)

CoC EU Retail has three engineering teams, each with its own language and its own release cadence. Each team
writes its pipelines in the language its production code already uses and deploys them as its own Dag bundle.
Airflow runs all of them, and the teams hand work to each other only through `TriggerDagRun` and a data contract.

```
[TS]   storefront_daily_orders (@daily)
         |- if suspicious orders --trigger--> [Java] risk_fraud_screening --trigger--+
         '- else ---------------------------------trigger---------------------------+
                                                                                     v
                                                                  [Go] finance_revenue_close
                                                                    '--trigger--> [TS] storefront_customer_invoices
```

## Before the talk

1. `demo/native-dags/setup.sh` on the host, then `breeze start-airflow --backend postgres`. With SQLite, tasks that
   start in the same millisecond occasionally lose an XCom write or get launched twice.
2. Log in, check that the Dags list shows four Dags from the bundles `storefront`, `risk` and `finance`, and
   that Browse > Import errors is empty.
3. Reset the knobs: `storefront.inject_fraud=false`, `storefront.inventory_scenario=normal`,
   `finance.inject_mismatch=false`, `finance.force_month_end=false`.
4. Optional warm-up: trigger `storefront_daily_orders` once so every Dag has a green run to show.

## 1. One Airflow, three languages (1 min)

- Dags list: filter by tag `typescript`, `java`, `go`. Point at the bundle name of each Dag.
- Open `storefront_daily_orders` > Code. The UI shows `src/dags/daily-orders.ts`, the file that declares the
  Dag, not a Python stub. Do the same for `risk_fraud_screening` (`FraudScreeningDag.java`) and
  `finance_revenue_close` (`revenue_close.go`).
- Open `storefront_customer_invoices` > Code: the same TypeScript bundle, but a different file, because each
  Dag shows its own source.

## 2. Storefront, the clean day (3 min)

Trigger `storefront_daily_orders` and open the Graph view.

- `ingest` task group: three exports running in parallel, each with retries.
- `validate_orders`: TaskFlow fan-in, its inputs are the return values of two exports. No task id strings.
- `write_lake_manifest`: an order-only edge (`.after(ingest, validated)`), it reads files, not return values.
- `has_suspicious_orders` (if/else): the clean day takes `close_revenue`, so `request_risk_review` is skipped.
- `restock_strategy` (switch/case): exactly one of three restock tasks runs.
- `publish_daily_kpis`: a join with `none_failed_min_one_success`, it runs whichever branches were taken.
- Logs of `validate_orders`: sales by currency, refunds, net revenue.

Follow `close_revenue` > Triggered Dag link into `finance_revenue_close` (Go):

- `reconcile` group, `snapshot_fx_rates` with the labelled edge "rates pinned", the double-entry journal,
  `ledger_balanced` (if/else) and `close_type` (switch: daily, week end, month end).
- Logs of `post_journal_entries`: the journal and trial balance tables.
- `trigger_customer_invoices` hands back to the storefront team: open `storefront_customer_invoices`.

## 3. A fraud attack (3 min)

Set `storefront.inject_fraud=true` and `storefront.inventory_scenario=stockout`, then trigger
`storefront_daily_orders`.

- `has_suspicious_orders` now takes `request_risk_review`, and `restock_strategy` picks `pause_campaigns`.
- Follow the trigger link into `risk_fraud_screening` (Java):
  - `features` group (device reputation, velocity, geo mismatch) fanning into `score_orders`.
  - `snapshot_model_weights` before `score_orders`: an order-only edge.
  - `chargeback_exposure_high` (if/else) takes `notify_payments_team`: the blocked and reviewed amount is over
    `risk.chargeback_threshold_usd_cents`.
  - Logs of `apply_decisions`: approve, review and block per order, with reasons. It writes `decisions.json`.
  - `route_by_worst_band` (switch/case): a branch with three cases. `block_and_refund` runs because an
    order is blocked, `auto_approve` and `queue_manual_review` are skipped. Its files are in
    `/files/demo/outbox/risk/<batch>/`.
  - `publish_decisions`: a join with `none_failed_min_one_success` over the cases and the alert check, so it
    runs although most of them were skipped.
  - `trigger_finance_close`: risk starts `finance_revenue_close` itself, without waiting for it. Follow the
    trigger link: `load_risk_decisions` reads the Java team's `decisions.json`, holds reviewed orders and
    reverses blocked ones.
- Code view of `FraudScreeningDag.java`: the branch, the condition and the trigger are plain annotated methods.
  The trigger task runs in the Java runtime, which is why it names the `java` queue like every other task.

## 4. Finance branches (2 min, optional)

- `finance.inject_mismatch=true`, trigger `finance_revenue_close`: `open_recon_incident` runs and the period
  close is skipped.
- `finance.force_month_end=true`, trigger again: `month_end_close` runs.

## 5. Wrap up (1 min)

- Same Airflow, same UI, same scheduling, retries, skips and trigger links for all three languages.
- Each team keeps its own language, libraries, tests and bundle. The only contract between teams is a Dag id,
  a Variable and the files on the lake.

## Knobs

| Variable | Values | Effect |
| --- | --- | --- |
| `storefront.inject_fraud` | `false`, `true` | `true` adds an organised attack, so the day goes to risk |
| `storefront.inventory_scenario` | `normal`, `low`, `stockout` | standard restock, expedited restock, campaigns paused |
| `risk.chargeback_threshold_usd_cents` | default `500000` | over it, risk notifies the payments team |
| `finance.inject_mismatch` | `false`, `true` | the processor settlement does not match, so a reconciliation incident opens |
| `finance.force_month_end` | `false`, `true` | runs the month-end close on any day |

## If something goes wrong

- A Dag is missing: Browse > Import errors, then the Dag processor log. Re-run `setup.sh <team>`; the bundles
  refresh every 30 seconds.
- A triggered Dag reads an older batch: the hand-off is the latest value of `handoff.<team>.<dataset>`, so
  wait for the producing run to finish before triggering a team by hand.
- Lake and outbox files are under `/files/demo/lake/<team>/<batch_id>/` and `/files/demo/outbox/`.
