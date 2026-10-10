/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// risk_fraud_screening: the risk team's screening of a storefront batch.
//
// Triggered by the storefront when its checkout rules flag orders. It scores every order of the batch,
// writes the decisions, routes the batch by its worst band (a branch with one task per outcome), alerts
// payments when the money at risk is high, publishes the decisions through the
// handoff.risk.latest_decisions Variable and triggers the finance close without waiting for it.
package com.coceuretail.risk;

import static com.coceuretail.risk.FraudScreeningDagBuilder.TaskIds.AUTO_APPROVE;
import static com.coceuretail.risk.FraudScreeningDagBuilder.TaskIds.BLOCK_AND_REFUND;
import static com.coceuretail.risk.FraudScreeningDagBuilder.TaskIds.QUEUE_MANUAL_REVIEW;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.coceuretail.risk.lib.Band;
import com.coceuretail.risk.lib.Decisions;
import com.coceuretail.risk.lib.DeviceReputation;
import com.coceuretail.risk.lib.GeoMismatch;
import com.coceuretail.risk.lib.Handoff;
import com.coceuretail.risk.lib.Lake;
import com.coceuretail.risk.lib.ModelWeights;
import com.coceuretail.risk.lib.Money;
import com.coceuretail.risk.lib.Outbox;
import com.coceuretail.risk.lib.Payments;
import com.coceuretail.risk.lib.Report;
import com.coceuretail.risk.lib.Routes;
import com.coceuretail.risk.lib.ScoreFile;
import com.coceuretail.risk.lib.Scorer;
import com.coceuretail.risk.lib.Velocity;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.Builder;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;
import org.apache.airflow.sdk.TaskId;
import org.apache.airflow.sdk.TriggerDagRun;

@Builder.Dag(
    id = "risk_fraud_screening",
    description = "Score a storefront batch for fraud, apply the decisions and hand them to finance",
    startDate = "2026-01-01T00:00:00Z",
    catchup = false,
    isPausedUponCreation = false,
    tags = {"risk", "java"},
    docMd =
        "### Risk fraud screening\n"
            + "\n"
            + "Triggered by `storefront_daily_orders`, never scheduled. Reads the batch from the Variable\n"
            + "`handoff.storefront.latest_batch` and writes `scores.json` and `decisions.json` to\n"
            + "`/files/demo/lake/risk/<batch>/`.\n"
            + "\n"
            + "* `features`: device reputation, velocity and geo mismatch, each a feature per order.\n"
            + "* `score_orders`: logistic score with the weights of the Variable `risk.model_weights`.\n"
            + "  Below 0.4 approve, from 0.4 review, from 0.8 block.\n"
            + "* `apply_decisions`: writes `decisions.json`, the contract the finance team reads.\n"
            + "* `route_by_worst_band`: a branch that runs one case and skips the others: `auto_approve`,\n"
            + "  `queue_manual_review` (worst is a review) or `block_and_refund` (any order blocked). Each\n"
            + "  writes its files to `/files/demo/outbox/risk/<batch>/`.\n"
            + "* `chargeback_exposure_high`: alerts payments when blocked and reviewed money is over the\n"
            + "  Variable `risk.chargeback_threshold_usd_cents`, otherwise logs that it is within tolerance.\n"
            + "* `publish_decisions`: sets the Variable `handoff.risk.latest_decisions`. It runs once a case\n"
            + "  and a side of the alert check ran, whatever was skipped.\n"
            + "* `trigger_finance_close`: starts `finance_revenue_close` with that Variable as its contract\n"
            + "  and does not wait for it. It runs in the Java runtime, so it names queue `java`.\n")
public class FraudScreeningDag {
  private static final System.Logger log = System.getLogger(FraudScreeningDag.class.getName());

  // Java has no Dag-level queue, so every task names it.
  static final String QUEUE = "java";

  static final String WEIGHTS_VARIABLE = "risk.model_weights";
  static final String THRESHOLD_VARIABLE = "risk.chargeback_threshold_usd_cents";
  static final String FINANCE_DAG_ID = "finance_revenue_close";
  static final String GATEWAY_CONNECTION = "payments_gateway";
  static final String DEFAULT_GATEWAY_HOST = "payments.internal.coceuretail";
  static final long DEFAULT_THRESHOLD_USD_CENTS = 500_000;
  static final int CARD_WINDOW_MINUTES = 30;
  static final double REVIEW_FROM = 0.4;
  static final double BLOCK_FROM = 0.8;

  // Reads the batch the storefront announced, from the Variable named in the trigger conf.
  @Builder.Task(id = "load_batch", retries = 1, queue = QUEUE)
  public String loadBatch(Context context, Client client) throws Exception {
    return Handoff.requestedBatch(context, client).toString();
  }

  @Builder.TaskGroup(id = "features")
  static class Features {
    @Builder.Task(id = "device_reputation", queue = QUEUE)
    public Map<String, Map<String, Double>> deviceReputation(String batchDir) {
      return Report.features("device_reputation", DeviceReputation.compute(Lake.orders(batchDir)));
    }

    @Builder.Task(id = "velocity", queue = QUEUE)
    public Map<String, Map<String, Double>> velocity(String batchDir, int windowMinutes) {
      var title = "velocity (card window " + windowMinutes + " min, customer window 24 h)";
      return Report.features(title, Velocity.compute(Lake.orders(batchDir), windowMinutes));
    }

    @Builder.Task(id = "geo_mismatch", queue = QUEUE)
    public Map<String, Map<String, Double>> geoMismatch(String batchDir) {
      return Report.features("geo_mismatch", GeoMismatch.compute(Lake.orders(batchDir)));
    }
  }

  // Keeps the weights this run scores with next to its scores, whatever the Variable becomes later.
  @Builder.Task(id = "snapshot_model_weights", queue = QUEUE)
  public void snapshotModelWeights(String batchDir, Client client) {
    var batch = Path.of(batchDir);
    var model = ModelWeights.snapshot(batch, client.getVariable(WEIGHTS_VARIABLE));
    log.log(
        INFO,
        "Snapshot of {0} with {1} weights written to {2}",
        WEIGHTS_VARIABLE,
        model.weights.size(),
        ModelWeights.snapshotFile(batch));
  }

  @Builder.Task(id = "score_orders", retries = 1, queue = QUEUE)
  public String scoreOrders(
      String batchDir,
      Map<String, Map<String, Double>> device,
      Map<String, Map<String, Double>> velocity,
      Map<String, Map<String, Double>> geo,
      double reviewFrom,
      double blockFrom) {
    var batch = Path.of(batchDir);
    var model = ModelWeights.readSnapshot(batch);
    var rates = Lake.fxRates(batch);
    var scored =
        Scorer.score(
            Lake.orders(batch),
            Lake.flaggedOrderIds(batch),
            rates,
            model,
            reviewFrom,
            blockFrom,
            List.of(device, velocity, geo));

    var scores = ScoreFile.write(batch, model, reviewFrom, blockFrom, scored);
    Report.bands("Scores", scored);
    Report.riskiest(scored);
    return scores.toString();
  }

  // Writes decisions.json, the contract finance reads. What each band triggers is left to the routing below.
  @Builder.Task(id = "apply_decisions", retries = 1, queue = QUEUE)
  public String applyDecisions(String scoresPath) {
    var scores = ScoreFile.read(scoresPath);
    var decisions = Decisions.write(scores.batchDir, scores.orders);
    log.log(INFO, "Wrote the decisions of {0} orders to {1}", scores.orders.size(), decisions);
    Report.bands("Decisions", scores.orders);
    return decisions.toString();
  }

  // Runs one case and skips the other two. The worst band wins, so a blocked order is never cleared.
  @Builder.Branch(id = "route_by_worst_band", queue = QUEUE)
  public TaskId routeByWorstBand(String decisionsPath) {
    var worst = Decisions.worstBand(Decisions.load(decisionsPath));
    log.log(INFO, "Worst band of the batch is {0}", worst.id);
    return caseFor(worst);
  }

  static TaskId caseFor(Band worst) {
    if (worst == Band.BLOCK) {
      return BLOCK_AND_REFUND;
    }
    return worst == Band.REVIEW ? QUEUE_MANUAL_REVIEW : AUTO_APPROVE;
  }

  @Builder.Task(id = "auto_approve", queue = QUEUE)
  public void autoApprove(String scoresPath) {
    Routes.clear(scoresPath);
  }

  @Builder.Task(id = "queue_manual_review", queue = QUEUE)
  public void queueManualReview(String scoresPath) {
    Routes.queueReview(scoresPath);
  }

  @Builder.Task(id = "block_and_refund", queue = QUEUE)
  public void blockAndRefund(String scoresPath, Client client) {
    Routes.blockAndRefund(scoresPath, Payments.gatewayHost(client, GATEWAY_CONNECTION, DEFAULT_GATEWAY_HOST));
  }

  @Builder.If(id = "chargeback_exposure_high", queue = QUEUE)
  public boolean chargebackExposureHigh(String decisionsPath, Client client) {
    var exposure = Decisions.chargebackExposure(Decisions.load(decisionsPath));
    var threshold = Payments.chargebackThreshold(client, THRESHOLD_VARIABLE, DEFAULT_THRESHOLD_USD_CENTS);
    log.log(
        INFO,
        "Chargeback exposure {0} against a threshold of {1} (Variable {2})",
        Money.formatUsd(exposure),
        Money.formatUsd(threshold),
        THRESHOLD_VARIABLE);
    return exposure > threshold;
  }

  @Builder.Task(id = "notify_payments_team", queue = QUEUE)
  public void notifyPaymentsTeam(String decisionsPath, Client client) {
    var gateway = Payments.gatewayHost(client, GATEWAY_CONNECTION, DEFAULT_GATEWAY_HOST);
    var threshold = Payments.chargebackThreshold(client, THRESHOLD_VARIABLE, DEFAULT_THRESHOLD_USD_CENTS);
    var alert = Outbox.alertPayments(decisionsPath, gateway, threshold);
    log.log(WARNING, "Alerted the payments team at {0}: {1}", gateway, alert);
  }

  @Builder.Task(id = "log_within_tolerance", queue = QUEUE)
  public void logWithinTolerance(String decisionsPath) {
    var exposure = Decisions.chargebackExposure(Decisions.load(decisionsPath));
    log.log(INFO, "Chargeback exposure {0} is within tolerance, no alert needed", Money.formatUsd(exposure));
  }

  // Runs once one case and one side of the condition ran. The branch and the condition each skip
  // their other tasks, so the default all_success would never let this run.
  @Builder.Task(id = "publish_decisions", triggerRule = "none_failed_min_one_success", retries = 1, queue = QUEUE)
  public String publishDecisions(String decisionsPath, Client client) {
    var riskDir = Decisions.writeSummary(decisionsPath);
    client.setVariable(
        Lake.LATEST_DECISIONS_VARIABLE,
        riskDir.toString(),
        "Latest risk decisions directory in the lake. Written by risk_fraud_screening.");
    log.log(INFO, "Set Variable {0} to {1}", Lake.LATEST_DECISIONS_VARIABLE, riskDir);
    return riskDir.toString();
  }

  // Declared when the Dag is built, so the method takes no arguments. The Java runtime runs it,
  // which is why it needs the java queue.
  @Builder.Task(id = "trigger_finance_close", queue = QUEUE)
  public TriggerDagRun triggerFinanceClose() {
    return new TriggerDagRun(FINANCE_DAG_ID)
        .config("conf", Handoff.financeConf())
        .config("wait_for_completion", false);
  }

  // Implements the generated wiring view, so javac type-checks the graph.
  @Builder.Deps
  static class Wiring implements FraudScreeningDagDeps {
    void depends() {
      var batch = loadBatch();
      var device = features().deviceReputation(batch);
      var velocity = features().velocity(batch, lit(CARD_WINDOW_MINUTES));
      var geo = features().geoMismatch(batch);

      var snapshot = snapshotModelWeights(batch);
      var scores = scoreOrders(batch, device, velocity, geo, lit(REVIEW_FROM), lit(BLOCK_FROM));
      // Ordering only: the scores read the snapshot from the lake, not from the task.
      snapshot.before(scores);

      var decisions = applyDecisions(scores);
      var approved = autoApprove(scores);
      var queued = queueManualReview(scores);
      var refunded = blockAndRefund(scores);
      routeByWorstBand(decisions).option(approved).option(queued).option(refunded);

      var alerted = notifyPaymentsTeam(decisions);
      var tolerated = logWithinTolerance(decisions);
      chargebackExposureHigh(decisions).then(alerted).orElse(tolerated);

      var published = publishDecisions(decisions);
      published.after(approved, queued, refunded, alerted, tolerated);
      published.before(triggerFinanceClose());
    }
  }
}
