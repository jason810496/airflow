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

package com.acme.risk.lib;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Hand-offs to other teams, written under the outbox of the batch. */
public final class Outbox {
  private Outbox() {}

  public static void queueManualReview(Path batchDir, List<ScoredOrder> review) {
    var items = new ArrayList<Map<String, Object>>();
    review.stream()
        .sorted(Comparator.comparing((ScoredOrder o) -> o.score).reversed())
        .forEach(o -> items.add(o.toMap()));
    var queue = new LinkedHashMap<String, Object>();
    queue.put("queue", "manual_review");
    queue.put("count", items.size());
    queue.put("orders", items);
    Lake.writeJson(Lake.outboxDir(batchDir).resolve("manual_review_queue.json"), queue);
  }

  public static void requestRefunds(Path batchDir, List<ScoredOrder> blocked, String gatewayHost) {
    var refunds = new ArrayList<Map<String, Object>>();
    for (var order : blocked) {
      var refund = new LinkedHashMap<String, Object>();
      refund.put("order_id", order.orderId);
      refund.put("amount_usd_cents", order.amountUsdCents);
      refund.put("reason", "fraud_block");
      refunds.add(refund);
    }
    var requests = new LinkedHashMap<String, Object>();
    requests.put("gateway", gatewayHost);
    requests.put("count", refunds.size());
    requests.put("refunds", refunds);
    Lake.writeJson(Lake.outboxDir(batchDir).resolve("refund_requests.json"), requests);
  }

  public static Path alertPayments(String decisionsPath, String gatewayHost, long thresholdUsdCents) {
    var document = Decisions.load(decisionsPath);
    var batchDir = Path.of((String) document.get("storefront_batch_dir"));
    var totals = (Map<?, ?>) document.get("totals_usd_cents");

    var alert = new LinkedHashMap<String, Object>();
    alert.put("to", gatewayHost);
    alert.put("subject", "Chargeback exposure above tolerance for batch " + Lake.batchId(batchDir));
    alert.put("exposure_usd_cents", Decisions.chargebackExposure(document));
    alert.put("threshold_usd_cents", thresholdUsdCents);
    alert.put("blocked_usd_cents", totals.get(Band.BLOCK.id));
    alert.put("review_usd_cents", totals.get(Band.REVIEW.id));
    alert.put("decisions", decisionsPath);
    return Lake.writeJson(Lake.outboxDir(batchDir).resolve("payments_alert.json"), alert);
  }
}
