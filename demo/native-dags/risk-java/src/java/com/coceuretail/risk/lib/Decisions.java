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

package com.coceuretail.risk.lib;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The risk team's hand-off, {@code decisions.json}:
 *
 * <pre>{@code
 * {"storefront_batch_dir": "...", "batch_id": "...",
 *  "decisions": [{"order_id", "action": "approve|review|block", "score", "reasons": [], "amount_usd_cents"}],
 *  "totals_usd_cents": {"approve": 0, "review": 0, "block": 0}}
 * }</pre>
 */
public final class Decisions {
  public static final String FILE = "decisions.json";

  private Decisions() {}

  public static Map<Band, Long> totals(List<ScoredOrder> orders) {
    var totals = new EnumMap<Band, Long>(Band.class);
    for (var band : Band.values()) {
      totals.put(band, 0L);
    }
    orders.forEach(o -> totals.merge(o.band, o.amountUsdCents, Long::sum));
    return totals;
  }

  public static List<ScoredOrder> in(List<ScoredOrder> orders, Band band) {
    return orders.stream().filter(o -> o.band == band).collect(Collectors.toList());
  }

  /** The most severe action of the batch: block over review over approve. */
  public static Band worstBand(Map<String, Object> decisionsDocument) {
    var worst = Band.APPROVE;
    for (var decision : (List<?>) decisionsDocument.get("decisions")) {
      var band = Band.of((String) ((Map<?, ?>) decision).get("action"));
      if (band.compareTo(worst) > 0) {
        worst = band;
      }
    }
    return worst;
  }

  public static Path write(Path storefrontBatchDir, List<ScoredOrder> orders) {
    return Lake.writeJson(Lake.riskDir(storefrontBatchDir).resolve(FILE), document(storefrontBatchDir, orders));
  }

  public static Map<String, Object> load(String decisionsPath) {
    return Lake.readObject(Path.of(decisionsPath));
  }

  /** Writes decisions_summary.json next to the decisions and returns the risk directory of the batch. */
  public static Path writeSummary(String decisionsPath) {
    var document = load(decisionsPath);
    var batchDir = Path.of((String) document.get("storefront_batch_dir"));
    var riskDir = Lake.riskDir(batchDir);

    var summary = new LinkedHashMap<String, Object>();
    summary.put("storefront_batch_dir", batchDir.toString());
    summary.put("batch_id", Lake.batchId(batchDir));
    summary.put("order_count", ((List<?>) document.get("decisions")).size());
    summary.put("totals_usd_cents", document.get("totals_usd_cents"));
    summary.put("chargeback_exposure_usd_cents", chargebackExposure(document));
    summary.put("decisions", decisionsPath);
    Lake.writeJson(riskDir.resolve("decisions_summary.json"), summary);
    return riskDir;
  }

  public static Map<String, Object> document(Path storefrontBatchDir, List<ScoredOrder> orders) {
    var decisions = new ArrayList<Map<String, Object>>();
    for (var order : orders) {
      var decision = new LinkedHashMap<String, Object>();
      decision.put("order_id", order.orderId);
      decision.put("action", order.band.id);
      decision.put("score", order.score);
      decision.put("reasons", order.reasons);
      decision.put("amount_usd_cents", order.amountUsdCents);
      decisions.add(decision);
    }
    var totals = new LinkedHashMap<String, Object>();
    totals(orders).forEach((band, cents) -> totals.put(band.id, cents));

    var document = new LinkedHashMap<String, Object>();
    document.put("storefront_batch_dir", storefrontBatchDir.toString());
    document.put("batch_id", Lake.batchId(storefrontBatchDir));
    document.put("decisions", decisions);
    document.put("totals_usd_cents", totals);
    return document;
  }

  /** Money tied up in blocked and reviewed orders, which the payments team may have to refund or chargeback. */
  public static long chargebackExposure(Map<String, Object> decisionsDocument) {
    var totals = (Map<?, ?>) decisionsDocument.get("totals_usd_cents");
    return cents(totals.get(Band.BLOCK.id)) + cents(totals.get(Band.REVIEW.id));
  }

  private static long cents(Object value) {
    return value == null ? 0 : ((Number) value).longValue();
  }
}
