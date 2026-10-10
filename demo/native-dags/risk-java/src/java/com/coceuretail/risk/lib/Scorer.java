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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Combines the feature maps into one logistic score per order. */
public final class Scorer {
  public static final String HIGH_VALUE = "high_value";
  public static final String CHECKOUT_FLAGGED = "checkout_flagged";

  /** An order worth this much or more in US cents is as high-value as it gets. */
  static final long HIGH_VALUE_USD_CENTS = 100_000;

  /** A feature has to add at least this much to the logit to be named as a reason. */
  static final double REASON_MIN_CONTRIBUTION = 0.5;

  private Scorer() {}

  public static List<ScoredOrder> score(
      List<Order> orders,
      Set<String> checkoutFlagged,
      Map<String, BigDecimal> fxRates,
      ModelWeights model,
      double reviewFrom,
      double blockFrom,
      List<Map<String, Map<String, Double>>> featureSets) {
    var scored = new ArrayList<ScoredOrder>();
    for (var order : orders) {
      long usdCents = Money.toUsdCents(order.totalMinor, order.currency, fxRates);
      var features = new LinkedHashMap<String, Double>();
      for (var set : featureSets) {
        features.putAll(set.getOrDefault(order.orderId, Map.of()));
      }
      features.put(HIGH_VALUE, Math.min(1.0, (double) usdCents / HIGH_VALUE_USD_CENTS));
      features.put(CHECKOUT_FLAGGED, checkoutFlagged.contains(order.orderId) ? 1.0 : 0.0);

      double logit = model.bias;
      var contributions = new LinkedHashMap<String, Double>();
      for (var feature : features.entrySet()) {
        double contribution = model.weights.getOrDefault(feature.getKey(), 0.0) * feature.getValue();
        contributions.put(feature.getKey(), contribution);
        logit += contribution;
      }
      var score = BigDecimal.valueOf(1.0 / (1.0 + Math.exp(-logit))).setScale(4, RoundingMode.HALF_UP);
      scored.add(
          new ScoredOrder(
              order.orderId,
              order.customerId,
              score,
              Band.of(score, reviewFrom, blockFrom),
              reasons(contributions),
              usdCents));
    }
    return scored;
  }

  static List<String> reasons(Map<String, Double> contributions) {
    var reasons = new ArrayList<String>();
    contributions.entrySet().stream()
        .filter(e -> e.getValue() >= REASON_MIN_CONTRIBUTION)
        .sorted(Map.Entry.<String, Double>comparingByValue(Comparator.reverseOrder()))
        .forEach(e -> reasons.add(e.getKey()));
    return reasons;
  }
}
