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

import static java.lang.System.Logger.Level.INFO;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Task log tables. */
public final class Report {
  private static final System.Logger log = System.getLogger(Report.class.getName());

  private Report() {}

  /** Logs a summary of the features and returns them. */
  public static Map<String, Map<String, Double>> features(String title, Map<String, Map<String, Double>> features) {
    var names = features.values().stream().findFirst().map(Map::keySet).orElse(Set.of());
    var rows = new ArrayList<List<String>>();
    for (var name : names) {
      long hits = features.values().stream().filter(f -> f.get(name) > 0).count();
      double max = features.values().stream().mapToDouble(f -> f.get(name)).max().orElse(0);
      rows.add(List.of(name, String.valueOf(hits), String.format("%.2f", max)));
    }
    log.log(INFO, "{0} for {1} orders\n{2}", title, features.size(), Table.render(List.of("feature", "orders hit", "max"), rows, 1, 2));
    return features;
  }

  public static void bands(String title, List<ScoredOrder> scored) {
    var rows = new ArrayList<List<String>>();
    var totals = Decisions.totals(scored);
    for (var band : Band.values()) {
      rows.add(List.of(band.id, String.valueOf(Decisions.in(scored, band).size()), Money.formatUsd(totals.get(band))));
    }
    log.log(INFO, "{0}\n{1}", title, Table.render(List.of("band", "orders", "amount"), rows, 1, 2));
  }

  public static void riskiest(List<ScoredOrder> scored) {
    var rows =
        scored.stream()
            .sorted(Comparator.comparing((ScoredOrder o) -> o.score).reversed())
            .limit(10)
            .map(o -> List.of(o.orderId, o.score.toPlainString(), o.band.id, Money.formatUsd(o.amountUsdCents), String.join(",", o.reasons)))
            .collect(Collectors.toList());
    log.log(INFO, "Riskiest orders\n{0}", Table.render(List.of("order", "score", "band", "amount", "reasons"), rows, 1, 3));
  }
}
