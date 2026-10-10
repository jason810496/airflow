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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ScoringTest {
  private static Path batchDir;
  private static List<ScoredOrder> scored;
  private static Set<String> flagged;

  @BeforeAll
  static void scoreTheFixtureBatch() throws URISyntaxException {
    batchDir =
        Path.of(ScoringTest.class.getResource("/lake/storefront/scheduled__2026-10-07T00-00-00/orders.json").toURI())
            .getParent();
    var orders = Order.load(batchDir.resolve("orders.json"));
    flagged = new HashSet<>();
    for (var o : (List<?>) Lake.readObject(batchDir.resolve("suspicious_orders.json")).get("orders")) {
      flagged.add((String) ((Map<?, ?>) o).get("order_id"));
    }
    var rates = Money.rates(Lake.readObject(batchDir.resolve("sales_summary.json")).get("fx_rates"));
    scored =
        Scorer.score(
            orders,
            flagged,
            rates,
            ModelWeights.parse(null),
            0.4,
            0.8,
            List.of(
                DeviceReputation.compute(orders), Velocity.compute(orders, 30), GeoMismatch.compute(orders)));
  }

  private static Map<String, ScoredOrder> byId() {
    return scored.stream().collect(Collectors.toMap(o -> o.orderId, o -> o));
  }

  @Test
  void bandsSplitAtTheThresholds() {
    assertEquals(Band.APPROVE, Band.of(new BigDecimal("0.3999"), 0.4, 0.8));
    assertEquals(Band.REVIEW, Band.of(new BigDecimal("0.4000"), 0.4, 0.8));
    assertEquals(Band.REVIEW, Band.of(new BigDecimal("0.7999"), 0.4, 0.8));
    assertEquals(Band.BLOCK, Band.of(new BigDecimal("0.8000"), 0.4, 0.8));
  }

  @Test
  void cardTestingBurstIsBlockedWithItsReasons() {
    var order = byId().get("ORD-20261007-00501");
    assertEquals(Band.BLOCK, order.band);
    assertEquals(2499, order.amountUsdCents);
    assertTrue(order.reasons.containsAll(List.of("card_velocity", "disposable_email", "shared_card")));
  }

  @Test
  void crossBorderHighValueOrdersGoToReview() {
    var order = byId().get("ORD-20261007-00508");
    assertEquals(Band.REVIEW, order.band);
    assertTrue(order.reasons.contains("high_risk_destination"));
  }

  @Test
  void everyUnflaggedOrderIsApproved() {
    var unflagged = scored.stream().filter(o -> !flagged.contains(o.orderId)).collect(Collectors.toList());
    assertEquals(25, unflagged.size());
    assertTrue(unflagged.stream().allMatch(o -> o.band == Band.APPROVE));
  }

  @Test
  void decisionsKeepTheContractAndTheMoneyAddsUp() {
    var document = Decisions.document(batchDir, scored);

    assertEquals(batchDir.toString(), document.get("storefront_batch_dir"));
    assertEquals("scheduled__2026-10-07T00-00-00", document.get("batch_id"));
    var decisions = (List<?>) document.get("decisions");
    assertEquals(scored.size(), decisions.size());
    assertEquals(
        List.of("order_id", "action", "score", "reasons", "amount_usd_cents"),
        List.copyOf(((Map<?, ?>) decisions.get(0)).keySet()));

    var totals = (Map<?, ?>) document.get("totals_usd_cents");
    long all = scored.stream().mapToLong(o -> o.amountUsdCents).sum();
    assertEquals(all, (long) totals.get("approve") + (long) totals.get("review") + (long) totals.get("block"));
    assertEquals((long) totals.get("block") + (long) totals.get("review"), Decisions.chargebackExposure(document));
  }

  @Test
  void decisionsSurviveAWriteAndRead() {
    var document = Decisions.document(batchDir, scored);
    var written = Json.parseObject(Json.write(document));

    assertEquals(Decisions.chargebackExposure(document), Decisions.chargebackExposure(written));
    assertEquals(scored.get(0).score, ScoredOrder.fromMap(Json.parseObject(Json.write(scored.get(0).toMap()))).score);
  }

  @Test
  void jsonRoundTripsNestedValues() {
    var text = "{\"a\": [1, 2.5, \"x\\n\\\"y\"], \"b\": {\"c\": null, \"d\": true}, \"e\": []}";
    assertEquals(Json.parse(text), Json.parse(Json.write(Json.parse(text))));
  }

  @Test
  void usdCentsRoundHalfUpFromTheMinorUnit() {
    var rates = Money.rates(Map.of("EUR", new BigDecimal("1.085"), "JPY", new BigDecimal("0.00668")));
    assertEquals(1085, Money.toUsdCents(1000, "EUR", rates));
    assertEquals(6680, Money.toUsdCents(10000, "JPY", rates));
    assertEquals(100, Money.toUsdCents(100, "USD", rates));
  }
}
