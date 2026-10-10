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

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class FeaturesTest {
  private static final Instant NOON = Instant.parse("2026-10-07T12:00:00Z");

  private static Order order(
      String id, String customer, String email, String card, String billing, String shipping, String currency, Instant at) {
    return new Order(id, customer, 400, email, card, billing, shipping, currency, 5000, at);
  }

  @Test
  void deviceReputationScoresDisposableDomainsAndSharedCards() {
    var orders =
        List.of(
            order("o1", "c1", "a@gmail.com", "card_x", "US", "US", "USD", NOON),
            order("o2", "c2", "b@tempmail.dev", "card_x", "US", "US", "USD", NOON),
            order("o3", "c3", "c@example.org", "card_y", "US", "US", "USD", NOON));
    var features = DeviceReputation.compute(orders);

    assertEquals(0.0, features.get("o1").get(DeviceReputation.DISPOSABLE_EMAIL));
    assertEquals(1.0, features.get("o2").get(DeviceReputation.DISPOSABLE_EMAIL));
    assertEquals(0.25, features.get("o3").get(DeviceReputation.DISPOSABLE_EMAIL));
    assertEquals(0.5, features.get("o1").get(DeviceReputation.SHARED_CARD));
    assertEquals(0.0, features.get("o3").get(DeviceReputation.SHARED_CARD));
  }

  @Test
  void velocityCountsTheCardInsideTheWindowOnly() {
    var orders =
        List.of(
            order("o1", "c1", "a@gmail.com", "card_x", "US", "US", "USD", NOON),
            order("o2", "c2", "a@gmail.com", "card_x", "US", "US", "USD", NOON.plusSeconds(60)),
            order("o3", "c3", "a@gmail.com", "card_x", "US", "US", "USD", NOON.plusSeconds(60 * 60)),
            order("o4", "c4", "a@gmail.com", "card_z", "US", "US", "USD", NOON));
    var features = Velocity.compute(orders, 30);

    assertEquals(0.25, features.get("o1").get(Velocity.CARD_VELOCITY));
    assertEquals(0.25, features.get("o2").get(Velocity.CARD_VELOCITY));
    assertEquals(0.0, features.get("o3").get(Velocity.CARD_VELOCITY));
    assertEquals(0.0, features.get("o4").get(Velocity.CARD_VELOCITY));
  }

  @Test
  void velocityCountsACustomerOverADay() {
    var orders =
        List.of(
            order("o1", "c1", "a@gmail.com", "card_a", "US", "US", "USD", NOON),
            order("o2", "c1", "a@gmail.com", "card_b", "US", "US", "USD", NOON.plusSeconds(3 * 3600)),
            order("o3", "c1", "a@gmail.com", "card_c", "US", "US", "USD", NOON.plusSeconds(25 * 3600)));
    var features = Velocity.compute(orders, 30);

    assertEquals(1.0 / 3, features.get("o1").get(Velocity.CUSTOMER_VELOCITY), 1e-9);
    assertEquals(2.0 / 3, features.get("o2").get(Velocity.CUSTOMER_VELOCITY), 1e-9);
  }

  @Test
  void geoMismatchComparesBillingShippingAndCurrency() {
    var orders =
        List.of(
            order("home", "c1", "a@gmail.com", "card_a", "DE", "DE", "EUR", NOON),
            order("gift", "c2", "a@gmail.com", "card_b", "US", "NG", "USD", NOON),
            order("odd", "c3", "a@gmail.com", "card_c", "GB", "GB", "USD", NOON));
    var features = GeoMismatch.compute(orders);

    assertEquals(0.0, features.get("home").get(GeoMismatch.BILLING_SHIPPING_MISMATCH));
    assertEquals(0.0, features.get("home").get(GeoMismatch.CURRENCY_COUNTRY_MISMATCH));
    assertEquals(1.0, features.get("gift").get(GeoMismatch.BILLING_SHIPPING_MISMATCH));
    assertEquals(1.0, features.get("gift").get(GeoMismatch.HIGH_RISK_DESTINATION));
    assertEquals(1.0, features.get("odd").get(GeoMismatch.CURRENCY_COUNTRY_MISMATCH));
  }
}
