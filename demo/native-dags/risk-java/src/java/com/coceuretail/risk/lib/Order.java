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
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** One order of the storefront's {@code orders.json}. */
public final class Order {
  public final String orderId;
  public final String customerId;
  public final int accountAgeDays;
  public final String email;
  public final String cardFingerprint;
  public final String billingCountry;
  public final String shippingCountry;
  public final String currency;
  public final long totalMinor;
  public final Instant placedAt;

  public Order(
      String orderId,
      String customerId,
      int accountAgeDays,
      String email,
      String cardFingerprint,
      String billingCountry,
      String shippingCountry,
      String currency,
      long totalMinor,
      Instant placedAt) {
    this.orderId = orderId;
    this.customerId = customerId;
    this.accountAgeDays = accountAgeDays;
    this.email = email;
    this.cardFingerprint = cardFingerprint;
    this.billingCountry = billingCountry;
    this.shippingCountry = shippingCountry;
    this.currency = currency;
    this.totalMinor = totalMinor;
    this.placedAt = placedAt;
  }

  public String emailDomain() {
    int at = email.lastIndexOf('@');
    return at < 0 ? "" : email.substring(at + 1).toLowerCase();
  }

  public static List<Order> load(Path ordersFile) {
    var orders = new ArrayList<Order>();
    var raw = Lake.readObject(ordersFile).get("orders");
    if (raw instanceof List) {
      for (var item : (List<?>) raw) {
        orders.add(fromMap(castMap(item)));
      }
    }
    return orders;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> castMap(Object raw) {
    return (Map<String, Object>) raw;
  }

  private static Order fromMap(Map<String, Object> m) {
    return new Order(
        (String) m.get("order_id"),
        (String) m.get("customer_id"),
        ((BigDecimal) m.get("account_age_days")).intValueExact(),
        (String) m.get("email"),
        (String) m.get("card_fingerprint"),
        (String) m.get("billing_country"),
        (String) m.get("shipping_country"),
        (String) m.get("currency"),
        ((BigDecimal) m.get("total_minor")).longValueExact(),
        Instant.parse((String) m.get("placed_at")));
  }
}
