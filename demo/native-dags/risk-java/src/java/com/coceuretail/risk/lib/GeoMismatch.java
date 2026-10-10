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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Where an order is billed, shipped and priced, and whether those agree. */
public final class GeoMismatch {
  public static final String BILLING_SHIPPING_MISMATCH = "billing_shipping_mismatch";
  public static final String CURRENCY_COUNTRY_MISMATCH = "currency_country_mismatch";
  public static final String HIGH_RISK_DESTINATION = "high_risk_destination";

  /** Shipping destinations the fraud engine treats as a high-risk corridor. */
  static final Set<String> HIGH_RISK_DESTINATIONS = Set.of("NG", "VN", "BR", "ID", "PH");

  /** The currency the store charges in each billing country. */
  static final Map<String, String> CURRENCY_BY_COUNTRY =
      Map.of(
          "US", "USD", "CA", "USD", "AU", "USD", "GB", "GBP", "DE", "EUR",
          "FR", "EUR", "NL", "EUR", "ES", "EUR", "IE", "EUR", "JP", "JPY");

  private GeoMismatch() {}

  public static Map<String, Map<String, Double>> compute(List<Order> orders) {
    var result = new LinkedHashMap<String, Map<String, Double>>();
    for (var order : orders) {
      var features = new LinkedHashMap<String, Double>();
      features.put(
          BILLING_SHIPPING_MISMATCH, flag(!order.billingCountry.equals(order.shippingCountry)));
      features.put(
          CURRENCY_COUNTRY_MISMATCH,
          flag(!order.currency.equals(CURRENCY_BY_COUNTRY.getOrDefault(order.billingCountry, "USD"))));
      features.put(HIGH_RISK_DESTINATION, flag(HIGH_RISK_DESTINATIONS.contains(order.shippingCountry)));
      result.put(order.orderId, features);
    }
    return result;
  }

  private static double flag(boolean on) {
    return on ? 1.0 : 0.0;
  }
}
