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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reputation of the card and the email behind an order. Deterministic: the same orders always give
 * the same features, each between 0 and 1.
 */
public final class DeviceReputation {
  public static final String DISPOSABLE_EMAIL = "disposable_email";
  public static final String SHARED_CARD = "shared_card";
  public static final String NEW_ACCOUNT = "new_account";

  static final Set<String> DISPOSABLE_DOMAINS =
      Set.of(
          "tempmail.dev",
          "mailinator.example",
          "burner.example",
          "mailinator.com",
          "guerrillamail.com",
          "10minutemail.com",
          "yopmail.com",
          "trashmail.com");

  static final Set<String> TRUSTED_DOMAINS =
      Set.of("gmail.com", "outlook.com", "yahoo.com", "icloud.com", "hotmail.com", "proton.me");

  private DeviceReputation() {}

  public static Map<String, Map<String, Double>> compute(List<Order> orders) {
    var customersPerCard =
        orders.parallelStream()
            .collect(
                Collectors.groupingByConcurrent(
                    o -> o.cardFingerprint, Collectors.mapping(o -> o.customerId, Collectors.toSet())));
    var byOrder =
        orders.parallelStream()
            .collect(
                Collectors.toConcurrentMap(
                    o -> o.orderId,
                    o -> features(o, customersPerCard.getOrDefault(o.cardFingerprint, Set.of()))));
    var ordered = new LinkedHashMap<String, Map<String, Double>>();
    orders.forEach(o -> ordered.put(o.orderId, byOrder.get(o.orderId)));
    return ordered;
  }

  private static Map<String, Double> features(Order order, Set<String> customersOnCard) {
    var features = new LinkedHashMap<String, Double>();
    features.put(DISPOSABLE_EMAIL, domainRisk(order.emailDomain()));
    features.put(SHARED_CARD, Math.min(1.0, Math.max(0, new HashSet<>(customersOnCard).size() - 1) / 2.0));
    features.put(NEW_ACCOUNT, accountRisk(order.accountAgeDays));
    return features;
  }

  static double domainRisk(String domain) {
    if (DISPOSABLE_DOMAINS.contains(domain)) {
      return 1.0;
    }
    return TRUSTED_DOMAINS.contains(domain) ? 0.0 : 0.25;
  }

  static double accountRisk(int ageDays) {
    if (ageDays < 2) {
      return 1.0;
    }
    return ageDays < 14 ? 0.4 : 0.0;
  }
}
