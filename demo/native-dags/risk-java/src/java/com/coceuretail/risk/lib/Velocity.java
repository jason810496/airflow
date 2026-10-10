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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * How fast a card and a customer are ordering. An order counts the orders of its card inside
 * {@code windowMinutes} of it, and the orders of its customer inside a day.
 */
public final class Velocity {
  public static final String CARD_VELOCITY = "card_velocity";
  public static final String CUSTOMER_VELOCITY = "customer_velocity";

  private static final Duration CUSTOMER_WINDOW = Duration.ofHours(24);

  private Velocity() {}

  public static Map<String, Map<String, Double>> compute(List<Order> orders, int windowMinutes) {
    var pool = Executors.newFixedThreadPool(2);
    try {
      var card =
          pool.submit(
              () ->
                  pressure(orders, o -> o.cardFingerprint, Duration.ofMinutes(windowMinutes), 4.0));
      var customer = pool.submit(() -> pressure(orders, o -> o.customerId, CUSTOMER_WINDOW, 3.0));
      var result = new LinkedHashMap<String, Map<String, Double>>();
      var cardPressure = card.get();
      var customerPressure = customer.get();
      for (var order : orders) {
        var features = new LinkedHashMap<String, Double>();
        features.put(CARD_VELOCITY, cardPressure.get(order.orderId));
        features.put(CUSTOMER_VELOCITY, customerPressure.get(order.orderId));
        result.put(order.orderId, features);
      }
      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while computing velocity", e);
    } catch (ExecutionException e) {
      throw new IllegalStateException("Velocity computation failed", e.getCause());
    } finally {
      pool.shutdown();
    }
  }

  /** Other orders of the same key within the window, as a share of {@code saturation}, capped at 1. */
  static Map<String, Double> pressure(
      List<Order> orders, Function<Order, String> key, Duration window, double saturation) {
    var result = new LinkedHashMap<String, Double>();
    var groups = orders.stream().collect(Collectors.groupingBy(key));
    for (var group : groups.values()) {
      var sorted = new ArrayList<>(group);
      sorted.sort(Comparator.comparing((Order o) -> o.placedAt));
      for (var order : sorted) {
        long nearby =
            sorted.stream()
                    .filter(o -> Duration.between(order.placedAt, o.placedAt).abs().compareTo(window) <= 0)
                    .count()
                - 1;
        result.put(order.orderId, Math.min(1.0, nearby / saturation));
      }
    }
    return result;
  }
}
