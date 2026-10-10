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
import java.util.Map;

/** Money as BigDecimal, converted to whole US cents the same way the storefront does. */
public final class Money {
  public static final Map<String, BigDecimal> DEFAULT_RATES =
      Map.of(
          "USD", new BigDecimal("1"),
          "EUR", new BigDecimal("1.08"),
          "GBP", new BigDecimal("1.27"),
          "JPY", new BigDecimal("0.0067"));

  private static final Map<String, Integer> MINOR_DIGITS =
      Map.of("USD", 2, "EUR", 2, "GBP", 2, "JPY", 0);

  private Money() {}

  /** US dollars per one major unit, read from a {@code fx_rates} object; missing currencies keep the default. */
  public static Map<String, BigDecimal> rates(Object fxRates) {
    var merged = new java.util.HashMap<>(DEFAULT_RATES);
    if (fxRates instanceof Map) {
      for (var entry : ((Map<?, ?>) fxRates).entrySet()) {
        if (entry.getValue() instanceof BigDecimal) {
          merged.put(String.valueOf(entry.getKey()), (BigDecimal) entry.getValue());
        }
      }
    }
    return merged;
  }

  public static long toUsdCents(long amountMinor, String currency, Map<String, BigDecimal> rates) {
    var rate = rates.get(currency);
    if (rate == null) {
      throw new IllegalArgumentException("No USD rate for currency " + currency);
    }
    int digits = MINOR_DIGITS.getOrDefault(currency, 2);
    return BigDecimal.valueOf(amountMinor)
        .multiply(rate)
        .multiply(BigDecimal.valueOf(100))
        .divide(BigDecimal.TEN.pow(digits), 0, RoundingMode.HALF_UP)
        .longValueExact();
  }

  public static String formatUsd(long cents) {
    var dollars = BigDecimal.valueOf(cents, 2);
    return String.format("$%,.2f", dollars);
  }
}
