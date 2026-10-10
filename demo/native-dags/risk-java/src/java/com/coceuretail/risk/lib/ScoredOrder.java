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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** An order with its score, band and the features that drove it. */
public final class ScoredOrder {
  public final String orderId;
  public final String customerId;
  public final BigDecimal score;
  public final Band band;
  public final List<String> reasons;
  public final long amountUsdCents;

  public ScoredOrder(
      String orderId,
      String customerId,
      BigDecimal score,
      Band band,
      List<String> reasons,
      long amountUsdCents) {
    this.orderId = orderId;
    this.customerId = customerId;
    this.score = score;
    this.band = band;
    this.reasons = reasons;
    this.amountUsdCents = amountUsdCents;
  }

  public Map<String, Object> toMap() {
    var map = new LinkedHashMap<String, Object>();
    map.put("order_id", orderId);
    map.put("customer_id", customerId);
    map.put("score", score);
    map.put("band", band.id);
    map.put("reasons", reasons);
    map.put("amount_usd_cents", amountUsdCents);
    return map;
  }

  public static ScoredOrder fromMap(Map<String, Object> m) {
    var reasons = new java.util.ArrayList<String>();
    for (var reason : (List<?>) m.get("reasons")) {
      reasons.add((String) reason);
    }
    return new ScoredOrder(
        (String) m.get("order_id"),
        (String) m.get("customer_id"),
        (BigDecimal) m.get("score"),
        Band.of((String) m.get("band")),
        reasons,
        ((BigDecimal) m.get("amount_usd_cents")).longValueExact());
  }
}
