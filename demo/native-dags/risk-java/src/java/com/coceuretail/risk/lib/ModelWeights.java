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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The scoring model of the Variable {@code risk.model_weights}:
 * {@code {"bias": -3.2, "weights": {"<feature>": <weight>, ...}}}.
 */
public final class ModelWeights {
  public static final String DEFAULT_JSON =
      "{\"bias\": -3.2, \"weights\": {"
          + "\"disposable_email\": 2.2, \"shared_card\": 1.6, \"new_account\": 1.2, "
          + "\"card_velocity\": 3.0, \"customer_velocity\": 0.8, "
          + "\"billing_shipping_mismatch\": 1.0, \"currency_country_mismatch\": 0.8, "
          + "\"high_risk_destination\": 1.6, \"high_value\": 1.5, \"checkout_flagged\": 0.6}}";

  public static final String SNAPSHOT_FILE = "model_weights.json";

  public final double bias;
  public final Map<String, Double> weights;

  public ModelWeights(double bias, Map<String, Double> weights) {
    this.bias = bias;
    this.weights = weights;
  }

  public static ModelWeights parse(String json) {
    var root = Json.parseObject(json == null || json.isBlank() ? DEFAULT_JSON : json);
    if (!(root.get("weights") instanceof Map) || !(root.get("bias") instanceof BigDecimal)) {
      throw new IllegalArgumentException(
          "Variable risk.model_weights must look like {\"bias\": -3.2, \"weights\": {\"card_velocity\": 3.0}}");
    }
    var weights = new LinkedHashMap<String, Double>();
    for (var entry : ((Map<?, ?>) root.get("weights")).entrySet()) {
      weights.put(String.valueOf(entry.getKey()), ((BigDecimal) entry.getValue()).doubleValue());
    }
    return new ModelWeights(((BigDecimal) root.get("bias")).doubleValue(), weights);
  }

  /** Writes the weights a run scores with next to its scores, so a later change of the Variable cannot alter them. */
  public static ModelWeights snapshot(Path batchDir, Object variableValue) {
    var model = parse(variableValue == null ? null : variableValue.toString());
    Lake.writeJson(snapshotFile(batchDir), model.toMap());
    return model;
  }

  public static Path snapshotFile(Path batchDir) {
    return Lake.riskDir(batchDir).resolve(SNAPSHOT_FILE);
  }

  public static ModelWeights readSnapshot(Path batchDir) {
    var file = snapshotFile(batchDir);
    try {
      return parse(Files.readString(file));
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read " + file, e);
    }
  }

  public Map<String, Object> toMap() {
    var map = new LinkedHashMap<String, Object>();
    map.put("bias", bias);
    map.put("weights", weights);
    return map;
  }
}
