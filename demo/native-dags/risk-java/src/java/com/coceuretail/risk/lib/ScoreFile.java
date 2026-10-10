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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** The scores.json a run writes for a batch, and what apply_decisions reads back from it. */
public final class ScoreFile {
  public static final String NAME = "scores.json";

  public final Path batchDir;
  public final List<ScoredOrder> orders;

  private ScoreFile(Path batchDir, List<ScoredOrder> orders) {
    this.batchDir = batchDir;
    this.orders = orders;
  }

  public static Path write(Path batchDir, ModelWeights model, double reviewFrom, double blockFrom, List<ScoredOrder> scored) {
    var document = new LinkedHashMap<String, Object>();
    document.put("storefront_batch_dir", batchDir.toString());
    document.put("batch_id", Lake.batchId(batchDir));
    document.put("model", model.toMap());
    document.put("review_from", BigDecimal.valueOf(reviewFrom));
    document.put("block_from", BigDecimal.valueOf(blockFrom));
    document.put("scores", scored.stream().map(ScoredOrder::toMap).collect(Collectors.toList()));
    return Lake.writeJson(Lake.riskDir(batchDir).resolve(NAME), document);
  }

  public static ScoreFile read(String scoresPath) {
    var scores = Lake.readObject(Path.of(scoresPath));
    var orders = new ArrayList<ScoredOrder>();
    for (var raw : (List<?>) scores.get("scores")) {
      @SuppressWarnings("unchecked")
      var order = ScoredOrder.fromMap((Map<String, Object>) raw);
      orders.add(order);
    }
    return new ScoreFile(Path.of((String) scores.get("storefront_batch_dir")), orders);
  }
}
