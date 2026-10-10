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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** The shared lake and outbox. Each team writes under {@code <lake>/<team>/<batch_id>/}. */
public final class Lake {
  public static final String TEAM = "risk";
  public static final String STOREFRONT_BATCH_VARIABLE = "handoff.storefront.latest_batch";
  public static final String LATEST_DECISIONS_VARIABLE = "handoff.risk.latest_decisions";

  private Lake() {}

  public static Path lakeRoot() {
    return Path.of(System.getenv().getOrDefault("COCEU_LAKE_ROOT", "/files/demo/lake"));
  }

  public static Path outboxRoot() {
    return Path.of(System.getenv().getOrDefault("COCEU_OUTBOX_ROOT", "/files/demo/outbox"));
  }

  /** The batch id is the name of the storefront batch directory. */
  public static String batchId(Path storefrontBatchDir) {
    return storefrontBatchDir.getFileName().toString();
  }

  /** Where the risk team writes for a storefront batch. */
  public static Path riskDir(Path storefrontBatchDir) {
    return lakeRoot().resolve(TEAM).resolve(batchId(storefrontBatchDir));
  }

  public static Path outboxDir(Path storefrontBatchDir) {
    return outboxRoot().resolve(TEAM).resolve(batchId(storefrontBatchDir));
  }

  public static List<Order> orders(Path batchDir) {
    return Order.load(batchDir.resolve("orders.json"));
  }

  public static List<Order> orders(String batchDir) {
    return orders(Path.of(batchDir));
  }

  /** The orders the storefront checkout rules already flagged. */
  public static Set<String> flaggedOrderIds(Path batchDir) {
    var suspicious = readObject(batchDir.resolve("suspicious_orders.json"));
    return ((List<?>) suspicious.get("orders"))
        .stream().map(o -> (String) ((Map<?, ?>) o).get("order_id")).collect(Collectors.toSet());
  }

  public static Map<String, BigDecimal> fxRates(Path batchDir) {
    return Money.rates(readObject(batchDir.resolve("sales_summary.json")).get("fx_rates"));
  }

  public static Map<String, Object> readObject(Path file) {
    try {
      return Json.parseObject(Files.readString(file, StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read " + file, e);
    }
  }

  /** Writes through a temporary file, so a reader never sees half a file. */
  public static Path writeJson(Path file, Object value) {
    try {
      Files.createDirectories(file.getParent());
      var staging = file.resolveSibling(file.getFileName() + ".tmp");
      Files.writeString(staging, Json.write(value), StandardCharsets.UTF_8);
      Files.move(staging, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot write " + file, e);
    }
  }
}
