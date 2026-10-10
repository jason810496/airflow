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

import static java.lang.System.Logger.Level.INFO;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;

/** Finds the storefront batch a run was asked to screen. */
public final class Handoff {
  private static final System.Logger log = System.getLogger(Handoff.class.getName());

  private Handoff() {}

  /** Reads the batch directory from the Variable named by the trigger conf, else the storefront contract. */
  public static Path requestedBatch(Context context, Client client) throws IOException {
    var conf = context.dagRun.conf;
    log.log(INFO, "Run {0} was triggered with conf {1}", context.dagRun.runId, conf);
    var variable = conf.get("contract") instanceof String ? (String) conf.get("contract") : Lake.STOREFRONT_BATCH_VARIABLE;
    var value = client.getVariable(variable);
    if (value == null || value.toString().isBlank()) {
      throw new IllegalStateException("Variable " + variable + " is not set, so there is no batch to screen");
    }
    var batchDir = Path.of(value.toString());
    var orders = Lake.orders(batchDir);
    var flagged = Lake.flaggedOrderIds(batchDir);

    var rows = new ArrayList<List<String>>();
    for (var name : List.of("orders.json", "suspicious_orders.json", "sales_summary.json")) {
      rows.add(List.of(name, String.valueOf(Files.size(batchDir.resolve(name)))));
    }
    log.log(
        INFO,
        "Screening batch {0} from {1}, requested by {2}\n{3}\n{4} orders, {5} flagged by checkout",
        Lake.batchId(batchDir),
        variable,
        conf.getOrDefault("requested_by", "unknown"),
        Table.render(List.of("file", "bytes"), rows, 1),
        orders.size(),
        flagged.size());
    return batchDir;
  }

  /** The conf of the run this team hands to finance, naming the Variable that points at the decisions. */
  public static Map<String, Object> financeConf() {
    var conf = new LinkedHashMap<String, Object>();
    conf.put("requested_by", Lake.TEAM);
    conf.put("contract", Lake.LATEST_DECISIONS_VARIABLE);
    return conf;
  }
}
