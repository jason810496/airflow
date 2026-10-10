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

import static java.lang.System.Logger.Level.WARNING;

import org.apache.airflow.sdk.Client;

/** The payments team's settings: its gateway Connection and the exposure tolerance Variable. */
public final class Payments {
  private static final System.Logger log = System.getLogger(Payments.class.getName());

  private Payments() {}

  public static String gatewayHost(Client client, String connection, String defaultHost) {
    var host = client.getConnection(connection).host;
    return host == null ? defaultHost : host;
  }

  public static long chargebackThreshold(Client client, String variable, long defaultUsdCents) {
    var value = client.getVariable(variable);
    if (value == null || value.toString().isBlank()) {
      log.log(WARNING, "Variable {0} is not set, using {1}", variable, defaultUsdCents);
      return defaultUsdCents;
    }
    return Long.parseLong(value.toString().trim());
  }
}
