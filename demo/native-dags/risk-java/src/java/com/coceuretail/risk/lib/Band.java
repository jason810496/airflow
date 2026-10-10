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

/** What happens to an order: approve below the review threshold, block at or above the block threshold. */
public enum Band {
  APPROVE("approve"),
  REVIEW("review"),
  BLOCK("block");

  public final String id;

  Band(String id) {
    this.id = id;
  }

  public static Band of(String id) {
    for (var band : values()) {
      if (band.id.equals(id)) {
        return band;
      }
    }
    throw new IllegalArgumentException("Unknown band: " + id);
  }

  public static Band of(BigDecimal score, double reviewFrom, double blockFrom) {
    if (score.compareTo(BigDecimal.valueOf(blockFrom)) >= 0) {
      return BLOCK;
    }
    return score.compareTo(BigDecimal.valueOf(reviewFrom)) >= 0 ? REVIEW : APPROVE;
  }
}
