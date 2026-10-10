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

/** What each outcome of the worst-band routing writes to the outbox of the batch. */
public final class Routes {
  private static final System.Logger log = System.getLogger(Routes.class.getName());

  private Routes() {}

  public static void clear(String scoresPath) {
    var scores = ScoreFile.read(scoresPath);
    var approved = Decisions.in(scores.orders, Band.APPROVE);
    Outbox.markCleared(scores.batchDir, approved);
    log.log(INFO, "No order needs review or a block, cleared all {0} orders", approved.size());
  }

  public static void queueReview(String scoresPath) {
    var scores = ScoreFile.read(scoresPath);
    var review = Decisions.in(scores.orders, Band.REVIEW);
    Outbox.queueManualReview(scores.batchDir, review);
    log.log(INFO, "Queued {0} orders for manual review", review.size());
  }

  /** Refunds and lists the blocked orders. Orders still in review are queued too, so none is dropped. */
  public static void blockAndRefund(String scoresPath, String gatewayHost) {
    var scores = ScoreFile.read(scoresPath);
    var blocked = Decisions.in(scores.orders, Band.BLOCK);
    var review = Decisions.in(scores.orders, Band.REVIEW);
    Outbox.requestRefunds(scores.batchDir, blocked, gatewayHost);
    Outbox.listBlocked(scores.batchDir, blocked);
    if (!review.isEmpty()) {
      Outbox.queueManualReview(scores.batchDir, review);
    }
    log.log(
        INFO,
        "Blocked and refunded {0} orders through {1}, queued {2} more for manual review",
        blocked.size(),
        gatewayHost,
        review.size());
  }
}
