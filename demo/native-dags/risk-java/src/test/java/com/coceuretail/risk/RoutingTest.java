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

package com.coceuretail.risk;

import static com.coceuretail.risk.FraudScreeningDagBuilder.TaskIds.AUTO_APPROVE;
import static com.coceuretail.risk.FraudScreeningDagBuilder.TaskIds.BLOCK_AND_REFUND;
import static com.coceuretail.risk.FraudScreeningDagBuilder.TaskIds.QUEUE_MANUAL_REVIEW;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.coceuretail.risk.lib.Band;
import com.coceuretail.risk.lib.Decisions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RoutingTest {
  private static Map<String, Object> decisions(String... actions) {
    var list = new ArrayList<Map<String, Object>>();
    for (var action : actions) {
      var decision = new LinkedHashMap<String, Object>();
      decision.put("order_id", "ORD-" + list.size());
      decision.put("action", action);
      list.add(decision);
    }
    return Map.of("decisions", list);
  }

  @Test
  void worstBandIsTheMostSevereAction() {
    assertEquals(Band.APPROVE, Decisions.worstBand(decisions("approve", "approve")));
    assertEquals(Band.REVIEW, Decisions.worstBand(decisions("approve", "review", "approve")));
    assertEquals(Band.BLOCK, Decisions.worstBand(decisions("review", "block", "approve")));
    assertEquals(Band.BLOCK, Decisions.worstBand(decisions("block", "review")));
  }

  @Test
  void anEmptyBatchIsCleared() {
    assertEquals(Band.APPROVE, Decisions.worstBand(Map.of("decisions", List.of())));
  }

  @Test
  void eachWorstBandRunsItsOwnCase() {
    assertEquals(AUTO_APPROVE, FraudScreeningDag.caseFor(Band.APPROVE));
    assertEquals(QUEUE_MANUAL_REVIEW, FraudScreeningDag.caseFor(Band.REVIEW));
    assertEquals(BLOCK_AND_REFUND, FraudScreeningDag.caseFor(Band.BLOCK));
  }

  @Test
  void aMixedBatchWithABlockedOrderIsRoutedToBlock() {
    var worst = Decisions.worstBand(decisions("approve", "review", "block"));
    assertEquals(BLOCK_AND_REFUND, FraudScreeningDag.caseFor(worst));
  }
}
