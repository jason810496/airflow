// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package risk reads the decisions that the risk team publishes for a storefront batch.
//
// The contract is the file <dir>/decisions.json, where dir is the value of the Variable
// handoff.risk.latest_decisions:
//
//	{
//	  "batch_id": "manual__2026-10-07T00-00-00",
//	  "storefront_batch_dir": "/files/demo/lake/storefront/scheduled__2026-10-07T00-00-00",
//	  "decisions": [
//	    {"order_id": "ORD-20261007-00042", "action": "block", "score": 0.8, "reasons": ["card_velocity"]},
//	    {"order_id": "ORD-20261007-00107", "action": "review", "score": 0.45, "reasons": ["cross_border_high_value"]}
//	  ]
//	}
//
// storefront_batch_dir and decisions[].order_id and decisions[].action are required, and the action
// is approve, review or block. The other fields are optional and ignored. An order that has no
// decision is treated as approved, so risk only has to list the orders it screened.
package risk

import (
	"fmt"
	"path/filepath"

	"coceuretail.example/finance/internal/lake"
)

type Action string

const (
	Approve Action = "approve"
	Review  Action = "review"
	Block   Action = "block"
)

// NoScreening is the message of a batch that risk did not screen.
const NoScreening = "no risk screening for this batch"

type Decision struct {
	OrderID string   `json:"order_id"`
	Action  Action   `json:"action"`
	Score   float64  `json:"score,omitempty"`
	Reasons []string `json:"reasons,omitempty"`
}

type decisionsFile struct {
	BatchID            string     `json:"batch_id"`
	StorefrontBatchDir string     `json:"storefront_batch_dir"`
	Decisions          []Decision `json:"decisions"`
}

// Screening is what load_risk_decisions hands to the journal.
type Screening struct {
	Screened bool `json:"screened"`
	// Message says why a batch is not screened, or what risk decided.
	Message      string     `json:"message"`
	DecisionsDir string     `json:"decisions_dir,omitempty"`
	RiskBatchID  string     `json:"risk_batch_id,omitempty"`
	Approved     int        `json:"approved"`
	Review       int        `json:"review"`
	Blocked      int        `json:"blocked"`
	Decisions    []Decision `json:"decisions,omitempty"`
}

// ActionFor is the action risk took on an order, approve when risk did not list it.
func (s Screening) ActionFor(orderID string) Action {
	for _, d := range s.Decisions {
		if d.OrderID == orderID {
			return d.Action
		}
	}
	return Approve
}

// Load reads the decisions in decisionsDir for the storefront batch storefrontDir. An empty
// decisionsDir, a missing file and decisions for another batch all mean the batch was not screened.
func Load(decisionsDir, storefrontDir string) (Screening, error) {
	if decisionsDir == "" {
		return Screening{Message: NoScreening}, nil
	}
	var file decisionsFile
	path := filepath.Join(decisionsDir, "decisions.json")
	if !lake.Exists(path) {
		return Screening{Message: NoScreening}, nil
	}
	if err := lake.ReadJSON(path, &file); err != nil {
		return Screening{}, err
	}
	if filepath.Clean(file.StorefrontBatchDir) != filepath.Clean(storefrontDir) {
		return Screening{Message: NoScreening}, nil
	}
	s := Screening{
		Screened:     true,
		DecisionsDir: decisionsDir,
		RiskBatchID:  file.BatchID,
		Decisions:    file.Decisions,
	}
	seen := map[string]bool{}
	for _, d := range file.Decisions {
		if seen[d.OrderID] {
			return Screening{}, fmt.Errorf("%s decides order %s twice", path, d.OrderID)
		}
		seen[d.OrderID] = true
		switch d.Action {
		case Approve:
			s.Approved++
		case Review:
			s.Review++
		case Block:
			s.Blocked++
		default:
			return Screening{}, fmt.Errorf("%s: order %s has action %q, expected approve, review or block", path, d.OrderID, d.Action)
		}
	}
	s.Message = fmt.Sprintf("risk decided %d orders: %d approve, %d review, %d block",
		len(file.Decisions), s.Approved, s.Review, s.Blocked)
	return s, nil
}
