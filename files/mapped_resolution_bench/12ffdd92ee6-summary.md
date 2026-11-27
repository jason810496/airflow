<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Mapped resolution bench: baseline vs A vs A+B

Sequential runs on `exp/python-full-supervisor-path` (`12ffdd92ee6`), 5 reps per cell, flags select the variant.
A = `AIRFLOW_EXP_RESOLVE_BEFORE_FORK=1`, A+B adds `AIRFLOW_EXP_SUPERVISOR_WRITE_PATH=1`.
`--check`: all 800 task instances match baseline for both A and A+B.

## Mapped task instances (all 8 shapes pooled)

| weight | mode | variant | wall med ms | wall p90 ms | supervisor peak MB | task peak MB | imports sup+task |
|---|---|---|---:|---:|---:|---:|---|
| light | fork | baseline | 137 | 179 | 260 | 256 | 0+1 |
| light | fork | A | 142 | 191 | 262 | 259 | 1+1 |
| light | fork | A+B | 138 | 167 | 262 | 259 | 1+1 |
| light | exec | baseline | 1969 | 6844 | 258 | 177 | 0+1 |
| light | exec | A | 1946 | 3100 | 261 | 177 | 1+1 |
| light | exec | A+B | 1932 | 6934 | 261 | 177 | 1+1 |
| heavy | fork | baseline | 1199 | 1230 | 260 | 456 | 0+1 |
| heavy | fork | A | 2250 | 2280 | 462 | 659 | 1+1 |
| heavy | fork | A+B | 2258 | 7239 | 462 | 659 | 1+1 |
| heavy | exec | baseline | 2974 | 3676 | 258 | 377 | 0+1 |
| heavy | exec | A | 4010 | 4285 | 461 | 377 | 1+1 |
| heavy | exec | A+B | 4113 | 9052 | 461 | 377 | 1+1 |

Unmapped task instances (producers, reduce) show no change in any variant.

## Tail stalls

Task instances more than 4.5 s above their cell's median: baseline 29/800, A 44/800, A+B 127/800.
Likely the ~5 s supervisor `select()` stall found in experiment A; not yet explained why A+B raises it.

## Not covered

The harness pushes small XComs with the default backend, so B's memory cost with large values or a custom backend is not measured here (see `files/exp-python-supervisor-path.md`).
