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

# Mapped resolution bench: seq-ab

- commit `12ffdd92ee6` on `exp/python-full-supervisor-path`, Python 3.12.16, 10 CPUs, Linux-6.12.76-linuxkit-aarch64-with-glibc2.36
- env overrides: `{"AIRFLOW_EXP_RESOLVE_BEFORE_FORK": "1", "AIRFLOW_EXP_SUPERVISOR_WRITE_PATH": "1"}`
- 5 repetitions per cell, 160 runs in 4251.4 s, started 2026-11-27T08:40:43+00:00
- command: `/Users/jason/airflow-workspace-worktrees/exp-python-full-supervisor-path/.venv/bin/python dev/mapped_resolution_bench/run.py --variant seq-ab --env AIRFLOW_EXP_RESOLVE_BEFORE_FORK=1 --env AIRFLOW_EXP_SUPERVISOR_WRITE_PATH=1`
- Each cell pools the task instances of all repetitions. `wall` is the time inside `supervise_task()`.
  `supervisor peak` is the supervisor process's peak RSS, `supervisor growth` that minus its RSS at the start of the call. `task peak` is the task process's `VmHWM`, sampled every 10 ms.
  `imports` is how often the Dag file was imported for one task instance, in the supervisor process plus the task process. `XCom API reads` counts the requests for XCom values the supervisor made to the API, for itself and for the task process. `handover used` counts task processes that logged using values the supervisor resolved.

## Mapped task instances

| shape | weight | mode | n | wall ms (med / p90) | supervisor peak MB (med / p90) | supervisor growth MB (med / p90) | task peak MB (med / p90) | imports supervisor+task (med) | XCom API reads (med) | handover used |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 15 | 138 / 154 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| expand | light | exec | 15 | 1860 / 6846 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/15 |
| expand | heavy | fork | 15 | 2256 / 7253 | 462 / 462 | 209 / 209 | 659 / 659 | 1+1 | 2 | 15/15 |
| expand | heavy | exec | 15 | 4076 / 8964 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/15 |
| partial_expand | light | fork | 20 | 139 / 150 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 20/20 |
| partial_expand | light | exec | 20 | 1944 / 6912 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 20/20 |
| partial_expand | heavy | fork | 20 | 2261 / 7235 | 462 / 462 | 209 / 209 | 659 / 659 | 1+1 | 2 | 20/20 |
| partial_expand | heavy | exec | 20 | 7376 / 9220 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 20/20 |
| expand_kwargs | light | fork | 15 | 131 / 3158 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| expand_kwargs | light | exec | 15 | 2128 / 7022 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/15 |
| expand_kwargs | heavy | fork | 15 | 2290 / 7241 | 462 / 463 | 209 / 209 | 659 / 659 | 1+1 | 2 | 15/15 |
| expand_kwargs | heavy | exec | 15 | 4156 / 8980 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/15 |
| map | light | fork | 15 | 145 / 166 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| map | light | exec | 15 | 1872 / 6849 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/15 |
| map | heavy | fork | 15 | 2257 / 7227 | 462 / 462 | 209 / 209 | 659 / 659 | 1+1 | 2 | 15/15 |
| map | heavy | exec | 15 | 4079 / 9060 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/15 |
| zip | light | fork | 15 | 138 / 3149 | 263 / 263 | 10 / 10 | 259 / 259 | 1+1 | 3 | 15/15 |
| zip | light | exec | 15 | 2075 / 6838 | 262 / 262 | 8 / 8 | 177 / 177 | 1+1 | 3 | 15/15 |
| zip | heavy | fork | 15 | 2244 / 7248 | 463 / 463 | 209 / 210 | 659 / 659 | 1+1 | 3 | 15/15 |
| zip | heavy | exec | 15 | 4113 / 8914 | 462 / 462 | 208 / 209 | 377 / 377 | 1+1 | 3 | 15/15 |
| concat | light | fork | 20 | 140 / 5153 | 263 / 263 | 10 / 10 | 259 / 259 | 1+1 | 3 | 20/20 |
| concat | light | exec | 20 | 2019 / 7040 | 262 / 262 | 8 / 9 | 177 / 177 | 1+1 | 3 | 20/20 |
| concat | heavy | fork | 20 | 2267 / 7255 | 463 / 463 | 209 / 210 | 659 / 659 | 1+1 | 3 | 20/20 |
| concat | heavy | exec | 20 | 4174 / 9041 | 461 / 462 | 208 / 208 | 377 / 377 | 1+1 | 3 | 20/20 |
| mapped_group | light | fork | 30 | 137 / 664 | 262 / 262 | 9 / 9 | 258 / 259 | 1+1 | 2 | 15/30 |
| mapped_group | light | exec | 30 | 1829 / 6730 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/30 |
| mapped_group | heavy | fork | 30 | 2258 / 2778 | 462 / 462 | 209 / 209 | 658 / 659 | 1+1 | 2 | 15/30 |
| mapped_group | heavy | exec | 30 | 3934 / 8946 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/30 |
| reduce | light | fork | 15 | 134 / 153 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| reduce | light | exec | 15 | 2069 / 7029 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/15 |
| reduce | heavy | fork | 15 | 2249 / 7237 | 462 / 462 | 209 / 209 | 659 / 659 | 1+1 | 2 | 15/15 |
| reduce | heavy | exec | 15 | 3979 / 9015 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/15 |

## Unmapped task instances (producers and reduce)

| shape | weight | mode | n | wall ms (med / p90) | supervisor peak MB (med / p90) | supervisor growth MB (med / p90) | task peak MB (med / p90) | imports supervisor+task (med) | XCom API reads (med) | handover used |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 5 | 120 / 148 | 259 / 259 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| expand | light | exec | 5 | 1754 / 1906 | 257 / 257 | 5 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| expand | heavy | fork | 5 | 1184 / 1240 | 259 / 259 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| expand | heavy | exec | 5 | 3005 / 3018 | 257 / 257 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| partial_expand | light | fork | 5 | 131 / 136 | 259 / 259 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| partial_expand | light | exec | 5 | 1959 / 4858 | 257 / 257 | 5 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| partial_expand | heavy | fork | 5 | 1177 / 1195 | 259 / 259 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| partial_expand | heavy | exec | 5 | 2740 / 3065 | 257 / 257 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| expand_kwargs | light | fork | 5 | 124 / 127 | 259 / 259 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| expand_kwargs | light | exec | 5 | 1696 / 1884 | 257 / 257 | 5 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| expand_kwargs | heavy | fork | 5 | 1185 / 1219 | 259 / 259 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| expand_kwargs | heavy | exec | 5 | 2862 / 3394 | 257 / 257 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| map | light | fork | 5 | 133 / 141 | 259 / 259 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| map | light | exec | 5 | 1751 / 2045 | 257 / 257 | 5 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| map | heavy | fork | 5 | 1191 / 1202 | 259 / 259 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| map | heavy | exec | 5 | 2906 / 3109 | 257 / 257 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| zip | light | fork | 10 | 119 / 128 | 259 / 259 | 7 / 7 | 255 / 255 | 0+1 | 1 | 0/10 |
| zip | light | exec | 10 | 1809 / 2007 | 257 / 257 | 5 / 5 | 177 / 177 | 0+1 | 1 | 0/10 |
| zip | heavy | fork | 10 | 1167 / 1189 | 259 / 259 | 7 / 7 | 455 / 455 | 0+1 | 1 | 0/10 |
| zip | heavy | exec | 10 | 2789 / 3105 | 257 / 257 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/10 |
| concat | light | fork | 10 | 134 / 145 | 259 / 259 | 7 / 7 | 255 / 255 | 0+1 | 1 | 0/10 |
| concat | light | exec | 10 | 1803 / 2027 | 257 / 257 | 5 / 5 | 177 / 177 | 0+1 | 1 | 0/10 |
| concat | heavy | fork | 10 | 1186 / 1195 | 259 / 259 | 7 / 7 | 455 / 455 | 0+1 | 1 | 0/10 |
| concat | heavy | exec | 10 | 2835 / 2951 | 257 / 257 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/10 |
| mapped_group | light | fork | 5 | 128 / 139 | 259 / 259 | 7 / 7 | 255 / 255 | 0+1 | 1 | 0/5 |
| mapped_group | light | exec | 5 | 1752 / 1950 | 257 / 257 | 5 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| mapped_group | heavy | fork | 5 | 1182 / 1188 | 259 / 259 | 7 / 7 | 455 / 455 | 0+1 | 1 | 0/5 |
| mapped_group | heavy | exec | 5 | 2856 / 2982 | 257 / 257 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| reduce | light | fork | 10 | 143 / 171 | 260 / 261 | 7 / 8 | 255 / 256 | 0+1 | 3.5 | 0/10 |
| reduce | light | exec | 10 | 1766 / 2539 | 258 / 260 | 5 / 6 | 177 / 177 | 0+1 | 3.5 | 0/10 |
| reduce | heavy | fork | 10 | 1197 / 1215 | 260 / 261 | 7 / 8 | 455 / 456 | 0+1 | 3.5 | 0/10 |
| reduce | heavy | exec | 10 | 2788 / 2911 | 258 / 260 | 5 / 6 | 377 / 377 | 0+1 | 3.5 | 0/10 |
