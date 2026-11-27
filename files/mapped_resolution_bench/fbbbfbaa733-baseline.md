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

# Mapped resolution bench: baseline

- commit `fbbbfbaa733` on `exp/python-resolve-before-fork`, Python 3.12.16, 10 CPUs, Linux-6.12.76-linuxkit-aarch64-with-glibc2.36
- env overrides: `{}`
- 5 repetitions per cell, 160 runs in 3923.3 s, started 2026-11-27T03:18:16+00:00
- command: `/Users/jason/airflow-workspace/.claude/worktrees/agent-a029f6feea2e8c5a3/.venv/bin/python dev/mapped_resolution_bench/run.py --variant baseline`
- Each cell pools the task instances of all repetitions. `wall` is the time inside `supervise_task()`.
  `supervisor peak` is the supervisor process's peak RSS, `supervisor growth` that minus its RSS at the start of the call. `task peak` is the task process's `VmHWM`, sampled every 10 ms.
  `imports` is how often the Dag file was imported for one task instance, in the supervisor process plus the task process. `XCom API reads` counts the requests for XCom values the supervisor made to the API, for itself and for the task process. `handover used` counts task processes that logged using values the supervisor resolved.

## Mapped task instances

| shape | weight | mode | n | wall ms (med / p90) | supervisor peak MB (med / p90) | supervisor growth MB (med / p90) | task peak MB (med / p90) | imports supervisor+task (med) | XCom API reads (med) | handover used |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 15 | 161 / 190 | 260 / 262 | 7 / 7 | 256 / 258 | 0+1 | 2 | 0/15 |
| expand | light | exec | 15 | 3109 / 7307 | 258 / 258 | 5 / 5 | 176 / 177 | 0+1 | 2 | 0/15 |
| expand | heavy | fork | 15 | 1190 / 1217 | 260 / 260 | 7 / 7 | 456 / 456 | 0+1 | 2 | 0/15 |
| expand | heavy | exec | 15 | 3415 / 3878 | 258 / 258 | 5 / 5 | 377 / 377 | 0+1 | 2 | 0/15 |
| partial_expand | light | fork | 20 | 149 / 191 | 260 / 260 | 7 / 7 | 256 / 256 | 0+1 | 2 | 0/20 |
| partial_expand | light | exec | 20 | 2216 / 7310 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 2 | 0/20 |
| partial_expand | heavy | fork | 20 | 1199 / 1229 | 260 / 260 | 7 / 7 | 456 / 456 | 0+1 | 2 | 0/20 |
| partial_expand | heavy | exec | 20 | 3264 / 4059 | 258 / 260 | 5 / 5 | 377 / 377 | 0+1 | 2 | 0/20 |
| expand_kwargs | light | fork | 15 | 131 / 195 | 260 / 260 | 7 / 7 | 256 / 256 | 0+1 | 2 | 0/15 |
| expand_kwargs | light | exec | 15 | 2012 / 5820 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 2 | 0/15 |
| expand_kwargs | heavy | fork | 15 | 1188 / 1243 | 260 / 260 | 7 / 7 | 456 / 456 | 0+1 | 2 | 0/15 |
| expand_kwargs | heavy | exec | 15 | 3339 / 3967 | 258 / 258 | 5 / 5 | 377 / 377 | 0+1 | 2 | 0/15 |
| map | light | fork | 15 | 137 / 197 | 260 / 260 | 7 / 7 | 256 / 256 | 0+1 | 2 | 0/15 |
| map | light | exec | 15 | 2799 / 8676 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 2 | 0/15 |
| map | heavy | fork | 15 | 1197 / 1331 | 260 / 260 | 7 / 7 | 456 / 456 | 0+1 | 2 | 0/15 |
| map | heavy | exec | 15 | 3812 / 6848 | 258 / 258 | 5 / 5 | 374 / 377 | 0+1 | 2 | 0/15 |
| zip | light | fork | 15 | 148 / 197 | 260 / 261 | 7 / 7 | 256 / 256 | 0+1 | 3 | 0/15 |
| zip | light | exec | 15 | 2070 / 5623 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 3 | 0/15 |
| zip | heavy | fork | 15 | 1212 / 1241 | 260 / 260 | 7 / 7 | 456 / 456 | 0+1 | 3 | 0/15 |
| zip | heavy | exec | 15 | 3626 / 8110 | 258 / 258 | 5 / 5 | 375 / 377 | 0+1 | 3 | 0/15 |
| concat | light | fork | 20 | 148 / 195 | 260 / 260 | 7 / 7 | 256 / 256 | 0+1 | 3 | 0/20 |
| concat | light | exec | 20 | 2047 / 6967 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 3 | 0/20 |
| concat | heavy | fork | 20 | 1205 / 1264 | 260 / 261 | 7 / 7 | 456 / 456 | 0+1 | 3 | 0/20 |
| concat | heavy | exec | 20 | 3514 / 4698 | 258 / 258 | 5 / 5 | 376 / 377 | 0+1 | 3 | 0/20 |
| mapped_group | light | fork | 30 | 150 / 192 | 260 / 261 | 7 / 7 | 256 / 256 | 0+1 | 2 | 0/30 |
| mapped_group | light | exec | 30 | 2575 / 7159 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 2 | 0/30 |
| mapped_group | heavy | fork | 30 | 1183 / 1233 | 260 / 260 | 7 / 7 | 456 / 456 | 0+1 | 2 | 0/30 |
| mapped_group | heavy | exec | 30 | 3454 / 4173 | 258 / 258 | 5 / 5 | 377 / 377 | 0+1 | 2 | 0/30 |
| reduce | light | fork | 15 | 171 / 681 | 260 / 260 | 7 / 7 | 256 / 256 | 0+1 | 2 | 0/15 |
| reduce | light | exec | 15 | 2067 / 5596 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 2 | 0/15 |
| reduce | heavy | fork | 15 | 1199 / 1218 | 260 / 260 | 7 / 7 | 456 / 456 | 0+1 | 2 | 0/15 |
| reduce | heavy | exec | 15 | 3404 / 4551 | 258 / 258 | 5 / 5 | 377 / 377 | 0+1 | 2 | 0/15 |

## Unmapped task instances (producers and reduce)

| shape | weight | mode | n | wall ms (med / p90) | supervisor peak MB (med / p90) | supervisor growth MB (med / p90) | task peak MB (med / p90) | imports supervisor+task (med) | XCom API reads (med) | handover used |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 5 | 152 / 184 | 258 / 260 | 7 / 7 | 254 / 256 | 0+1 | 1 | 0/5 |
| expand | light | exec | 5 | 2109 / 2834 | 256 / 256 | 4 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| expand | heavy | fork | 5 | 1214 / 1259 | 258 / 258 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| expand | heavy | exec | 5 | 3106 / 6470 | 256 / 256 | 4 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| partial_expand | light | fork | 5 | 138 / 167 | 258 / 258 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| partial_expand | light | exec | 5 | 2050 / 2130 | 256 / 256 | 5 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| partial_expand | heavy | fork | 5 | 1164 / 1186 | 258 / 258 | 6 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| partial_expand | heavy | exec | 5 | 3107 / 3830 | 256 / 257 | 4 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| expand_kwargs | light | fork | 5 | 123 / 155 | 258 / 258 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| expand_kwargs | light | exec | 5 | 2250 / 5319 | 256 / 256 | 4 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| expand_kwargs | heavy | fork | 5 | 1181 / 1281 | 258 / 258 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| expand_kwargs | heavy | exec | 5 | 3834 / 5200 | 256 / 256 | 5 / 5 | 368 / 375 | 0+1 | 1 | 0/5 |
| map | light | fork | 5 | 159 / 194 | 258 / 258 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| map | light | exec | 5 | 2522 / 4647 | 256 / 256 | 4 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| map | heavy | fork | 5 | 1170 / 1211 | 258 / 258 | 6 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| map | heavy | exec | 5 | 3343 / 3984 | 256 / 256 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| zip | light | fork | 10 | 158 / 178 | 258 / 259 | 6 / 7 | 254 / 255 | 0+1 | 1 | 0/10 |
| zip | light | exec | 10 | 2093 / 2279 | 256 / 256 | 4 / 4 | 177 / 177 | 0+1 | 1 | 0/10 |
| zip | heavy | fork | 10 | 1180 / 1226 | 258 / 259 | 6 / 6 | 454 / 454 | 0+1 | 1 | 0/10 |
| zip | heavy | exec | 10 | 3379 / 4164 | 256 / 256 | 4 / 5 | 376 / 377 | 0+1 | 1 | 0/10 |
| concat | light | fork | 10 | 149 / 185 | 258 / 259 | 6 / 6 | 254 / 254 | 0+1 | 1 | 0/10 |
| concat | light | exec | 10 | 2117 / 2673 | 256 / 256 | 4 / 4 | 177 / 177 | 0+1 | 1 | 0/10 |
| concat | heavy | fork | 10 | 1188 / 1254 | 258 / 258 | 6 / 6 | 454 / 454 | 0+1 | 1 | 0/10 |
| concat | heavy | exec | 10 | 3069 / 4109 | 256 / 256 | 4 / 5 | 377 / 377 | 0+1 | 1 | 0/10 |
| mapped_group | light | fork | 5 | 163 / 181 | 259 / 259 | 7 / 7 | 254 / 255 | 0+1 | 1 | 0/5 |
| mapped_group | light | exec | 5 | 1938 / 2469 | 257 / 257 | 4 / 5 | 176 / 177 | 0+1 | 1 | 0/5 |
| mapped_group | heavy | fork | 5 | 1178 / 1194 | 258 / 259 | 7 / 7 | 454 / 455 | 0+1 | 1 | 0/5 |
| mapped_group | heavy | exec | 5 | 3053 / 3650 | 257 / 257 | 4 / 5 | 368 / 377 | 0+1 | 1 | 0/5 |
| reduce | light | fork | 10 | 170 / 256 | 259 / 261 | 7 / 8 | 255 / 256 | 0+1 | 3.5 | 0/10 |
| reduce | light | exec | 10 | 2057 / 2936 | 257 / 259 | 5 / 6 | 177 / 177 | 0+1 | 3.5 | 0/10 |
| reduce | heavy | fork | 10 | 1201 / 1236 | 259 / 261 | 7 / 8 | 455 / 456 | 0+1 | 3.5 | 0/10 |
| reduce | heavy | exec | 10 | 3106 / 3995 | 257 / 259 | 5 / 5 | 377 / 377 | 0+1 | 3.5 | 0/10 |
