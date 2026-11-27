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

# Mapped resolution bench: resolve-before-fork

- commit `fbbbfbaa733` on `exp/python-resolve-before-fork`, Python 3.12.16, 10 CPUs, Linux-6.12.76-linuxkit-aarch64-with-glibc2.36
- env overrides: `{"AIRFLOW_EXP_RESOLVE_BEFORE_FORK": "1"}`
- 5 repetitions per cell, 160 runs in 4285.0 s, started 2026-11-27T03:24:18+00:00
- command: `/Users/jason/airflow-workspace/.claude/worktrees/agent-a029f6feea2e8c5a3/.venv/bin/python dev/mapped_resolution_bench/run.py --variant resolve-before-fork --env AIRFLOW_EXP_RESOLVE_BEFORE_FORK=1`
- Each cell pools the task instances of all repetitions. `wall` is the time inside `supervise_task()`.
  `supervisor peak` is the supervisor process's peak RSS, `supervisor growth` that minus its RSS at the start of the call. `task peak` is the task process's `VmHWM`, sampled every 10 ms.
  `imports` is how often the Dag file was imported for one task instance, in the supervisor process plus the task process. `XCom API reads` counts the requests for XCom values the supervisor made to the API, for itself and for the task process. `handover used` counts task processes that logged using values the supervisor resolved.

## Mapped task instances

| shape | weight | mode | n | wall ms (med / p90) | supervisor peak MB (med / p90) | supervisor growth MB (med / p90) | task peak MB (med / p90) | imports supervisor+task (med) | XCom API reads (med) | handover used |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 15 | 172 / 203 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| expand | light | exec | 15 | 3009 / 4347 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/15 |
| expand | heavy | fork | 15 | 2248 / 2408 | 461 / 463 | 208 / 209 | 658 / 659 | 1+1 | 2 | 15/15 |
| expand | heavy | exec | 15 | 4827 / 9196 | 461 / 463 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/15 |
| partial_expand | light | fork | 20 | 181 / 236 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 20/20 |
| partial_expand | light | exec | 20 | 2842 / 7150 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 20/20 |
| partial_expand | heavy | fork | 20 | 2236 / 2262 | 461 / 462 | 208 / 209 | 658 / 659 | 1+1 | 2 | 20/20 |
| partial_expand | heavy | exec | 20 | 4796 / 5660 | 461 / 461 | 208 / 208 | 375 / 377 | 1+1 | 2 | 20/20 |
| expand_kwargs | light | fork | 15 | 146 / 3164 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| expand_kwargs | light | exec | 15 | 2597 / 7192 | 261 / 261 | 8 / 8 | 176 / 177 | 1+1 | 2 | 15/15 |
| expand_kwargs | heavy | fork | 15 | 2216 / 2261 | 462 / 462 | 208 / 209 | 659 / 659 | 1+1 | 2 | 15/15 |
| expand_kwargs | heavy | exec | 15 | 4734 / 9991 | 461 / 461 | 208 / 208 | 370 / 377 | 1+1 | 2 | 15/15 |
| map | light | fork | 15 | 166 / 3161 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| map | light | exec | 15 | 2207 / 5579 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/15 |
| map | heavy | fork | 15 | 2235 / 2265 | 461 / 462 | 208 / 209 | 658 / 659 | 1+1 | 2 | 15/15 |
| map | heavy | exec | 15 | 4494 / 5277 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/15 |
| zip | light | fork | 15 | 151 / 193 | 262 / 263 | 9 / 9 | 259 / 259 | 1+1 | 3 | 15/15 |
| zip | light | exec | 15 | 2171 / 5950 | 261 / 262 | 8 / 8 | 177 / 177 | 1+1 | 3 | 15/15 |
| zip | heavy | fork | 15 | 2256 / 2288 | 461 / 462 | 208 / 209 | 659 / 659 | 1+1 | 3 | 15/15 |
| zip | heavy | exec | 15 | 4523 / 4993 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 3 | 15/15 |
| concat | light | fork | 20 | 182 / 5143 | 262 / 263 | 9 / 9 | 259 / 259 | 1+1 | 3 | 20/20 |
| concat | light | exec | 20 | 2570 / 3873 | 261 / 262 | 8 / 8 | 177 / 177 | 1+1 | 3 | 20/20 |
| concat | heavy | fork | 20 | 2242 / 2269 | 462 / 463 | 209 / 209 | 659 / 659 | 1+1 | 3 | 20/20 |
| concat | heavy | exec | 20 | 4635 / 7527 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 3 | 20/20 |
| mapped_group | light | fork | 30 | 143 / 193 | 262 / 262 | 9 / 9 | 258 / 259 | 1+1 | 2 | 15/30 |
| mapped_group | light | exec | 30 | 2254 / 7199 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/30 |
| mapped_group | heavy | fork | 30 | 2234 / 2270 | 461 / 462 | 208 / 209 | 658 / 659 | 1+1 | 2 | 15/30 |
| mapped_group | heavy | exec | 30 | 4318 / 9068 | 461 / 461 | 207 / 208 | 377 / 377 | 1+1 | 2 | 15/30 |
| reduce | light | fork | 15 | 146 / 3165 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| reduce | light | exec | 15 | 2468 / 5814 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/15 |
| reduce | heavy | fork | 15 | 2228 / 2259 | 461 / 462 | 208 / 209 | 658 / 659 | 1+1 | 2 | 15/15 |
| reduce | heavy | exec | 15 | 4766 / 6780 | 461 / 461 | 207 / 208 | 377 / 377 | 1+1 | 2 | 15/15 |

## Unmapped task instances (producers and reduce)

| shape | weight | mode | n | wall ms (med / p90) | supervisor peak MB (med / p90) | supervisor growth MB (med / p90) | task peak MB (med / p90) | imports supervisor+task (med) | XCom API reads (med) | handover used |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 5 | 133 / 191 | 258 / 261 | 7 / 8 | 254 / 256 | 0+1 | 1 | 0/5 |
| expand | light | exec | 5 | 2093 / 2743 | 256 / 256 | 5 / 5 | 176 / 177 | 0+1 | 1 | 0/5 |
| expand | heavy | fork | 5 | 1173 / 1291 | 258 / 260 | 7 / 7 | 454 / 455 | 0+1 | 1 | 0/5 |
| expand | heavy | exec | 5 | 3689 / 3760 | 256 / 257 | 5 / 5 | 369 / 377 | 0+1 | 1 | 0/5 |
| partial_expand | light | fork | 5 | 135 / 140 | 258 / 258 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| partial_expand | light | exec | 5 | 2007 / 2191 | 256 / 256 | 5 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| partial_expand | heavy | fork | 5 | 1192 / 1232 | 258 / 258 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| partial_expand | heavy | exec | 5 | 3681 / 4182 | 256 / 256 | 5 / 5 | 374 / 377 | 0+1 | 1 | 0/5 |
| expand_kwargs | light | fork | 5 | 139 / 248 | 258 / 258 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| expand_kwargs | light | exec | 5 | 2398 / 2879 | 256 / 256 | 5 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| expand_kwargs | heavy | fork | 5 | 1182 / 1209 | 258 / 258 | 6 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| expand_kwargs | heavy | exec | 5 | 3529 / 3823 | 256 / 256 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| map | light | fork | 5 | 139 / 162 | 258 / 258 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| map | light | exec | 5 | 1986 / 3003 | 256 / 256 | 4 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| map | heavy | fork | 5 | 1202 / 1219 | 258 / 258 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| map | heavy | exec | 5 | 3050 / 3859 | 256 / 256 | 5 / 5 | 375 / 377 | 0+1 | 1 | 0/5 |
| zip | light | fork | 10 | 128 / 173 | 258 / 259 | 6 / 6 | 255 / 255 | 0+1 | 1 | 0/10 |
| zip | light | exec | 10 | 2082 / 2315 | 256 / 256 | 4 / 5 | 177 / 177 | 0+1 | 1 | 0/10 |
| zip | heavy | fork | 10 | 1168 / 1218 | 258 / 259 | 6 / 6 | 454 / 455 | 0+1 | 1 | 0/10 |
| zip | heavy | exec | 10 | 3055 / 4135 | 256 / 256 | 4 / 4 | 374 / 377 | 0+1 | 1 | 0/10 |
| concat | light | fork | 10 | 132 / 162 | 259 / 259 | 6 / 6 | 255 / 255 | 0+1 | 1 | 0/10 |
| concat | light | exec | 10 | 2046 / 3078 | 256 / 256 | 4 / 5 | 177 / 177 | 0+1 | 1 | 0/10 |
| concat | heavy | fork | 10 | 1200 / 1234 | 258 / 259 | 6 / 6 | 455 / 455 | 0+1 | 1 | 0/10 |
| concat | heavy | exec | 10 | 3023 / 4085 | 256 / 256 | 4 / 5 | 377 / 377 | 0+1 | 1 | 0/10 |
| mapped_group | light | fork | 5 | 129 / 157 | 259 / 259 | 7 / 7 | 255 / 255 | 0+1 | 1 | 0/5 |
| mapped_group | light | exec | 5 | 2021 / 2644 | 256 / 257 | 4 / 4 | 177 / 177 | 0+1 | 1 | 0/5 |
| mapped_group | heavy | fork | 5 | 1185 / 1193 | 259 / 259 | 7 / 7 | 455 / 455 | 0+1 | 1 | 0/5 |
| mapped_group | heavy | exec | 5 | 3044 / 3533 | 257 / 257 | 4 / 4 | 377 / 377 | 0+1 | 1 | 0/5 |
| reduce | light | fork | 10 | 150 / 184 | 260 / 261 | 7 / 8 | 255 / 256 | 0+1 | 3.5 | 0/10 |
| reduce | light | exec | 10 | 2041 / 2705 | 257 / 259 | 5 / 6 | 177 / 177 | 0+1 | 3.5 | 0/10 |
| reduce | heavy | fork | 10 | 1193 / 1229 | 259 / 261 | 7 / 8 | 455 / 456 | 0+1 | 3.5 | 0/10 |
| reduce | heavy | exec | 10 | 3252 / 4408 | 258 / 259 | 5 / 6 | 376 / 377 | 0+1 | 3.5 | 0/10 |
