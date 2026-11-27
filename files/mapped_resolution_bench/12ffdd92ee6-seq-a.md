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

# Mapped resolution bench: seq-a

- commit `12ffdd92ee6` on `exp/python-full-supervisor-path`, Python 3.12.16, 10 CPUs, Linux-6.12.76-linuxkit-aarch64-with-glibc2.36
- env overrides: `{"AIRFLOW_EXP_RESOLVE_BEFORE_FORK": "1"}`
- 5 repetitions per cell, 160 runs in 3968.1 s, started 2026-11-27T07:15:26+00:00
- command: `/Users/jason/airflow-workspace-worktrees/exp-python-full-supervisor-path/.venv/bin/python dev/mapped_resolution_bench/run.py --variant seq-a --env AIRFLOW_EXP_RESOLVE_BEFORE_FORK=1`
- Each cell pools the task instances of all repetitions. `wall` is the time inside `supervise_task()`.
  `supervisor peak` is the supervisor process's peak RSS, `supervisor growth` that minus its RSS at the start of the call. `task peak` is the task process's `VmHWM`, sampled every 10 ms.
  `imports` is how often the Dag file was imported for one task instance, in the supervisor process plus the task process. `XCom API reads` counts the requests for XCom values the supervisor made to the API, for itself and for the task process. `handover used` counts task processes that logged using values the supervisor resolved.

## Mapped task instances

| shape | weight | mode | n | wall ms (med / p90) | supervisor peak MB (med / p90) | supervisor growth MB (med / p90) | task peak MB (med / p90) | imports supervisor+task (med) | XCom API reads (med) | handover used |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 15 | 130 / 179 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| expand | light | exec | 15 | 1877 / 6785 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/15 |
| expand | heavy | fork | 15 | 2242 / 2269 | 462 / 462 | 209 / 209 | 659 / 659 | 1+1 | 2 | 15/15 |
| expand | heavy | exec | 15 | 4180 / 4235 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/15 |
| partial_expand | light | fork | 20 | 136 / 656 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 20/20 |
| partial_expand | light | exec | 20 | 1896 / 2265 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 20/20 |
| partial_expand | heavy | fork | 20 | 2242 / 2274 | 462 / 462 | 209 / 209 | 659 / 659 | 1+1 | 2 | 20/20 |
| partial_expand | heavy | exec | 20 | 4151 / 22992 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 20/20 |
| expand_kwargs | light | fork | 15 | 138 / 169 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| expand_kwargs | light | exec | 15 | 2062 / 2642 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/15 |
| expand_kwargs | heavy | fork | 15 | 2245 / 2309 | 462 / 462 | 209 / 209 | 659 / 659 | 1+1 | 2 | 15/15 |
| expand_kwargs | heavy | exec | 15 | 3933 / 4152 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/15 |
| map | light | fork | 15 | 149 / 5146 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| map | light | exec | 15 | 1881 / 2130 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/15 |
| map | heavy | fork | 15 | 2242 / 2310 | 462 / 462 | 209 / 209 | 659 / 659 | 1+1 | 2 | 15/15 |
| map | heavy | exec | 15 | 4086 / 4226 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/15 |
| zip | light | fork | 15 | 145 / 166 | 262 / 263 | 9 / 9 | 259 / 259 | 1+1 | 3 | 15/15 |
| zip | light | exec | 15 | 2099 / 7031 | 261 / 262 | 8 / 8 | 177 / 177 | 1+1 | 3 | 15/15 |
| zip | heavy | fork | 15 | 2248 / 2281 | 462 / 463 | 209 / 209 | 659 / 659 | 1+1 | 3 | 15/15 |
| zip | heavy | exec | 15 | 4021 / 5501 | 461 / 462 | 208 / 208 | 377 / 377 | 1+1 | 3 | 15/15 |
| concat | light | fork | 20 | 137 / 165 | 263 / 263 | 9 / 9 | 259 / 259 | 1+1 | 3 | 20/20 |
| concat | light | exec | 20 | 1957 / 2985 | 261 / 262 | 8 / 8 | 177 / 177 | 1+1 | 3 | 20/20 |
| concat | heavy | fork | 20 | 2262 / 2279 | 462 / 463 | 209 / 209 | 659 / 659 | 1+1 | 3 | 20/20 |
| concat | heavy | exec | 20 | 4032 / 5245 | 461 / 462 | 208 / 208 | 377 / 377 | 1+1 | 3 | 20/20 |
| mapped_group | light | fork | 30 | 153 / 5147 | 262 / 262 | 9 / 9 | 258 / 259 | 1+1 | 2 | 15/30 |
| mapped_group | light | exec | 30 | 1943 / 2449 | 261 / 261 | 7 / 8 | 177 / 177 | 1+1 | 2 | 15/30 |
| mapped_group | heavy | fork | 30 | 2253 / 2288 | 462 / 462 | 209 / 209 | 658 / 659 | 1+1 | 2 | 15/30 |
| mapped_group | heavy | exec | 30 | 3952 / 4189 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/30 |
| reduce | light | fork | 15 | 128 / 3143 | 262 / 262 | 9 / 9 | 259 / 259 | 1+1 | 2 | 15/15 |
| reduce | light | exec | 15 | 1830 / 4961 | 261 / 261 | 8 / 8 | 177 / 177 | 1+1 | 2 | 15/15 |
| reduce | heavy | fork | 15 | 2256 / 2268 | 462 / 462 | 209 / 209 | 659 / 659 | 1+1 | 2 | 15/15 |
| reduce | heavy | exec | 15 | 3941 / 4168 | 461 / 461 | 208 / 208 | 377 / 377 | 1+1 | 2 | 15/15 |

## Unmapped task instances (producers and reduce)

| shape | weight | mode | n | wall ms (med / p90) | supervisor peak MB (med / p90) | supervisor growth MB (med / p90) | task peak MB (med / p90) | imports supervisor+task (med) | XCom API reads (med) | handover used |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 5 | 120 / 123 | 258 / 259 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| expand | light | exec | 5 | 1842 / 1966 | 256 / 256 | 4 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| expand | heavy | fork | 5 | 1193 / 1196 | 258 / 258 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| expand | heavy | exec | 5 | 2980 / 3012 | 256 / 256 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| partial_expand | light | fork | 5 | 126 / 130 | 258 / 258 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| partial_expand | light | exec | 5 | 1892 / 5060 | 256 / 256 | 5 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| partial_expand | heavy | fork | 5 | 1185 / 1189 | 258 / 258 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| partial_expand | heavy | exec | 5 | 2983 / 20692 | 256 / 256 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| expand_kwargs | light | fork | 5 | 134 / 167 | 258 / 259 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| expand_kwargs | light | exec | 5 | 1907 / 2541 | 256 / 256 | 4 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| expand_kwargs | heavy | fork | 5 | 1202 / 1650 | 258 / 259 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| expand_kwargs | heavy | exec | 5 | 3018 / 3072 | 256 / 256 | 4 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| map | light | fork | 5 | 132 / 142 | 258 / 258 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| map | light | exec | 5 | 1995 / 2037 | 256 / 256 | 4 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| map | heavy | fork | 5 | 1176 / 1177 | 258 / 259 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| map | heavy | exec | 5 | 2911 / 3068 | 256 / 256 | 5 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| zip | light | fork | 10 | 126 / 168 | 259 / 259 | 6 / 7 | 255 / 255 | 0+1 | 1 | 0/10 |
| zip | light | exec | 10 | 1964 / 4224 | 256 / 256 | 4 / 4 | 177 / 177 | 0+1 | 1 | 0/10 |
| zip | heavy | fork | 10 | 1171 / 1199 | 259 / 259 | 6 / 7 | 455 / 455 | 0+1 | 1 | 0/10 |
| zip | heavy | exec | 10 | 2987 / 3866 | 256 / 256 | 4 / 4 | 377 / 377 | 0+1 | 1 | 0/10 |
| concat | light | fork | 10 | 118 / 173 | 259 / 259 | 6 / 7 | 255 / 255 | 0+1 | 1 | 0/10 |
| concat | light | exec | 10 | 2020 / 6989 | 256 / 256 | 4 / 4 | 177 / 177 | 0+1 | 1 | 0/10 |
| concat | heavy | fork | 10 | 1191 / 1206 | 259 / 259 | 6 / 7 | 455 / 455 | 0+1 | 1 | 0/10 |
| concat | heavy | exec | 10 | 2950 / 3086 | 256 / 256 | 4 / 4 | 377 / 377 | 0+1 | 1 | 0/10 |
| mapped_group | light | fork | 5 | 133 / 168 | 259 / 259 | 7 / 7 | 255 / 255 | 0+1 | 1 | 0/5 |
| mapped_group | light | exec | 5 | 2047 / 4936 | 257 / 257 | 4 / 4 | 177 / 177 | 0+1 | 1 | 0/5 |
| mapped_group | heavy | fork | 5 | 1190 / 1192 | 259 / 259 | 7 / 7 | 455 / 455 | 0+1 | 1 | 0/5 |
| mapped_group | heavy | exec | 5 | 2821 / 2978 | 257 / 257 | 4 / 4 | 377 / 377 | 0+1 | 1 | 0/5 |
| reduce | light | fork | 10 | 139 / 151 | 260 / 261 | 7 / 7 | 255 / 256 | 0+1 | 3.5 | 0/10 |
| reduce | light | exec | 10 | 2013 / 6695 | 257 / 259 | 5 / 6 | 177 / 177 | 0+1 | 3.5 | 0/10 |
| reduce | heavy | fork | 10 | 1185 / 1224 | 260 / 261 | 7 / 8 | 455 / 456 | 0+1 | 3.5 | 0/10 |
| reduce | heavy | exec | 10 | 2876 / 3035 | 257 / 259 | 5 / 5 | 377 / 377 | 0+1 | 3.5 | 0/10 |
