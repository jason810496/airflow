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

# Mapped resolution bench: seq-baseline

- commit `12ffdd92ee6` on `exp/python-full-supervisor-path`, Python 3.12.16, 10 CPUs, Linux-6.12.76-linuxkit-aarch64-with-glibc2.36
- env overrides: `{}`
- 5 repetitions per cell, 160 runs in 3707.2 s, started 2026-11-27T06:09:18+00:00
- command: `/Users/jason/airflow-workspace-worktrees/exp-python-full-supervisor-path/.venv/bin/python dev/mapped_resolution_bench/run.py --variant seq-baseline`
- Each cell pools the task instances of all repetitions. `wall` is the time inside `supervise_task()`.
  `supervisor peak` is the supervisor process's peak RSS, `supervisor growth` that minus its RSS at the start of the call. `task peak` is the task process's `VmHWM`, sampled every 10 ms.
  `imports` is how often the Dag file was imported for one task instance, in the supervisor process plus the task process. `XCom API reads` counts the requests for XCom values the supervisor made to the API, for itself and for the task process. `handover used` counts task processes that logged using values the supervisor resolved.

## Mapped task instances

| shape | weight | mode | n | wall ms (med / p90) | supervisor peak MB (med / p90) | supervisor growth MB (med / p90) | task peak MB (med / p90) | imports supervisor+task (med) | XCom API reads (med) | handover used |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 15 | 137 / 145 | 260 / 260 | 7 / 7 | 256 / 256 | 0+1 | 2 | 0/15 |
| expand | light | exec | 15 | 2065 / 5398 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 2 | 0/15 |
| expand | heavy | fork | 15 | 1197 / 1247 | 260 / 260 | 7 / 7 | 456 / 456 | 0+1 | 2 | 0/15 |
| expand | heavy | exec | 15 | 2915 / 3096 | 258 / 258 | 5 / 5 | 377 / 377 | 0+1 | 2 | 0/15 |
| partial_expand | light | fork | 20 | 127 / 149 | 260 / 260 | 7 / 7 | 256 / 256 | 0+1 | 2 | 0/20 |
| partial_expand | light | exec | 20 | 1812 / 2055 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 2 | 0/20 |
| partial_expand | heavy | fork | 20 | 1196 / 1210 | 260 / 260 | 7 / 7 | 456 / 456 | 0+1 | 2 | 0/20 |
| partial_expand | heavy | exec | 20 | 3035 / 3922 | 258 / 258 | 5 / 5 | 377 / 377 | 0+1 | 2 | 0/20 |
| expand_kwargs | light | fork | 15 | 134 / 145 | 260 / 260 | 7 / 7 | 256 / 256 | 0+1 | 2 | 0/15 |
| expand_kwargs | light | exec | 15 | 1850 / 2278 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 2 | 0/15 |
| expand_kwargs | heavy | fork | 15 | 1196 / 1224 | 260 / 260 | 7 / 7 | 456 / 456 | 0+1 | 2 | 0/15 |
| expand_kwargs | heavy | exec | 15 | 2978 / 10339 | 258 / 258 | 5 / 5 | 377 / 377 | 0+1 | 2 | 0/15 |
| map | light | fork | 15 | 129 / 628 | 260 / 260 | 7 / 7 | 256 / 256 | 0+1 | 2 | 0/15 |
| map | light | exec | 15 | 2037 / 7061 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 2 | 0/15 |
| map | heavy | fork | 15 | 1198 / 1238 | 260 / 260 | 7 / 7 | 456 / 456 | 0+1 | 2 | 0/15 |
| map | heavy | exec | 15 | 2916 / 3304 | 258 / 258 | 5 / 5 | 377 / 377 | 0+1 | 2 | 0/15 |
| zip | light | fork | 15 | 140 / 160 | 260 / 260 | 7 / 7 | 256 / 256 | 0+1 | 3 | 0/15 |
| zip | light | exec | 15 | 1969 / 5062 | 258 / 259 | 5 / 5 | 177 / 177 | 0+1 | 3 | 0/15 |
| zip | heavy | fork | 15 | 1203 / 1218 | 260 / 261 | 7 / 7 | 456 / 456 | 0+1 | 3 | 0/15 |
| zip | heavy | exec | 15 | 2906 / 3062 | 258 / 258 | 5 / 5 | 377 / 377 | 0+1 | 3 | 0/15 |
| concat | light | fork | 20 | 139 / 156 | 260 / 261 | 7 / 7 | 256 / 256 | 0+1 | 3 | 0/20 |
| concat | light | exec | 20 | 2032 / 6967 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 3 | 0/20 |
| concat | heavy | fork | 20 | 1213 / 1246 | 260 / 261 | 7 / 7 | 456 / 456 | 0+1 | 3 | 0/20 |
| concat | heavy | exec | 20 | 3078 / 4778 | 258 / 260 | 5 / 5 | 377 / 377 | 0+1 | 3 | 0/20 |
| mapped_group | light | fork | 30 | 149 / 216 | 260 / 262 | 7 / 7 | 256 / 258 | 0+1 | 2 | 0/30 |
| mapped_group | light | exec | 30 | 2036 / 7103 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 2 | 0/30 |
| mapped_group | heavy | fork | 30 | 1201 / 1233 | 260 / 262 | 7 / 7 | 456 / 457 | 0+1 | 2 | 0/30 |
| mapped_group | heavy | exec | 30 | 3004 / 3335 | 258 / 258 | 5 / 5 | 377 / 377 | 0+1 | 2 | 0/30 |
| reduce | light | fork | 15 | 135 / 170 | 260 / 260 | 7 / 7 | 256 / 256 | 0+1 | 2 | 0/15 |
| reduce | light | exec | 15 | 2012 / 6928 | 258 / 258 | 5 / 5 | 177 / 177 | 0+1 | 2 | 0/15 |
| reduce | heavy | fork | 15 | 1197 / 1207 | 260 / 261 | 7 / 7 | 456 / 456 | 0+1 | 2 | 0/15 |
| reduce | heavy | exec | 15 | 2926 / 3325 | 258 / 258 | 5 / 5 | 377 / 377 | 0+1 | 2 | 0/15 |

## Unmapped task instances (producers and reduce)

| shape | weight | mode | n | wall ms (med / p90) | supervisor peak MB (med / p90) | supervisor growth MB (med / p90) | task peak MB (med / p90) | imports supervisor+task (med) | XCom API reads (med) | handover used |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 5 | 127 / 180 | 259 / 259 | 7 / 7 | 254 / 255 | 0+1 | 1 | 0/5 |
| expand | light | exec | 5 | 1964 / 2050 | 256 / 256 | 4 / 4 | 177 / 177 | 0+1 | 1 | 0/5 |
| expand | heavy | fork | 5 | 1202 / 1262 | 258 / 259 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| expand | heavy | exec | 5 | 2818 / 4393 | 256 / 256 | 4 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| partial_expand | light | fork | 5 | 122 / 130 | 258 / 258 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| partial_expand | light | exec | 5 | 1829 / 2992 | 256 / 256 | 4 / 4 | 177 / 177 | 0+1 | 1 | 0/5 |
| partial_expand | heavy | fork | 5 | 1199 / 1204 | 258 / 258 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| partial_expand | heavy | exec | 5 | 2878 / 3443 | 256 / 256 | 4 / 4 | 377 / 377 | 0+1 | 1 | 0/5 |
| expand_kwargs | light | fork | 5 | 129 / 131 | 258 / 258 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| expand_kwargs | light | exec | 5 | 1850 / 8143 | 256 / 256 | 4 / 5 | 177 / 177 | 0+1 | 1 | 0/5 |
| expand_kwargs | heavy | fork | 5 | 1201 / 1214 | 258 / 258 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| expand_kwargs | heavy | exec | 5 | 2903 / 4201 | 256 / 256 | 4 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| map | light | fork | 5 | 126 / 670 | 258 / 258 | 7 / 7 | 254 / 254 | 0+1 | 1 | 0/5 |
| map | light | exec | 5 | 1913 / 5883 | 256 / 256 | 4 / 4 | 177 / 177 | 0+1 | 1 | 0/5 |
| map | heavy | fork | 5 | 1184 / 1236 | 258 / 258 | 7 / 7 | 454 / 454 | 0+1 | 1 | 0/5 |
| map | heavy | exec | 5 | 2986 / 6225 | 256 / 256 | 4 / 4 | 377 / 377 | 0+1 | 1 | 0/5 |
| zip | light | fork | 10 | 133 / 162 | 259 / 259 | 6 / 6 | 255 / 255 | 0+1 | 1 | 0/10 |
| zip | light | exec | 10 | 2016 / 2681 | 256 / 256 | 4 / 4 | 177 / 177 | 0+1 | 1 | 0/10 |
| zip | heavy | fork | 10 | 1188 / 1280 | 259 / 259 | 6 / 6 | 455 / 455 | 0+1 | 1 | 0/10 |
| zip | heavy | exec | 10 | 2911 / 3116 | 256 / 256 | 4 / 4 | 377 / 377 | 0+1 | 1 | 0/10 |
| concat | light | fork | 10 | 126 / 138 | 259 / 259 | 6 / 7 | 255 / 255 | 0+1 | 1 | 0/10 |
| concat | light | exec | 10 | 2026 / 2665 | 256 / 256 | 4 / 4 | 177 / 177 | 0+1 | 1 | 0/10 |
| concat | heavy | fork | 10 | 1179 / 1212 | 259 / 259 | 6 / 7 | 455 / 455 | 0+1 | 1 | 0/10 |
| concat | heavy | exec | 10 | 3043 / 5652 | 256 / 258 | 4 / 4 | 377 / 377 | 0+1 | 1 | 0/10 |
| mapped_group | light | fork | 5 | 133 / 172 | 259 / 260 | 7 / 7 | 255 / 256 | 0+1 | 1 | 0/5 |
| mapped_group | light | exec | 5 | 2563 / 3485 | 257 / 257 | 4 / 5 | 177 / 178 | 0+1 | 1 | 0/5 |
| mapped_group | heavy | fork | 5 | 1181 / 1207 | 259 / 260 | 7 / 7 | 455 / 456 | 0+1 | 1 | 0/5 |
| mapped_group | heavy | exec | 5 | 3024 / 3167 | 257 / 257 | 4 / 5 | 377 / 377 | 0+1 | 1 | 0/5 |
| reduce | light | fork | 10 | 150 / 188 | 260 / 261 | 7 / 8 | 255 / 256 | 0+1 | 3.5 | 0/10 |
| reduce | light | exec | 10 | 1899 / 2219 | 257 / 259 | 5 / 6 | 177 / 177 | 0+1 | 3.5 | 0/10 |
| reduce | heavy | fork | 10 | 1196 / 1218 | 260 / 261 | 7 / 7 | 455 / 456 | 0+1 | 3.5 | 0/10 |
| reduce | heavy | exec | 10 | 3051 / 4434 | 257 / 259 | 5 / 6 | 377 / 377 | 0+1 | 3.5 | 0/10 |
