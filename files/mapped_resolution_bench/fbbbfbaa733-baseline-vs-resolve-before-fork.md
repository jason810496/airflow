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

# Baseline vs resolve-before-fork (variant A)

Inputs: `fbbbfbaa733-baseline.json` and `fbbbfbaa733-resolve-before-fork.json`, 5 repetitions per cell, both
runs executed at the same time on one 10-CPU Linux VM that other jobs shared.

- `uv run --project airflow-core --no-sync python dev/mapped_resolution_bench/run.py --check <baseline.json> <A.json>`
  reports `OK: 800 task instances match`.
- Each cell shows median and p90 of the pooled task instances as `baseline -> A`. Wall time is the time inside
  `supervise_task()`, peak RSS is in MB, imports are `supervisor+task process` per task instance.
- Light fork cells: A's p90 holds a fixed ~5.1 s stall that baseline hits far less often (11 of 200 task
  instances against 1 of 200). It is a race of `WatchedSubprocess._monitor_subprocess`: when every socket has
  reported EOF before the exited child can be reaped, the loop waits in `select()` for
  `MIN_HEARTBEAT_INTERVAL` (5 s) with nothing left to wake it. Medians are not affected.
- Exec mode wall times are noisy (a fresh interpreter plus a re-import per task instance on a shared machine).

## Mapped task instances

| shape | weight | mode | n | wall ms med base -> A | wall ms p90 base -> A | sup peak MB med base -> A | task peak MB med base -> A | imports base -> A | XCom reads base -> A |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 15/15 | 161 -> 172 | 190 -> 203 | 260 -> 262 | 256 -> 259 | 0+1 -> 1+1 | 2 -> 2 |
| expand | light | exec | 15/15 | 3109 -> 3009 | 7307 -> 4347 | 258 -> 261 | 176 -> 177 | 0+1 -> 1+1 | 2 -> 2 |
| expand | heavy | fork | 15/15 | 1190 -> 2248 | 1217 -> 2408 | 260 -> 461 | 456 -> 658 | 0+1 -> 1+1 | 2 -> 2 |
| expand | heavy | exec | 15/15 | 3415 -> 4827 | 3878 -> 9196 | 258 -> 461 | 377 -> 377 | 0+1 -> 1+1 | 2 -> 2 |
| partial_expand | light | fork | 20/20 | 149 -> 181 | 191 -> 236 | 260 -> 262 | 256 -> 259 | 0+1 -> 1+1 | 2 -> 2 |
| partial_expand | light | exec | 20/20 | 2216 -> 2842 | 7310 -> 7150 | 258 -> 261 | 177 -> 177 | 0+1 -> 1+1 | 2 -> 2 |
| partial_expand | heavy | fork | 20/20 | 1199 -> 2236 | 1229 -> 2262 | 260 -> 461 | 456 -> 658 | 0+1 -> 1+1 | 2 -> 2 |
| partial_expand | heavy | exec | 20/20 | 3264 -> 4796 | 4059 -> 5660 | 258 -> 461 | 377 -> 375 | 0+1 -> 1+1 | 2 -> 2 |
| expand_kwargs | light | fork | 15/15 | 131 -> 146 | 195 -> 3164 | 260 -> 262 | 256 -> 259 | 0+1 -> 1+1 | 2 -> 2 |
| expand_kwargs | light | exec | 15/15 | 2012 -> 2597 | 5820 -> 7192 | 258 -> 261 | 177 -> 176 | 0+1 -> 1+1 | 2 -> 2 |
| expand_kwargs | heavy | fork | 15/15 | 1188 -> 2216 | 1243 -> 2261 | 260 -> 462 | 456 -> 659 | 0+1 -> 1+1 | 2 -> 2 |
| expand_kwargs | heavy | exec | 15/15 | 3339 -> 4734 | 3967 -> 9991 | 258 -> 461 | 377 -> 370 | 0+1 -> 1+1 | 2 -> 2 |
| map | light | fork | 15/15 | 137 -> 166 | 197 -> 3161 | 260 -> 262 | 256 -> 259 | 0+1 -> 1+1 | 2 -> 2 |
| map | light | exec | 15/15 | 2799 -> 2207 | 8676 -> 5579 | 258 -> 261 | 177 -> 177 | 0+1 -> 1+1 | 2 -> 2 |
| map | heavy | fork | 15/15 | 1197 -> 2235 | 1331 -> 2265 | 260 -> 461 | 456 -> 658 | 0+1 -> 1+1 | 2 -> 2 |
| map | heavy | exec | 15/15 | 3812 -> 4494 | 6848 -> 5277 | 258 -> 461 | 374 -> 377 | 0+1 -> 1+1 | 2 -> 2 |
| zip | light | fork | 15/15 | 148 -> 151 | 197 -> 193 | 260 -> 262 | 256 -> 259 | 0+1 -> 1+1 | 3 -> 3 |
| zip | light | exec | 15/15 | 2070 -> 2171 | 5623 -> 5950 | 258 -> 261 | 177 -> 177 | 0+1 -> 1+1 | 3 -> 3 |
| zip | heavy | fork | 15/15 | 1212 -> 2256 | 1241 -> 2288 | 260 -> 461 | 456 -> 659 | 0+1 -> 1+1 | 3 -> 3 |
| zip | heavy | exec | 15/15 | 3626 -> 4523 | 8110 -> 4993 | 258 -> 461 | 375 -> 377 | 0+1 -> 1+1 | 3 -> 3 |
| concat | light | fork | 20/20 | 148 -> 182 | 195 -> 5143 | 260 -> 262 | 256 -> 259 | 0+1 -> 1+1 | 3 -> 3 |
| concat | light | exec | 20/20 | 2047 -> 2570 | 6967 -> 3873 | 258 -> 261 | 177 -> 177 | 0+1 -> 1+1 | 3 -> 3 |
| concat | heavy | fork | 20/20 | 1205 -> 2242 | 1264 -> 2269 | 260 -> 462 | 456 -> 659 | 0+1 -> 1+1 | 3 -> 3 |
| concat | heavy | exec | 20/20 | 3514 -> 4635 | 4698 -> 7527 | 258 -> 461 | 376 -> 377 | 0+1 -> 1+1 | 3 -> 3 |
| mapped_group | light | fork | 30/30 | 150 -> 143 | 192 -> 193 | 260 -> 262 | 256 -> 258 | 0+1 -> 1+1 | 2 -> 2 |
| mapped_group | light | exec | 30/30 | 2575 -> 2254 | 7159 -> 7199 | 258 -> 261 | 177 -> 177 | 0+1 -> 1+1 | 2 -> 2 |
| mapped_group | heavy | fork | 30/30 | 1183 -> 2234 | 1233 -> 2270 | 260 -> 461 | 456 -> 658 | 0+1 -> 1+1 | 2 -> 2 |
| mapped_group | heavy | exec | 30/30 | 3454 -> 4318 | 4173 -> 9068 | 258 -> 461 | 377 -> 377 | 0+1 -> 1+1 | 2 -> 2 |
| reduce | light | fork | 15/15 | 171 -> 146 | 681 -> 3165 | 260 -> 262 | 256 -> 259 | 0+1 -> 1+1 | 2 -> 2 |
| reduce | light | exec | 15/15 | 2067 -> 2468 | 5596 -> 5814 | 258 -> 261 | 177 -> 177 | 0+1 -> 1+1 | 2 -> 2 |
| reduce | heavy | fork | 15/15 | 1199 -> 2228 | 1218 -> 2259 | 260 -> 461 | 456 -> 658 | 0+1 -> 1+1 | 2 -> 2 |
| reduce | heavy | exec | 15/15 | 3404 -> 4766 | 4551 -> 6780 | 258 -> 461 | 377 -> 377 | 0+1 -> 1+1 | 2 -> 2 |

## Unmapped task instances

| shape | weight | mode | n | wall ms med base -> A | wall ms p90 base -> A | sup peak MB med base -> A | task peak MB med base -> A | imports base -> A | XCom reads base -> A |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| expand | light | fork | 5/5 | 152 -> 133 | 184 -> 191 | 258 -> 258 | 254 -> 254 | 0+1 -> 0+1 | 1 -> 1 |
| expand | light | exec | 5/5 | 2109 -> 2093 | 2834 -> 2743 | 256 -> 256 | 177 -> 176 | 0+1 -> 0+1 | 1 -> 1 |
| expand | heavy | fork | 5/5 | 1214 -> 1173 | 1259 -> 1291 | 258 -> 258 | 454 -> 454 | 0+1 -> 0+1 | 1 -> 1 |
| expand | heavy | exec | 5/5 | 3106 -> 3689 | 6470 -> 3760 | 256 -> 256 | 377 -> 369 | 0+1 -> 0+1 | 1 -> 1 |
| partial_expand | light | fork | 5/5 | 138 -> 135 | 167 -> 140 | 258 -> 258 | 254 -> 254 | 0+1 -> 0+1 | 1 -> 1 |
| partial_expand | light | exec | 5/5 | 2050 -> 2007 | 2130 -> 2191 | 256 -> 256 | 177 -> 177 | 0+1 -> 0+1 | 1 -> 1 |
| partial_expand | heavy | fork | 5/5 | 1164 -> 1192 | 1186 -> 1232 | 258 -> 258 | 454 -> 454 | 0+1 -> 0+1 | 1 -> 1 |
| partial_expand | heavy | exec | 5/5 | 3107 -> 3681 | 3830 -> 4182 | 256 -> 256 | 377 -> 374 | 0+1 -> 0+1 | 1 -> 1 |
| expand_kwargs | light | fork | 5/5 | 123 -> 139 | 155 -> 248 | 258 -> 258 | 254 -> 254 | 0+1 -> 0+1 | 1 -> 1 |
| expand_kwargs | light | exec | 5/5 | 2250 -> 2398 | 5319 -> 2879 | 256 -> 256 | 177 -> 177 | 0+1 -> 0+1 | 1 -> 1 |
| expand_kwargs | heavy | fork | 5/5 | 1181 -> 1182 | 1281 -> 1209 | 258 -> 258 | 454 -> 454 | 0+1 -> 0+1 | 1 -> 1 |
| expand_kwargs | heavy | exec | 5/5 | 3834 -> 3529 | 5200 -> 3823 | 256 -> 256 | 368 -> 377 | 0+1 -> 0+1 | 1 -> 1 |
| map | light | fork | 5/5 | 159 -> 139 | 194 -> 162 | 258 -> 258 | 254 -> 254 | 0+1 -> 0+1 | 1 -> 1 |
| map | light | exec | 5/5 | 2522 -> 1986 | 4647 -> 3003 | 256 -> 256 | 177 -> 177 | 0+1 -> 0+1 | 1 -> 1 |
| map | heavy | fork | 5/5 | 1170 -> 1202 | 1211 -> 1219 | 258 -> 258 | 454 -> 454 | 0+1 -> 0+1 | 1 -> 1 |
| map | heavy | exec | 5/5 | 3343 -> 3050 | 3984 -> 3859 | 256 -> 256 | 377 -> 375 | 0+1 -> 0+1 | 1 -> 1 |
| zip | light | fork | 10/10 | 158 -> 128 | 178 -> 173 | 258 -> 258 | 254 -> 255 | 0+1 -> 0+1 | 1 -> 1 |
| zip | light | exec | 10/10 | 2093 -> 2082 | 2279 -> 2315 | 256 -> 256 | 177 -> 177 | 0+1 -> 0+1 | 1 -> 1 |
| zip | heavy | fork | 10/10 | 1180 -> 1168 | 1226 -> 1218 | 258 -> 258 | 454 -> 454 | 0+1 -> 0+1 | 1 -> 1 |
| zip | heavy | exec | 10/10 | 3379 -> 3055 | 4164 -> 4135 | 256 -> 256 | 376 -> 374 | 0+1 -> 0+1 | 1 -> 1 |
| concat | light | fork | 10/10 | 149 -> 132 | 185 -> 162 | 258 -> 259 | 254 -> 255 | 0+1 -> 0+1 | 1 -> 1 |
| concat | light | exec | 10/10 | 2117 -> 2046 | 2673 -> 3078 | 256 -> 256 | 177 -> 177 | 0+1 -> 0+1 | 1 -> 1 |
| concat | heavy | fork | 10/10 | 1188 -> 1200 | 1254 -> 1234 | 258 -> 258 | 454 -> 455 | 0+1 -> 0+1 | 1 -> 1 |
| concat | heavy | exec | 10/10 | 3069 -> 3023 | 4109 -> 4085 | 256 -> 256 | 377 -> 377 | 0+1 -> 0+1 | 1 -> 1 |
| mapped_group | light | fork | 5/5 | 163 -> 129 | 181 -> 157 | 259 -> 259 | 254 -> 255 | 0+1 -> 0+1 | 1 -> 1 |
| mapped_group | light | exec | 5/5 | 1938 -> 2021 | 2469 -> 2644 | 257 -> 256 | 176 -> 177 | 0+1 -> 0+1 | 1 -> 1 |
| mapped_group | heavy | fork | 5/5 | 1178 -> 1185 | 1194 -> 1193 | 258 -> 259 | 454 -> 455 | 0+1 -> 0+1 | 1 -> 1 |
| mapped_group | heavy | exec | 5/5 | 3053 -> 3044 | 3650 -> 3533 | 257 -> 257 | 368 -> 377 | 0+1 -> 0+1 | 1 -> 1 |
| reduce | light | fork | 10/10 | 170 -> 150 | 256 -> 184 | 259 -> 260 | 255 -> 255 | 0+1 -> 0+1 | 3.5 -> 3.5 |
| reduce | light | exec | 10/10 | 2057 -> 2041 | 2936 -> 2705 | 257 -> 257 | 177 -> 177 | 0+1 -> 0+1 | 3.5 -> 3.5 |
| reduce | heavy | fork | 10/10 | 1201 -> 1193 | 1236 -> 1229 | 259 -> 259 | 455 -> 455 | 0+1 -> 0+1 | 3.5 -> 3.5 |
| reduce | heavy | exec | 10/10 | 3106 -> 3252 | 3995 -> 4408 | 257 -> 258 | 377 -> 376 | 0+1 -> 0+1 | 3.5 -> 3.5 |
