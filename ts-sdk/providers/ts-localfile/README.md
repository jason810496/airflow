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

# @apache-airflow/providers-ts-localfile

The operators of `apache-airflow-providers-ts-localfile` 1.0.0, implemented in TypeScript. They work on files of the
machine that runs the task.

A task of these operators runs in the Node runtime, whichever language its Dag is written in. A TypeScript Dag uses
them directly, and its tasks run from its bundle:

```ts
import { Bundle, Dag } from "@apache-airflow/ts-sdk";
import {
  HasFileOperator,
  WriteFileOperator,
} from "@apache-airflow/providers-ts-localfile/operators/file";

const dag = new Dag("report", { queue: "typescript" });
const write = dag.task(
  "write",
  WriteFileOperator({ path: "/tmp/{{ run_id }}.txt", content: "done" }),
)();
dag.task("check", HasFileOperator({ path: "/tmp/{{ run_id }}.txt" }))().after(write);

await new Bundle(dag).serve();
```

A Python Dag uses the generated wrapper of `apache-airflow-providers-ts-localfile` and sets the task's `queue` to one that
`[sdk] queue_to_coordinator` routes to the Node coordinator. Install this package and `@apache-airflow/ts-sdk` into one
`node_modules` directory on the workers, and give it to the coordinator as `node_modules_path`:

```bash
npm install --prefix /opt/airflow/ts @apache-airflow/ts-sdk @apache-airflow/providers-ts-localfile
```

A module mirrors the Python module of the same path, and an argument is the Python argument in camel case. A
templated argument takes a Jinja template, or the result of another task.

## Operators

| Operator | Module | Result |
| --- | --- | --- |
| `CreateFileOperator` | `@apache-airflow/providers-ts-localfile/operators/file` | `{ path, created }` |
| `WriteFileOperator` | `@apache-airflow/providers-ts-localfile/operators/file` | `{ path, bytes, sha256 }` |
| `HasFileOperator` | `@apache-airflow/providers-ts-localfile/operators/file` | `true` when the file exists |
| `DeleteFileOperator` | `@apache-airflow/providers-ts-localfile/operators/file` | `{ path, deleted }` |

## Spec

The spec that the Python wrapper and the other SDKs read is generated from the sources:

```bash
pnpm --dir ts-sdk run generate:operator-spec
```
