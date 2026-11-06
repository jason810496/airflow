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

# Plan: enforce the operator schema on serialized Dag tasks

Part of apache/airflow#71364 (point 5). It lands before the serialized Dag versioning framework (branch `feature/serialization/dag-format-versioning`), which leans on `schema.json` being right.

## Problem

`definitions.tasks` in `airflow-core/src/airflow/serialization/schema.json` checks each task only for `__type`, `__var` and `__var.task_id`. Nothing points task objects at `definitions.operator`, so the operator rules have never been enforced on write, and they have drifted from what the serializer emits since 3.1.

## How the drift was measured

- **Probe:** on upstream/main `51140809d2a`, point `tasks.items.properties.__var` at `{"$ref": "#/definitions/operator"}`.
- **Tests:** run `airflow-core/tests/unit/serialization` and `airflow-core/tests/unit/models/test_serialized_dag.py`.
- **Example Dags:** validate the 97 example Dags in `airflow-core/src/airflow/example_dags` with this script:

```python
import collections, json, sys
from jsonschema import Draft7Validator
from airflow.dag_processing.dagbag import DagBag
from airflow.serialization.serialized_objects import DagSerialization, OperatorSerialization

validator = Draft7Validator(json.load(open(sys.argv[1])))
bag = DagBag(dag_folder=sys.argv[2])
errors = collections.Counter()
for dag in bag.dags.values():
    OperatorSerialization.generate_client_defaults.cache_clear()
    data = {"__version": 3, "dag": DagSerialization.serialize_dag(dag)}
    if defaults := OperatorSerialization.generate_client_defaults():
        data["client_defaults"] = {"tasks": defaults}
    for e in validator.iter_errors(data):
        errors[(e.validator, e.message[:90])] += 1
for key, count in errors.most_common():
    print(count, key)
```

Every failure traced back to a stale schema rule. None came from a serializer bug.

| Stale rule | Hits in tests | Cause | Fix |
|---|---|---|---|
| `operator.required` lists `ui_color` and `ui_fgcolor` | 1034 | #55849 (3.1.0) omits keys equal to the schema default (`#fff`, `#000`) | Remove both from `required` |
| `operator.dependencies` makes `partial_kwargs` and `_is_mapped` require `expand_input` | 74 | TaskFlow mapped tasks store the expansion under `_expand_input_attr`, i.e. `op_kwargs_expand_input` (`serialize_mapped_operator`) | Accept either key, e.g. `anyOf` on `required: [expand_input]` / `required: [op_kwargs_expand_input]` |
| `_operator_extra_links` refs `definitions.extra_links`, an array of single-key objects | 36 | The serializer writes a `{name: "_link_<Class>"}` dict | Make it an object whose values are strings |
| A timedelta field declared as `number` receives `{"__type": "timedelta", "__var": 60.0}` | 4 | Not located yet | Find the field and align the schema with what the writer emits |

With the first fix applied, 59 tests still fail, all from the other three rows. On the example Dags only the `expand_input` rule fires (`mapped_task_loop`).

The test `airflow-core/tests/unit/serialization/test_dag_serialization.py::TestStringifiedDAGs::test_deserialization_across_process` hangs forever once anything fails. The child process raises, and the parent blocks on `queue.get()` (line 723).

## Cost

Validating a 2000-task Dag (alternating `EmptyOperator` / `BashOperator`, chained) takes 15 ms with today's schema and 73 ms with the strict one. A full `DagSerialization.to_dict` on that Dag takes 95 ms today. Validating strictly on every write would make large Dags about 60% slower to serialize on every parse.

## Decision

`schema.json` becomes strict. It is the vendored source for the Lang SDKs, so they validate against it too.

- **Python Dag processor:** `DagSerialization.to_dict` validates with a relaxed copy built in code, where task `__var` is not checked against `operator`. Parsing speed stays as it is.
- **Lang SDK Dags:** `DagSerialization.validate_serialized_dag` validates with the strict schema. It is the gate for TS/Go/Java Dags (`airflow-core/src/airflow/dag_processing/lang_sdk_processor.py:430-432`), so a Lang SDK Dag must match the operator rules.
- **Tests:** they validate the Python writer strictly, including a new test over all example Dags.

## Changes

- **`airflow-core/src/airflow/serialization/schema.json`:**
  - `definitions.tasks.items.properties.__var` becomes `{"$ref": "#/definitions/operator"}`, keeping `required: [__type, __var]` and `__type: const operator`.
  - Apply the four rule fixes from the table.
- **`airflow-core/src/airflow/serialization/json_schema.py`:**
  - Keep `load_dag_schema()` strict.
  - Add a relaxed loader that replaces `definitions.tasks.items.properties.__var` with `{"type": "object", "required": ["task_id"]}`.
- **`airflow-core/src/airflow/serialization/serialized_objects.py`:**
  - `BaseSerialization._json_schema` for `DagSerialization` uses the relaxed validator.
  - `validate_serialized_dag` uses the strict one.
- **Lang SDK copies:** regenerate with the existing hooks (`sync-java-sdk-dag-schema` and the TS/Go schema sync hooks in `.pre-commit-config.yaml`):
  - `ts-sdk/schema/dag-schema.json`, plus the generated `ts-sdk/src/generated/dag-schema-fields.ts` if it changes
  - `go-sdk/schema/dag-schema.json`, plus generated Go models if they change
  - `java-sdk/sdk/schema/dag-schema.json`
- **Tests:**
  - `test_deserialization_across_process`: use `queue.get(timeout=...)` so a child failure fails the test instead of hanging it.
  - New test: every example Dag validates against the strict schema.
  - New test: `validate_serialized_dag` rejects a task that breaks an operator rule.

## Risk

Lang SDK Dags now face the operator rules. Before merging, run each SDK's tests that validate against `dag-schema.json`, plus the Lang SDK serialization compare job (`scripts/ci/lang_sdk_serialization/compare.py`). The rules only got looser except for `__var` now being checked, so failures there mean a real SDK mismatch.

## Verification

```bash
uv run --project airflow-core pytest airflow-core/tests/unit/serialization airflow-core/tests/unit/models/test_serialized_dag.py airflow-core/tests/unit/dag_processing/test_lang_sdk_processor.py -q
(cd ts-sdk && pnpm test)
(cd go-sdk && go test ./...)
SKIP=check-supervisor-schemas-versions,regenerate-java-sdk-verification-metadata prek run --from-ref upstream/main
```

- **Example Dags:** run the script above against the new `schema.json`; it must report zero errors.
- **Java:** run Gradle tests only with `-Dmaven.repo.local` pointed at a scratch directory, never `~/.m2`.
- **Timing:** re-measure strict vs relaxed validation on the 2000-task Dag and put the numbers in the PR description.
