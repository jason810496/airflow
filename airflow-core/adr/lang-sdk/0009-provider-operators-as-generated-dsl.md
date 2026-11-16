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

# ADR-0009: Provider Operators as Generated Lang-SDK DSL

## Status

Proposed.

## Decision

1. **Provider operators reach every Lang SDK as generated, serialization-only bindings.**
   Only a task wrapping a host-language function executes in that language; a generated operator carries no body.
   Both go through the SDK's ordinary task registration.
2. **A provider DSL task runs on a Python worker**, so it must not inherit the SDK's queue, and the deployment must have that provider installed.
3. **Generate from the Python constructors, commit the output, and guard it with a prek hook.**
4. **Generate an operator only when every required constructor parameter is JSON-serializable**
   (primitive, list, dict, or a nested spec of those); omit optional parameters that are not;
   skip entirely any operator requiring a callable or a live object.
5. **The namespace mirrors `providers/`, adapted to each language's naming rules.**
6. **Templated fields pass through untouched.** The SDK writes the Jinja string; rendering stays server-side, where it already happens.
7. **Version skew warns at Dag parsing time and never blocks execution.** Warning on SDK provider DSL version and the server-side Python provider runtime version mismatch, shouldn't be a fatal error.
8. **Bindings ship as one package per provider from day one**, on that provider's release cadence, with an aggregate pin published alongside.

## Context

The design review on #72043 asked whether authoring a Dag in Go means giving up Python provider operators.

**It must not**, for any language: Airflow's ~100 provider distributions are what a native Lang-SDK Dag cannot afford to lose, and what no workflow engine outside Airflow's ecosystem can offer.
A native Dag serializes into the same Dag JSON a Python Dag produces, so any operator whose
constructor arguments are JSON-representable can be expressed from another language as a DSL that
emits serialization and nothing else.

## Example

The Go SDK spelling, mixing a native task with two generated operators:

```go
import (
    "github.com/apache/airflow/go-sdk/airflowprovider/amazon"
    "github.com/apache/airflow/go-sdk/airflowprovider/cncf/kubernetes"
)

extracted := dag.Task(extract) // native Go: runs on a Go worker

staged := dag.Task(amazon.S3ToRedshiftOperator{
    SchemaName: "public", TableName: "events", S3Bucket: "raw", S3Key: "events/{{ ds }}",
}, airflow.TaskSpec{TaskID: "stage"}).After(extracted) // DSL only: runs on a Python worker

dag.Task(kubernetes.KubernetesPodOperator{
    Namespace: "airflow", Image: "report:latest", Name: "report",
}, airflow.TaskSpec{TaskID: "report"}).After(staged)
```

`airflow.TriggerDagRun` ([ADR-0008](0008-control-flow-constructs.md)) follows the same concept but it is the hand-written.

## Consequences

- **Authors should pin the DSL to the provider version their deployment has installed.** The bindings
  describe one provider version's constructors, the operator that actually runs is whatever the
  Python worker imports, and skew between them only warns. Matching the two is what keeps that
  warning from becoming a surprise at run time.
- **Most skew is harmless, so it must not be fatal.** Python already fails loudly and precisely when
  a class or argument genuinely is not there, and a parse-time hard failure would take a whole Dag
  out over a version difference its tasks may not even touch. The hook also emits a coverage report,
  so which operators each SDK can reach is reviewable.

## Appendix: Implementation Notes

The TypeScript SDK implements decisions 1 to 6 and 8. Decision 7 and the Go and Java bindings are
not implemented yet.

- **One language-neutral spec per provider.** `scripts/ci/lang_sdk_providers/generate_operator_specs.py
  --allow-list <provider-id>,...` imports each operator and sensor module a provider's `provider.yaml`
  lists, reads the constructors across the class hierarchy down to `BaseOperator`, and writes
  `providers/<provider>/_generated/lang_sdk_operators.json`, which follows
  `airflow-core/src/airflow/lang_sdk_provider_operators.schema.json`. Each provider is read in its own `uv`
  environment. `BaseOperator`'s own arguments are left out: each SDK already has its task options
  for them. The skipped operators and omitted arguments are listed in the spec, which is the
  coverage report decision 3 asks for.
- **Each SDK generates its bindings from the specs.** `ts-sdk/scripts/generate-providers.mjs`
  writes one `@apache-airflow/providers-<provider-id>` package per spec, versioned as the provider.
  A TypeScript SDK unit test fails when the committed packages are not what the specs generate.
- **The serialized task is the Python operator.** A binding writes the class as `task_type` and
  `_task_module`, its arguments under `_operator_kwargs` in the type encoding of
  `BaseSerialization.serialize()`, and the class attributes the scheduler reads, such as
  `template_fields`, `_is_sensor` and `_can_skip_downstream`. It is not a stub, so it gets no
  argument bindings. A task's result given to a templated argument is written as an `xcomref` and
  drawn as an upstream edge.
- **The worker builds the operator from what the API server sends.** When a worker cannot import the
  Dag file, because a coordinator's importer claims it or no importer does, the task runner asks the
  Execution API for the task's operator (`GET /task-instances/{id}/provider-operator`). The API server
  reads it from the serialized Dag of the task instance's Dag version and returns only the class path,
  the `_operator_kwargs` and the Dag's edges. The worker builds the class
  with the arguments, inside a Dag whose other tasks are placeholders with the same ids and edges.
  That is what an operator reads of its Dag, such as `ApprovalOperator` choosing what to skip.
  When the task has no `_operator_kwargs`, the API server answers 404 and the task fails as before.
- **Limitations.** A placeholder Dag has no task groups, so a branch operator that names a group
  does not resolve it. A provider task cannot be mapped.
