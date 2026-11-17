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

# ADR-0018: Provider Operators Implemented in a Lang SDK

## Status

Proposed.

## Decision

1. **A provider operator may be implemented in a Lang SDK.** Its identity is a Python class path, such as
   `airflow.providers.localfile.operators.file.WriteFileOperator`, in every SDK.
2. **The task's queue picks the runtime.** `[sdk] queue_to_coordinator` routes the task to a coordinator. A task
   left on a queue that a Python worker serves fails and names the queue setting.
3. **The Python package of such a provider is a generated wrapper.** Its classes subclass
   `LangSdkProviderOperator`, which is a stub. The wrapper also names the module that implements the operator.
   Providers implemented in Python are unchanged.
4. **The task carries its operator in the run context.** The serialized task has `is_stub`, `_operator_kwargs`
   and `_operator_module`. When the task starts, the Execution API fills `TIRunContext.provider_operator`
   with the class path, the module, the arguments and the Dag's edges. The arguments are the argument
   bindings of [ADR-0007](0007-taskflow-across-language-boundary.md), the same shape that
   [ADR-0009](0009-provider-operators-as-generated-dsl.md) uses. No request is needed to read them.
5. **Templates render in Python.** The runtime sends the literals of its template fields to the supervisor in
   `RenderTemplateFields`. The helper process that runs the XCom backend renders them against the task's
   context, and the supervisor answers with `RenderedTemplateFieldsResult`. Both messages are internal to the
   runtime protocol. Results of other tasks are `xcom` bindings, which the runtime pulls and Python does not
   render.
6. **A runtime finds the implementation in one of two ways.** When its bundle declares the task, the
   implementation comes from the bundle. Otherwise the runtime imports `provider_operator.module` and takes
   the export named after the class. Both ways share one task path for failure, retry and the
   `return_value` XCom.
7. **The spec comes from the implementation.** The implementing SDK writes
   `providers/<provider>/_generated/lang_sdk_operators.json` with `implemented_in` naming its runtime and an
   `implementation_module` for each operator. Every other SDK generates its wrapper from that file.

## Context

[ADR-0009](0009-provider-operators-as-generated-dsl.md) lets a Dag written in any Lang SDK use a provider
implemented in Python. Some integrations belong in another ecosystem, where the library they need only exists
in that language. This ADR covers that direction: a provider implemented once, in one Lang SDK, and usable from
a Dag written in Python or in any other Lang SDK.

The run context already carries a provider operator for a Python worker that cannot import a Lang SDK Dag file.
It also carries the argument bindings of a stub task. What was missing is a way for a runtime to run an
operator that its bundle did not declare, and a way to render the operator's templates outside Python.

## Example

The `localfile` provider is implemented in TypeScript:

```ts
export const WriteFileOperator = defineProviderOperator<WriteFileOperatorArgs, WriteFileResult>({
  module: "airflow.providers.localfile.operators.file",
  className: "WriteFileOperator",
  params: { path: { name: "path", required: true }, content: { name: "content", required: true } },
  templateFields: ["path", "content"],
  async execute({ path, content }) {
    await writeFile(path, content);
    return { path, bytes: Buffer.byteLength(content) };
  },
});
```

A Python Dag uses the generated wrapper and routes the task to the TypeScript runtime with its queue:

```python
from airflow.providers.localfile.operators.file import WriteFileOperator

with DAG("write_report"):
    WriteFileOperator(
        task_id="write", path="/tmp/{{ run_id }}.txt", content=build_report(), queue="typescript"
    )
```

A TypeScript Dag uses the same operator directly, and a Go or Java Dag would use a binding generated from the
spec.

## Serialized task

The wrapper and a TypeScript Dag write the same task:

```json
{
  "task_type": "WriteFileOperator",
  "_task_module": "airflow.providers.localfile.operators.file",
  "_operator_module": "@apache-airflow/providers-localfile/operators/file",
  "is_stub": true,
  "_operator_kwargs": [
    {"name": "path", "kind": "literal", "value": "/tmp/{{ run_id }}.txt"},
    {"name": "content", "kind": "xcom", "task_id": "build_report"}
  ]
}
```

- A literal is plain JSON. A datetime is an ISO 8601 string with `value_schema` `{"type": "string", "format":
  "date-time"}`, and a timedelta is a number of seconds with `{"type": "number", "format": "duration-seconds"}`.
- A task's result is an `xcom` binding and an upstream edge. It must be the whole argument. A result inside a
  list or a dict fails when the Dag is serialized.
- An argument left unset is left out, so the implementation applies its own default.
- `is_stub` makes the API server fill `TIRunContext.provider_operator` when the task starts.
  `_operator_module` is written when the binding or the wrapper names the module.

## Task execution

```
Dag processor            the wrapper writes task_type, _task_module, is_stub, _operator_kwargs
                         and _operator_module
scheduler -> executor    the task goes to a worker serving queue "typescript"
supervisor               queue_to_coordinator picks the Node coordinator
  coordinator            a bundle declares the dag_id: run that bundle
                         no bundle does, and node_modules_path is set: run the provider runner
  runtime                the bundle declares the task: use its implementation
                         it does not: import provider_operator.module, take the export named
                         after the class, and check that it implements the class path
                         then send RenderTemplateFields, pull the xcom bindings and run it
```

A TypeScript Dag's own task takes the first branch: the bundle declares the Dag, and the Dag's record holds the
implementation.

The runtime knows a task is a provider operator from `ti_context.provider_operator` alone. It maps the Python
argument names to the implementation's keys, reads a `date-time` literal as a `Date`, and leaves a
`duration-seconds` literal as a number of seconds.

The helper process builds the template context from the task's start-up details and a placeholder task, then
renders each value with the Task SDK's `render_template`. The supervisor starts one helper process per task, on
the first XCom backend call or render. The supervisor stores the rendered values as the task's rendered fields,
so the UI shows them as it does for a Python task.

## Provider runner

The `@apache-airflow/ts-sdk` package ships `airflow-ts-provider-runner`, which serves an empty bundle, so every
task it runs takes the import branch. `NodeCoordinator` gets the `node_modules` directory it runs from:

```json
{
  "ts": {
    "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
    "kwargs": {"node_modules_path": "/opt/airflow/ts/node_modules"}
  }
}
```

The directory holds `@apache-airflow/ts-sdk` and the provider packages, for example from
`npm install --prefix /opt/airflow/ts @apache-airflow/ts-sdk @apache-airflow/providers-localfile`. A provider
package installed beside the SDK resolves from the runner and shares its copy of the SDK. The coordinator reads
the runner's supervisor schema version from `airflow.supervisorSchemaVersion` in the SDK's `package.json`.
Without `node_modules_path`, a task that no bundle declares fails as it did before.

## Consequences

- **One serialized shape for every authoring language.** A Python, TypeScript, Go or Java Dag writes the same
  task, so any runtime that implements the class path can run it.
- **Authors set the queue.** A task left on a queue that a Python worker serves fails, with a message naming the
  runtimes that implement the operator.
- **Templates render with less context than in Python.** Dag params, `template_ext` files and user-defined macros
  do not reach the helper, since the Execution API sends neither. The run's details, `var`, `conn`, `xcom_pull`
  and macro plugins work.
- **A result of another task is pulled, not rendered.** It is not in the task's rendered fields. A result inside
  a template string works through `xcom_pull`, as in Python.
- **A provider task cannot be mapped**, as in ADR-0009.
- **Known limitation.** A Python Dag whose `dag_id` a bundle declares, for example because it also has
  `@task.stub` handlers, runs in that bundle and not in the provider runner. A TypeScript-implemented operator
  in such a Dag only runs when the bundle resolves the operator's package and shares its copy of the SDK.
  The deployment choice is to keep such operators in Dags that no bundle declares.

## Go and Java

They follow the same rules, keyed by the Python class path:

- **Go** cannot load code at run time, so a provider host executable imports the provider modules, which register
  their implementations in `init()`. Arguments decode into a struct with `json:"path"` tags.
- **Java** finds implementations on the classpath through `ServiceLoader`. Arguments decode into a record.

Each SDK needs one generator that reads any spec, so supporting every SDK takes four generators, not one per pair.

## Appendix: Implementation Notes

The TypeScript SDK implements this ADR, with `apache-airflow-providers-localfile` as the first provider.

- **TypeScript.** `defineProviderOperator` takes an optional `execute`. With it, the binding is also the
  implementation. A provider package exports each binding under its class name from the module the spec names.
- **Spec.** `ts-sdk/scripts/generate-operator-spec.mjs` reads the bindings that have an `execute` and the
  `<ClassName>Args` interface of each, and writes the provider's spec. `implementation_module` is the package
  name and the path of the source file under `src/`.
- **Python.** `scripts/ci/lang_sdk_providers/generate_python_wrappers.py` writes the wrapper from the spec. A unit
  test fails when the committed wrapper is not what the spec generates.
- **localfile.** `CreateFileOperator`, `WriteFileOperator`, `HasFileOperator` and `DeleteFileOperator`.
  `HasFileOperator` with `skip_downstream_if_missing` skips its direct downstream tasks when the file is missing,
  which uses the edges in `provider_operator.downstream_task_ids`.
