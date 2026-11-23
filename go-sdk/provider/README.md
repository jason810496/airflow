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

# provider

Package `provider` defines provider operators in Go with `Define`, and serves them as a plugin with `Serve`.
A task runs the same operator in one of two ways, picked by the Go coordinator:

- A task of a Go Dag runs in the Dag's own executable, which links the provider package.
- A task of a Dag in another language, such as Python, runs in the generic `airflow-provider-runner`, which
  starts the provider's plugin executable with [go-plugin](https://github.com/hashicorp/go-plugin).

## Layout of `go_providers_path`

```
<go_providers_path>/
├── airflow-provider-runner
└── plugins/
    └── airflow-provider-go-localfile
```

A task's `_operator_module` is the file name of its plugin in `plugins/`.

## Python Dag using a Go provider

```mermaid
sequenceDiagram
    autonumber
    participant DP as Dag processor
    participant S as Scheduler
    participant W as Worker supervisor
    participant C as Go coordinator
    participant R as airflow-provider-runner
    participant P as Plugin executable
    participant API as API server

    DP->>S: Serialized task: stub_kind=provider_operator, _operator_module, _operator_kwargs, queue
    S->>W: Queue the task instance
    W->>API: ti_run
    API-->>W: TIRunContext.provider_operator
    W->>C: queue_to_coordinator picks the Go coordinator
    Note over C: Not a native Go Dag file, and stub_kind is provider_operator
    C->>R: runner --airflow-metadata, for the schema version (cached)
    C-->>W: runner --plugins-dir=go_providers_path/plugins
    W->>R: Start with --comm and --logs, send StartupDetails
    Note over R: Operator not linked, so load plugins/_operator_module
    R->>P: Start with the handshake cookie
    P-->>R: Handshake with its address
    R->>P: Describe(classpath)
    P-->>R: Template fields and params
    R->>W: Pull upstream XComs, render template fields
    W-->>R: Final argument values
    R->>P: Execute(classpath, args, task instance, Dag run, callback stream)
    loop While the operator runs
        P->>R: Callback, such as GetVariable, XCom or Log
        R->>W: SDK client call
        W->>API: Execution API request
        API-->>W: Response
        W-->>R: Response
        R-->>P: Response
    end
    P-->>R: Result as JSON
    R->>W: Push return_value XCom, succeed the task
    R->>P: Kill
    R-->>W: Exit
    W->>API: Report the task state
```

## Go Dag using a Go provider

```mermaid
sequenceDiagram
    autonumber
    participant DP as Dag processor
    participant S as Scheduler
    participant W as Worker supervisor
    participant C as Go coordinator
    participant B as Go Dag executable
    participant API as API server

    DP->>B: Parse the Dag
    Note over B: Importing the provider package registers its operators with Define
    B-->>DP: Serialized task: stub_kind=provider_operator, _operator_module, _operator_kwargs, Dag queue
    DP->>S: Store the serialized Dag
    S->>W: Queue the task instance
    W->>API: ti_run
    API-->>W: TIRunContext.provider_operator
    W->>C: queue_to_coordinator picks the Go coordinator
    Note over C: Native Go Dag file, so stub_kind is not checked
    C-->>W: The Dag executable
    W->>B: Start with --comm and --logs, send StartupDetails
    Note over B: Operator linked, so it runs in this process
    B->>W: Pull upstream XComs, render template fields
    W-->>B: Final argument values
    B->>W: Calls of the operator, such as GetVariable, XCom or Log
    W->>API: Execution API requests
    B->>W: Push return_value XCom, succeed the task
    B-->>W: Exit
    W->>API: Report the task state
```
