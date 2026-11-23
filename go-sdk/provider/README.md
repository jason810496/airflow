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
    participant DP as Dag processor
    participant S as Scheduler
    participant W as Worker supervisor
    participant C as Go coordinator
    participant R as Provider runner
    participant P as Provider plugin
    participant API as API server

    DP->>S: Store the provider task with stub_kind provider_operator
    S->>W: Queue the task instance
    W->>API: Start the task with ti_run
    API-->>W: Provider operator details
    W->>C: Pick the Go coordinator by queue
    Note over C: Python Dag file and a provider_operator task
    C->>R: Read the supervisor schema version
    C-->>W: Command of the provider runner
    W->>R: Start the runner and send the startup details
    Note over R: Operator not linked, so start its plugin
    R->>P: Start the plugin executable named by the task
    P-->>R: Handshake
    R->>P: Describe the operator
    P-->>R: Template fields and parameters
    R->>W: Pull upstream results and render templates
    W-->>R: Final argument values
    R->>P: Execute the operator with a callback stream
    P->>R: Callback such as a Variable, an XCom or a log
    R->>W: SDK client request
    W->>API: Execution API request
    API-->>W: Response
    W-->>R: Response
    R-->>P: Response
    P-->>R: Result as JSON
    R->>W: Push the return value XCom and succeed
    R->>P: Stop the plugin
    R-->>W: Exit
    W->>API: Report the task state
```

## Go Dag using a Go provider

```mermaid
sequenceDiagram
    participant DP as Dag processor
    participant S as Scheduler
    participant W as Worker supervisor
    participant C as Go coordinator
    participant B as Go Dag executable
    participant API as API server

    DP->>B: Parse the Dag
    Note over B: Importing the provider package registers its operators
    B-->>DP: Provider task with stub_kind provider_operator
    DP->>S: Store the serialized Dag
    S->>W: Queue the task instance
    W->>API: Start the task with ti_run
    API-->>W: Provider operator details
    W->>C: Pick the Go coordinator by queue
    Note over C: Go Dag file, so run the Dag executable
    C-->>W: Command of the Dag executable
    W->>B: Start the executable and send the startup details
    Note over B: Operator linked, so it runs in this process
    B->>W: Pull upstream results and render templates
    W-->>B: Final argument values
    B->>W: Operator calls such as a Variable, an XCom or a log
    W->>API: Execution API requests
    B->>W: Push the return value XCom and succeed
    B-->>W: Exit
    W->>API: Report the task state
```
