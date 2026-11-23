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

Package `org.apache.airflow.sdk.provider` defines provider operators in Java with `@ProviderOperator` and
`@Param`, and adds them to a Dag with `OperatorTask.useOperator`. A task runs the same operator in one of two
ways, picked by the Java coordinator:

- A task of a Java Dag runs in the Dag's own JVM, which has the provider's JAR on its classpath.
- A task of a Dag in another language, such as Python, runs in the generic `org.apache.airflow.sdk.ProviderRunner`,
  which loads the provider's JARs in a class loader of their own.

## Layout of `java_providers_path`

```
<java_providers_path>/
├── sdk/
│   └── airflow-sdk-1.0.0.jar
└── java-localfile/
    └── airflow-provider-java-localfile-1.0.0.jar
```

A task's `_operator_module` is `<provider directory>/<class name>`, such as
`java-localfile/org.apache.airflow.providers.localfile.CreateFileOperator`. Each provider directory gets one
class loader whose parent has the SDK, so providers share the SDK's classes and keep their other dependencies
apart.

## Python Dag using a Java provider

```mermaid
sequenceDiagram
    participant DP as Dag processor
    participant S as Scheduler
    participant W as Worker supervisor
    participant C as Java coordinator
    participant R as ProviderRunner JVM
    participant L as Provider class loader
    participant API as API server

    DP->>S: Store the provider task with stub_kind provider_operator
    S->>W: Queue the task instance
    W->>API: Start the task with ti_run
    API-->>W: Provider operator details
    W->>C: Pick the Java coordinator by queue
    Note over C: Python Dag file and a provider_operator task
    Note over C: Read the supervisor schema version from the SDK JAR manifest
    C-->>W: Java command with the SDK JARs and the providers path
    W->>R: Start the JVM and send the startup details
    R->>L: Create the class loader of the provider directory
    L-->>R: Operator class
    Note over R: Check the class implements the task classpath
    R->>W: Pull upstream results and render templates
    W-->>R: Final argument values
    Note over R: Create the operator and set its fields
    R->>R: Execute the operator in this JVM
    R->>W: Operator calls such as a Variable, an XCom or a log
    W->>API: Execution API requests
    R->>W: Push the return value XCom and succeed
    R-->>W: Exit
    W->>API: Report the task state
```

## Java Dag using a Java provider

```mermaid
sequenceDiagram
    participant DP as Dag processor
    participant S as Scheduler
    participant W as Worker supervisor
    participant C as Java coordinator
    participant B as Java Dag JVM
    participant API as API server

    DP->>B: Parse the Dag
    B-->>DP: Provider task with stub_kind provider_operator
    DP->>S: Store the serialized Dag
    S->>W: Queue the task instance
    W->>API: Start the task with ti_run
    API-->>W: Provider operator details
    W->>C: Pick the Java coordinator by queue
    Note over C: Java Dag file, so run the Dag bundle
    C-->>W: Java command with the Dag JARs
    W->>B: Start the JVM and send the startup details
    Note over B: Load the operator class from the Dag classpath
    B->>W: Pull upstream results and render templates
    W-->>B: Final argument values
    B->>B: Execute the operator in this JVM
    B->>W: Operator calls such as a Variable, an XCom or a log
    W->>API: Execution API requests
    B->>W: Push the return value XCom and succeed
    B-->>W: Exit
    W->>API: Report the task state
```
