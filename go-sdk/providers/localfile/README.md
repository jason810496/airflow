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

# go.localfile

The operators of `apache-airflow-providers-go-localfile`, implemented in Go: `CreateFileOperator`,
`WriteFileOperator`, `HasFileOperator`, `DeleteFileOperator` and `WriteVariableOperator`. They touch only
local files, so an end-to-end test can run a whole scenario and check the result without an external
service. `WriteVariableOperator` reads an Airflow Variable through the SDK client, so it shows that an
operator can call back to Airflow.

A Go Dag that links the package runs the operators from its own executable:

```go
import "github.com/apache/airflow/go-sdk/providers/localfile"

write := dag.Task(
    localfile.WriteFileOperator(localfile.WriteFileOperatorArgs{
        Path:    airflow.Of("/tmp/{{ run_id }}/report.txt"),
        Content: airflow.ResultOf[string](content),
    }),
    airflow.TaskSpec{TaskID: "write"},
)
```

A Python Dag, or a Dag in another language SDK, uses the Python wrapper
`airflow.providers.go.localfile.operators.file` and routes the task to the Go coordinator with its queue. The
coordinator then runs `airflow-provider-runner`, which starts the plugin executable of this provider. Install
both into the directory that the coordinator's `go_providers_path` names:

```bash
GOBIN=<go_providers_path> go install \
    github.com/apache/airflow/go-sdk/cmd/airflow-provider-runner
GOBIN=<go_providers_path>/plugins go install \
    github.com/apache/airflow/go-sdk/providers/localfile/cmd/airflow-provider-go-localfile
```

The runner loads plugins with [hashicorp/go-plugin](https://github.com/hashicorp/go-plugin), which is
MPL-2.0. That license needs a review before a release.

`go-sdk/example/localfile` is a Dag that uses the provider, and `go-sdk/dags/go_localfile_example.py` is the
Python Dag. The spec of the provider is generated with `just generate-operator-spec`, and the Python
wrapper package in `providers/go/localfile` is generated from it.
