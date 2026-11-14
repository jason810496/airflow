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

# @apache-airflow/providers-common-ai

The operators and sensors of `apache-airflow-providers-common-ai` 1.1.0, for a Dag written with
[`@apache-airflow/ts-sdk`](https://www.npmjs.com/package/@apache-airflow/ts-sdk).

A binding has no task body. It records the Python operator class and its arguments in the
Dag, and a Python worker that has the provider installed runs the operator. Such a task does
not take the Dag's queue: give it one in its task options when the default queue is not
served by Python workers.

```ts
import { Dag } from "@apache-airflow/ts-sdk";
import { AgentOperator } from "@apache-airflow/providers-common-ai/operators/agent";

const dag = new Dag("example");
dag.task("my_task", AgentOperator({ /* ... */ }))();
```

A module mirrors the Python module of the same path, and an argument is the Python
argument in camel case. A templated argument takes a Jinja template, or the result of
another task. A duration is a number of seconds.

This package is generated. To change it, regenerate the operator specs and the packages:

```bash
python3 scripts/ci/lang_sdk_providers/generate_operator_specs.py --allow-list common.ai
```

```bash
pnpm --dir ts-sdk run generate:providers
```

## Operators

| Operator | Module |
| --- | --- |
| `AgentOperator` | `@apache-airflow/providers-common-ai/operators/agent` |
| `DocumentLoaderOperator` | `@apache-airflow/providers-common-ai/operators/document_loader` |
| `LlamaIndexEmbeddingOperator` | `@apache-airflow/providers-common-ai/operators/llamaindex_embedding` |
| `LlamaIndexRetrievalOperator` | `@apache-airflow/providers-common-ai/operators/llamaindex_retrieval` |
| `LLMOperator` | `@apache-airflow/providers-common-ai/operators/llm` |
| `LLMBatchOperator` | `@apache-airflow/providers-common-ai/operators/llm_batch` |
| `LLMBranchOperator` | `@apache-airflow/providers-common-ai/operators/llm_branch` |
| `LLMFileAnalysisOperator` | `@apache-airflow/providers-common-ai/operators/llm_file_analysis` |
| `LLMSchemaCompareOperator` | `@apache-airflow/providers-common-ai/operators/llm_schema_compare` |
| `LLMSQLQueryOperator` | `@apache-airflow/providers-common-ai/operators/llm_sql` |
