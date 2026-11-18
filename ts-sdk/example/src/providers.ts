/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// A native TypeScript Dag that mixes its own tasks with provider operators:
//
//     build_report -> create_bucket -> write_report -> wait_for_report -> summarize
//                                                                     -> review -> publish
//
// `build_report` and `publish` run in Node. The amazon, common.ai and standard operators carry no
// body: their class and arguments go into the serialized Dag, and a Python worker that has those
// providers installed builds and runs them. Values flow both ways: the report a TypeScript task
// builds is what the S3 operator writes and the model summarizes, and the summary and the
// reviewer's answer are what the last TypeScript task receives.
//
// Pack it into a bundle the Node coordinator parses:
//
//     airflow-ts-pack src/providers.ts --outfile dist/providers.min.mjs

import { Bundle, Dag } from "@apache-airflow/ts-sdk";
import {
  S3CreateBucketOperator,
  S3CreateObjectOperator,
} from "@apache-airflow/providers-amazon/aws/operators/s3";
import { S3KeySensor } from "@apache-airflow/providers-amazon/aws/sensors/s3";
import { LLMOperator } from "@apache-airflow/providers-common-ai/operators/llm";
import { ApprovalOperator } from "@apache-airflow/providers-standard/operators/hitl";

/** The queue the deployment's `[sdk] queue_to_coordinator` routes to the Node coordinator. */
const QUEUE = "typescript";
/** A queue that Python workers serve, for the provider tasks that a Python worker runs. */
const PYTHON_QUEUE = "default";
const BUCKET = "ts-provider-operators";
/** Rendered by the Python worker, as it renders a Python Dag's templates. */
const REPORT_KEY = "runs/{{ run_id }}/report.json";
const AWS_CONN_ID = "aws_localstack";
const LLM_CONN_ID = "e2e_llm";

/** What `ApprovalOperator` returns: `HITLTriggerEventSuccessPayload`, as Python names its fields. */
interface ApprovalResult {
  chosen_options: string[];
  params_input: Record<string, unknown>;
}

const dag = new Dag("ts_provider_operators", {
  queue: QUEUE,
  tags: ["typescript", "providers", "aws", "ai", "hitl"],
});

// 1. TypeScript builds the report.
const report = dag.task("build_report", async (): Promise<string> =>
  JSON.stringify({ orders: 3, total: 120 }),
)();

// 2. Python stores it in S3 and waits until it is there. A provider task takes the Dag's queue
// like any other task, so each of these names a queue that a Python worker serves.
const bucket = dag.task(
  "create_bucket",
  S3CreateBucketOperator({ bucketName: BUCKET, awsConnId: AWS_CONN_ID }),
  { queue: PYTHON_QUEUE },
)();
const written = dag.task(
  "write_report",
  S3CreateObjectOperator({
    s3Bucket: BUCKET,
    s3Key: REPORT_KEY,
    // A task's result, which the worker resolves when it renders the template.
    data: report,
    replace: true,
    awsConnId: AWS_CONN_ID,
  }),
  { queue: PYTHON_QUEUE },
)();
written.after(bucket);
const landed = dag.task(
  "wait_for_report",
  S3KeySensor({
    bucketName: BUCKET,
    bucketKey: REPORT_KEY,
    awsConnId: AWS_CONN_ID,
    pokeInterval: 5,
    timeout: 120,
  }),
  { queue: PYTHON_QUEUE },
)();
landed.after(written);

// 3. common.ai summarizes the report.
const summary = dag.task(
  "summarize",
  LLMOperator({
    prompt: report,
    systemPrompt: "Summarize the order report in one sentence.",
    llmConnId: LLM_CONN_ID,
    modelId: "test",
  }),
  { queue: PYTHON_QUEUE },
)();
summary.after(landed);

// 4. A person approves the summary. On "Reject", the operator skips `publish`.
const decision = dag.task(
  "review",
  ApprovalOperator({ subject: "Publish the report summary?", body: summary }),
  { queue: PYTHON_QUEUE },
)();

// 5. TypeScript publishes what was approved.
dag.task("publish", async ({ text, answer }: { text: string; answer: ApprovalResult }) => ({
  published: answer.chosen_options[0] === "Approve",
  summary: text,
}))({ text: summary, answer: decision });

await new Bundle(dag).serve();
