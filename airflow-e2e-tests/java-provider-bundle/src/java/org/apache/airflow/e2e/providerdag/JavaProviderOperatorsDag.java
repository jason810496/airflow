/*
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

package org.apache.airflow.e2e.providerdag;

import java.util.LinkedHashMap;
import java.util.List;
import org.apache.airflow.providers.amazon.aws.operators.s3.S3CreateBucketOperator;
import org.apache.airflow.providers.amazon.aws.operators.s3.S3CreateObjectOperator;
import org.apache.airflow.providers.amazon.aws.sensors.s3.S3KeySensor;
import org.apache.airflow.providers.standard.operators.bash.BashOperator;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;
import org.apache.airflow.sdk.DagDef;
import org.apache.airflow.sdk.Task;

/**
 * The native Dag {@code java_provider_operators}, which mixes its own Java tasks with the generated
 * wrappers of Python providers:
 *
 * <pre>
 * build_report -&gt; create_bucket -&gt; write_report -&gt; wait_for_report -&gt; run_bash -&gt; publish
 * </pre>
 *
 * <p>{@code build_report} and {@code publish} run in Java. The amazon and standard operators carry
 * no body: their class and arguments go into the serialized Dag, and a Python worker builds and runs
 * them. The report a Java task builds is what the S3 operator writes, and the answer of the Bash
 * task is what the last Java task reads.
 */
public class JavaProviderOperatorsDag {
  /** The queue that {@code [sdk] queue_to_coordinator} routes to a Java coordinator. */
  public static final String QUEUE = "java-native";

  /** A queue that Python workers serve, for the provider tasks that a Python worker runs. */
  static final String PYTHON_QUEUE = "default";

  static final String BUCKET = "java-provider-operators";

  /** Rendered by the Python worker, as it renders a Python Dag's templates. */
  static final String REPORT_KEY = "runs/{{ run_id }}/report.json";

  static final String AWS_CONN_ID = "aws_localstack";

  public static final String REPORT = "{\"orders\":3,\"total\":120}";

  public static final String BASH_ANSWER = "bash says hello";

  public static class BuildReport implements Task {
    @Override
    public void execute(Context context, Client client) {
      client.setXCom(REPORT);
    }
  }

  /** Reads what the Python operators produced, and reports it. */
  public static class Publish implements Task {
    @Override
    public void execute(Context context, Client client) {
      var published = new LinkedHashMap<String, Object>();
      published.put("bash", client.getXCom("run_bash"));
      published.put("report_task", client.getXCom("build_report"));
      client.setXCom(published);
    }
  }

  public static DagDef build() {
    var dag =
        new DagDef("java_provider_operators")
            .config("description", "Native Java Dag that uses generated provider wrappers")
            .config("catchup", false)
            .config("tags", List.of("java-sdk", "providers", "aws"))
            .config("queue", QUEUE);

    var report = dag.task("build_report", BuildReport.class);

    // A provider task takes the Dag's queue like any other task, so each of these names a queue that
    // a Python worker serves.
    var bucket =
        dag.task(
                "create_bucket",
                S3CreateBucketOperator.builder().bucketName(BUCKET).awsConnId(AWS_CONN_ID).build())
            .config("queue", PYTHON_QUEUE);
    var written =
        dag.task(
                "write_report",
                S3CreateObjectOperator.builder()
                    .s3Bucket(BUCKET)
                    .s3Key(REPORT_KEY)
                    // A task's result, which the worker resolves from its XCom.
                    .data(report)
                    .replace(true)
                    .awsConnId(AWS_CONN_ID)
                    .build())
            .config("queue", PYTHON_QUEUE);
    var landed =
        dag.task(
                "wait_for_report",
                S3KeySensor.builder()
                    .bucketName(BUCKET)
                    .bucketKey(REPORT_KEY)
                    .awsConnId(AWS_CONN_ID)
                    .pokeInterval(5)
                    .timeout(120)
                    .build())
            .config("queue", PYTHON_QUEUE);
    var bash =
        dag.task("run_bash", BashOperator.builder().bashCommand("echo '" + BASH_ANSWER + "'").build())
            .config("queue", PYTHON_QUEUE);
    var publish = dag.task("publish", Publish.class);

    bucket.after(report);
    written.after(bucket);
    landed.after(written);
    bash.after(landed);
    publish.after(bash);
    return dag;
  }
}
