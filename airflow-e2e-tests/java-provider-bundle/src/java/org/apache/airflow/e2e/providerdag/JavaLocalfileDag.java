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

import java.util.List;
import org.apache.airflow.providers.java.localfile.operators.file.CreateFileOperator;
import org.apache.airflow.providers.java.localfile.operators.file.DeleteFileOperator;
import org.apache.airflow.providers.java.localfile.operators.file.HasFileOperator;
import org.apache.airflow.providers.java.localfile.operators.file.WriteFileOperator;
import org.apache.airflow.providers.java.localfile.operators.file.WriteVariableOperator;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;
import org.apache.airflow.sdk.DagDef;
import org.apache.airflow.sdk.Task;

/**
 * The native Dag {@code java_localfile}, which uses the operators of the Java-implemented
 * {@code java.localfile} provider and runs them from this JAR.
 *
 * <pre>
 * build_content -&gt; create -&gt; write -&gt; has_file -&gt; delete -&gt; has_after_delete -&gt; after_skip
 * write_variable
 * </pre>
 *
 * <p>{@code has_after_delete} finds the file gone and skips its direct downstream task, {@code
 * after_skip}. {@code write_variable} reads an Airflow Variable while it runs.
 */
public class JavaLocalfileDag {
  /** The queue that {@code [sdk] queue_to_coordinator} routes to a Java coordinator. */
  public static final String QUEUE = "java-native";

  /** Rendered by the Python supervisor, as it renders a Python Dag's templates. */
  static final String REPORT_PATH = "/tmp/airflow-localfile/java_localfile/{{ run_id }}/report.txt";

  static final String VARIABLE_PATH = "/tmp/airflow-localfile/java_localfile/{{ run_id }}/variable.txt";

  /** The Variable that {@code write_variable} reads; the test sets it first. */
  public static final String VARIABLE_KEY = "java_localfile_greeting";

  /** Has non-ASCII characters, so its size in bytes differs from its length. */
  static final String CONTENT = "localfile report\nnaïve café, 3 orders\n";

  public static class BuildContent implements Task {
    @Override
    public void execute(Context context, Client client) {
      client.setXCom(CONTENT);
    }
  }

  public static class AfterSkip implements Task {
    @Override
    public void execute(Context context, Client client) {
      client.setXCom("unreachable");
    }
  }

  public static DagDef build() {
    var dag =
        new DagDef("java_localfile")
            .config("description", "Native Java Dag that uses the java.localfile provider")
            .config("catchup", false)
            .config("tags", List.of("java-sdk", "providers", "java.localfile"))
            .config("queue", QUEUE);

    // Every task takes the Dag's queue, the provider tasks included.
    var buildContent = dag.task("build_content", BuildContent.class);
    var create = dag.task("create", CreateFileOperator.builder().path(REPORT_PATH).build());
    var write =
        dag.task(
            "write", WriteFileOperator.builder().path(REPORT_PATH).content(buildContent).build());
    var hasFile = dag.task("has_file", HasFileOperator.builder().path(REPORT_PATH).build());
    var delete = dag.task("delete", DeleteFileOperator.builder().path(REPORT_PATH).build());
    var hasAfterDelete =
        dag.task(
            "has_after_delete",
            HasFileOperator.builder().path(REPORT_PATH).skipDownstreamIfMissing(true).build());
    var afterSkip = dag.task("after_skip", AfterSkip.class);
    dag.task(
        "write_variable",
        WriteVariableOperator.builder().path(VARIABLE_PATH).key(VARIABLE_KEY).build());

    create.after(buildContent);
    write.after(create);
    hasFile.after(write);
    delete.after(hasFile);
    hasAfterDelete.after(delete);
    afterSkip.after(hasAfterDelete);
    return dag;
  }
}
