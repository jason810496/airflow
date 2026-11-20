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

package org.apache.airflow.providers.localfile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.airflow.sdk.provider.Operator;
import org.apache.airflow.sdk.provider.OperatorContext;
import org.apache.airflow.sdk.provider.Param;
import org.apache.airflow.sdk.provider.ProviderOperator;

/**
 * Writes the value of an Airflow Variable to a file. It reads the Variable through the SDK's client
 * while it runs, so it needs nothing but the task's own access to Airflow. The result is the one
 * of {@link WriteFileOperator}.
 */
@ProviderOperator(
    module = "airflow.providers.java.localfile.operators.file",
    description = "Write the value of an Airflow Variable to a file.")
public class WriteVariableOperator implements Operator<WriteFileOperator.Result> {

  @Param(
      required = true,
      templated = true,
      description = "The file to write. Missing parent directories are created.")
  public String path;

  @Param(
      required = true,
      templated = true,
      description = "The key of the Airflow Variable to read.")
  public String key;

  @Override
  public WriteFileOperator.Result execute(OperatorContext context) throws IOException {
    Object value = context.client.getVariable(key);
    if (value == null) {
      throw new IllegalStateException("The Airflow Variable '" + key + "' is not set");
    }
    return WriteFileOperator.write(path, value.toString(), StandardCharsets.UTF_8, false);
  }
}
