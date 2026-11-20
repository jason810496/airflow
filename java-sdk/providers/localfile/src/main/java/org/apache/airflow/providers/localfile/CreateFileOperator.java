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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.airflow.sdk.provider.Operator;
import org.apache.airflow.sdk.provider.OperatorContext;
import org.apache.airflow.sdk.provider.Param;
import org.apache.airflow.sdk.provider.ProviderOperator;

/** Creates an empty file. An existing file is left as it is. */
@ProviderOperator(
    module = "airflow.providers.java.localfile.operators.file",
    description = "Create an empty file.")
public class CreateFileOperator implements Operator<CreateFileOperator.Result> {

  @Param(
      required = true,
      templated = true,
      description = "The file to create. Missing parent directories are created too.")
  public String path;

  @Param(
      description =
          "Succeed when the file exists already. When false, an existing file fails the task.")
  public boolean existOk = true;

  /** What the operator returns. */
  public static final class Result {
    public final String path;

    /** False when the file existed already. */
    public final boolean created;

    Result(String path, boolean created) {
      this.path = path;
      this.created = created;
    }
  }

  @Override
  public Result execute(OperatorContext context) throws IOException {
    Path file = Path.of(path);
    Files.createDirectories(file.toAbsolutePath().getParent());
    try {
      Files.createFile(file);
      return new Result(path, true);
    } catch (FileAlreadyExistsException e) {
      if (existOk) {
        return new Result(path, false);
      }
      throw new IOException(path + " exists already", e);
    }
  }
}
