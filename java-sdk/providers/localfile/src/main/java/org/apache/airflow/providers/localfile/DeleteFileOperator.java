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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.apache.airflow.sdk.provider.Operator;
import org.apache.airflow.sdk.provider.OperatorContext;
import org.apache.airflow.sdk.provider.Param;
import org.apache.airflow.sdk.provider.ProviderOperator;

/** Deletes a file. */
@ProviderOperator(
    module = "airflow.providers.java.localfile.operators.file",
    description = "Delete a file.")
public class DeleteFileOperator implements Operator<DeleteFileOperator.Result> {

  @Param(required = true, templated = true, description = "The file to delete.")
  public String path;

  @Param(
      description = "Succeed when the file is missing. When false, a missing file fails the task.")
  public boolean missingOk = true;

  /** What the operator returns. */
  public static final class Result {
    public final String path;

    /** False when the file was missing. */
    public final boolean deleted;

    Result(String path, boolean deleted) {
      this.path = path;
      this.deleted = deleted;
    }
  }

  @Override
  public Result execute(OperatorContext context) throws IOException {
    Path file = Path.of(path);
    if (Files.isDirectory(file)) {
      throw new IOException(path + " is a directory, not a file");
    }
    try {
      Files.delete(file);
      return new Result(path, true);
    } catch (NoSuchFileException e) {
      if (missingOk) {
        return new Result(path, false);
      }
      throw new IOException(path + " is missing", e);
    }
  }
}
