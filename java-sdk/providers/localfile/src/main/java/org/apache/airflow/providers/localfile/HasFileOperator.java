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
import java.nio.file.attribute.BasicFileAttributes;
import org.apache.airflow.sdk.provider.Operator;
import org.apache.airflow.sdk.provider.OperatorContext;
import org.apache.airflow.sdk.provider.Param;
import org.apache.airflow.sdk.provider.ProviderOperator;

/** Checks that a file exists. The result is true when something is at the path, and false when not. */
@ProviderOperator(
    module = "airflow.providers.java.localfile.operators.file",
    description = "Check that a file exists.",
    canSkipDownstream = true)
public class HasFileOperator implements Operator<Boolean> {

  @Param(required = true, templated = true, description = "The path to look for.")
  public String path;

  @Param(description = "Skip the direct downstream tasks when nothing is at the path.")
  public boolean skipDownstreamIfMissing = false;

  @Override
  public Boolean execute(OperatorContext context) throws IOException {
    try {
      Files.readAttributes(Path.of(path), BasicFileAttributes.class);
      return true;
    } catch (NoSuchFileException e) {
      if (skipDownstreamIfMissing) {
        context.skipDownstream();
      }
      return false;
    }
  }
}
