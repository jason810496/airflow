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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.airflow.sdk.provider.Operator;
import org.apache.airflow.sdk.provider.OperatorContext;
import org.apache.airflow.sdk.provider.Param;
import org.apache.airflow.sdk.provider.ProviderOperator;

/** Writes text to a file. The result describes the file as it is on disk after the write. */
@ProviderOperator(
    module = "airflow.providers.java.localfile.operators.file",
    description = "Write text to a file.")
public class WriteFileOperator implements Operator<WriteFileOperator.Result> {

  @Param(
      required = true,
      templated = true,
      description = "The file to write. Missing parent directories are created.")
  public String path;

  @Param(required = true, templated = true, description = "The text to write.")
  public String content;

  @Param(
      choices = {"overwrite", "append"},
      description = "Replace what the file holds, or add to its end.")
  public String mode = "overwrite";

  @Param(
      choices = {"utf-8", "ascii", "latin1"},
      description = "How the text is encoded in the file.")
  public String encoding = "utf-8";

  /** What the operator returns, read from the file after the write. */
  public static final class Result {
    public final String path;

    /** The size of the whole file. */
    public final long bytes;

    /** The SHA-256 of the whole file, in hex. */
    public final String sha256;

    Result(String path, long bytes, String sha256) {
      this.path = path;
      this.bytes = bytes;
      this.sha256 = sha256;
    }
  }

  @Override
  public Result execute(OperatorContext context) throws IOException {
    return write(path, content, getCharset(), "append".equals(mode));
  }

  private Charset getCharset() {
    switch (encoding) {
      case "ascii":
        return StandardCharsets.US_ASCII;
      case "latin1":
        return StandardCharsets.ISO_8859_1;
      default:
        return StandardCharsets.UTF_8;
    }
  }

  /** Writes [content] to [path], and describes the file as it is afterwards. */
  static Result write(String path, String content, Charset charset, boolean append)
      throws IOException {
    Path file = Path.of(path);
    Files.createDirectories(file.toAbsolutePath().getParent());
    Files.write(
        file,
        content.getBytes(charset),
        StandardOpenOption.CREATE,
        append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE);
    byte[] written = Files.readAllBytes(file);
    return new Result(path, written.length, sha256Hex(written));
  }

  private static String sha256Hex(byte[] data) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
      StringBuilder hex = new StringBuilder();
      for (byte b : digest) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
