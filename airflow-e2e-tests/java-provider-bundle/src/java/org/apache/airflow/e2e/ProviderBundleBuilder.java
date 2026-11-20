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

package org.apache.airflow.e2e;

import org.apache.airflow.e2e.providerdag.JavaLocalfileDag;
import org.apache.airflow.e2e.providerdag.JavaProviderOperatorsDag;
import org.apache.airflow.sdk.Bundle;
import org.apache.airflow.sdk.Server;

/**
 * Bundle for the provider operator E2E tests: two Dags declared entirely in Java, parsed by the Dag
 * processor from this JAR.
 *
 * <p>{@code java_localfile} uses the operators of {@code apache-airflow-providers-java-localfile},
 * which this JAR holds, so its tasks run from this JAR. {@code java_provider_operators} uses the
 * generated wrappers of the amazon and standard providers, which a Python worker runs.
 */
public class ProviderBundleBuilder {
  public static Bundle build() {
    return new Bundle().register(JavaLocalfileDag.build()).register(JavaProviderOperatorsDag.build());
  }

  public static void main(String[] args) {
    Server.create(args).serve(build());
  }
}
