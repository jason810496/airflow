// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"errors"
	"fmt"
	"strings"

	"coceuretail.example/finance/internal/closing"
	"coceuretail.example/finance/internal/lake"
	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/sdk"
)

func ptr[T any](v T) *T { return &v }

// variable reads a Variable and reports whether it is set.
func variable(actx airflow.Context, key string) (string, bool, error) {
	value, err := actx.Client().GetVariable(actx, key)
	if errors.Is(err, sdk.VariableNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read Variable %s: %w", key, err)
	}
	return value, true, nil
}

func flag(actx airflow.Context, key string) (bool, error) {
	value, _, err := variable(actx, key)
	return strings.EqualFold(strings.TrimSpace(value), "true"), err
}

// storefrontBatchDir is the batch that this close is for.
func storefrontBatchDir(actx airflow.Context) (string, error) {
	dir, ok, err := variable(actx, lake.StorefrontBatchVariable)
	if err != nil {
		return "", err
	}
	if !ok || dir == "" {
		return "", fmt.Errorf("the Variable %s is not set, so there is no storefront batch to close", lake.StorefrontBatchVariable)
	}
	return dir, nil
}

func logf(actx airflow.Context, format string, args ...any) {
	actx.Logger().InfoContext(actx, fmt.Sprintf(format, args...))
}

func logger(actx airflow.Context) closing.Logf {
	return func(format string, args ...any) { logf(actx, format, args...) }
}
