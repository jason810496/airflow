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

package execution

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"runtime/debug"
	"slices"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/pkg/binding"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
	"github.com/apache/airflow/go-sdk/sdk"
)

// runProviderOperator runs the provider operator that ti_context.provider_operator names. The task is
// a task of a Go Dag, or a task of a Dag in another language that the generic provider runner runs.
// Both get the operator the same way, so they share one path for the arguments, the failure and the
// return_value XCom.
//
// It finds the implementation among the operators this binary links, or else loads it from the module
// the task names. It pulls the results of other tasks, has the supervisor render the template fields,
// runs the operator and pushes what it returns. An error from any step fails the task, or retries it
// when ti_context.should_retry is set.
func runProviderOperator(
	ctx context.Context,
	details *genmodels.StartupDetails,
	provider *genmodels.TIProviderOperator,
	client *CoordinatorClient,
	ti sdk.TaskInstance,
	dagRun sdk.DagRun,
	logger *slog.Logger,
) (result any) {
	shouldRetry := details.TIContext.ShouldRetry
	fail := func(err error) any {
		logger.ErrorContext(ctx, "Task failed", "error", err)
		return failTask(shouldRetry, err)
	}
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Recovered panic in task", "error", r, "stack", string(debug.Stack()))
			result = fail(fmt.Errorf("panic: %v", r))
		}
	}()

	impl, closer, err := findProviderImpl(ctx, provider, ti, logger)
	if err != nil {
		return fail(err)
	}
	if closer != nil {
		defer func() {
			if err := closer.Close(); err != nil {
				logger.Warn("Cannot stop the provider plugin", "error", err)
			}
		}()
	}

	args, err := resolveProviderArgs(ctx, impl, provider, client, ti, logger)
	if err != nil {
		return fail(err)
	}

	downstream := provider.DownstreamTaskIds[ti.TaskID]
	call := bundle.ProviderCall{
		Args:         args,
		TaskInstance: ti,
		DagRun:       dagRun,
		Client:       client,
		Logger:       logger,
		SkipDownstream: func(ctx context.Context) error {
			return bundle.SkipTasks(ctx, client, client.skipDownstreamTasks, ti, downstream, logger)
		},
	}
	value, err := impl.Execute(ctx, call)
	if err != nil {
		return fail(err)
	}
	if hasResult(value) {
		if err := client.PushXCom(ctx, ti, sdk.XComReturnValueKey, value); err != nil {
			return fail(fmt.Errorf("setting the return value XCom: %w", err))
		}
	}
	return succeedTask()
}

// hasResult reports whether an operator returned something to push as its return_value XCom. Like a
// task function, an operator that returns a nil pointer pushes nothing.
func hasResult(value any) bool {
	if value == nil {
		return false
	}
	v := reflect.ValueOf(value)
	return v.Kind() != reflect.Pointer || !v.IsNil()
}

// findProviderImpl returns the implementation of the operator, and what to close once the task is
// over.
func findProviderImpl(
	ctx context.Context,
	provider *genmodels.TIProviderOperator,
	ti sdk.TaskInstance,
	logger *slog.Logger,
) (bundle.ProviderImpl, io.Closer, error) {
	classpath := provider.Classpath
	if impl, ok := bundle.LookupProvider(classpath); ok {
		return impl, nil, nil
	}
	module, _ := provider.Module.(string)
	switch loader := bundle.GetProviderLoader(); {
	case module == "":
		return nil, nil, fmt.Errorf(
			"task %q is a %s, which only a Python worker runs; give it a queue that "+
				"[sdk] queue_to_coordinator does not route to the Go coordinator",
			ti.TaskID, classpath,
		)
	case loader == nil:
		return nil, nil, fmt.Errorf(
			"task %q is a %s, which the module %q implements, but this executable does not link it "+
				"and loads no plugins; build the Dag executable with the provider package, or route "+
				"the task to the generic provider runner",
			ti.TaskID,
			classpath,
			module,
		)
	default:
		impl, closer, err := loader.Load(ctx, classpath, module, logger)
		if err != nil {
			return nil, nil, err
		}
		return impl, closer, nil
	}
}

// resolveProviderArgs returns the arguments of the task by their Python names. A literal is its
// value, except that the template fields are rendered by the supervisor. The result of another task is
// read from its return_value XCom.
func resolveProviderArgs(
	ctx context.Context,
	impl bundle.ProviderImpl,
	provider *genmodels.TIProviderOperator,
	client *CoordinatorClient,
	ti sdk.TaskInstance,
	logger *slog.Logger,
) (map[string]any, error) {
	bindings := genmodels.ArgBindings(provider.Kwargs)
	args, err := convertArgBindings(&bindings)
	if err != nil {
		return nil, fmt.Errorf("invalid operator arguments of %s: %w", provider.Classpath, err)
	}

	defined := make(map[string]bundle.ProviderParam)
	for _, param := range impl.Params() {
		defined[param.Name] = param
	}
	var ignored []string
	var known []binding.Arg
	for _, arg := range args {
		if _, ok := defined[arg.ArgName()]; ok {
			known = append(known, arg)
		} else {
			ignored = append(ignored, arg.ArgName())
		}
	}
	if len(ignored) > 0 {
		// The task's BaseOperator options, such as queue, travel with the arguments for a Python worker.
		logger.Debug("Ignoring arguments that the operator does not define",
			"operator", provider.Classpath, "arguments", ignored)
	}
	given := make(map[string]bool, len(known))
	for _, arg := range known {
		given[arg.ArgName()] = true
	}
	for _, param := range impl.Params() {
		if param.Required && !given[param.Name] {
			return nil, fmt.Errorf(
				"%s needs the argument %q, which the task lacks", provider.Classpath, param.Name,
			)
		}
	}

	templated := impl.TemplateFields()
	values := make(map[string]any, len(known))
	toRender := map[string]any{}
	for _, arg := range known {
		switch arg := arg.(type) {
		case binding.LiteralArg:
			values[arg.Name] = arg.Value
			if slices.Contains(templated, arg.Name) {
				toRender[arg.Name] = arg.Value
			}
		case binding.XComArg:
			value, err := pullUpstreamResult(ctx, client, ti, arg)
			if err != nil {
				return nil, err
			}
			values[arg.Name] = value
		}
	}
	if len(toRender) > 0 {
		rendered, err := client.renderTemplateFields(ctx, toRender)
		if err != nil {
			return nil, fmt.Errorf(
				"rendering the template fields of %s: %w",
				provider.Classpath,
				err,
			)
		}
		for name := range toRender {
			value, ok := rendered[name]
			if !ok {
				return nil, fmt.Errorf(
					"the supervisor did not render the template field %q of %s",
					name,
					provider.Classpath,
				)
			}
			values[name] = value
		}
	}
	return values, nil
}

// pullUpstreamResult reads the return_value XCom of the task that an argument takes its value from.
func pullUpstreamResult(
	ctx context.Context,
	client *CoordinatorClient,
	ti sdk.TaskInstance,
	arg binding.XComArg,
) (any, error) {
	value, err := client.GetXCom(
		ctx,
		ti.DagID,
		ti.RunID,
		arg.TaskID,
		nil,
		sdk.XComReturnValueKey,
		nil,
	)
	if errors.Is(err, sdk.XComNotFound) {
		return nil, fmt.Errorf(
			"the argument %q takes the result of upstream task %q, which pushed no %s XCom; "+
				"a task that returns nothing pushes none",
			arg.Name, arg.TaskID, sdk.XComReturnValueKey,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("reading the result of upstream task %q: %w", arg.TaskID, err)
	}
	return value, nil
}
