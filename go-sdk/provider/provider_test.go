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

package provider

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/internal/clienttest"
)

type greetArgs struct {
	Name  airflow.Templated[string] `airflow:"name,required" doc:"Who to greet."`
	Times *int                      `airflow:"times"         doc:"How often."    default:"1"`
}

// GreetOperator is a test operator of the provider go.test.
var GreetOperator = Define(Meta{
	Module:               "airflow.providers.go.test.operators.greet",
	ClassName:            "GreetOperator",
	ImplementationModule: "airflow-provider-go-test",
	Description:          "Greet someone.",
	CanSkipDownstream:    true,
}, func(actx airflow.Context, args greetArgs) (string, error) {
	if args.Name.Value() == "nobody" {
		return "", SkipDownstream(actx)
	}
	if args.Name.Value() == "fail" {
		return "", errors.New("cannot greet")
	}
	actx.Logger().InfoContext(actx, "greeting", "name", args.Name.Value())
	return "hello " + args.Name.Value(), nil
})

// SilentOperator is a test operator that returns no result.
var SilentOperator = Define(Meta{
	Module:    "airflow.providers.go.test.operators.greet",
	ClassName: "SilentOperator",
}, func(airflow.Context, greetArgs) (struct{}, error) { return struct{}{}, nil })

func lookup(t *testing.T, classpath string) bundle.ProviderImpl {
	t.Helper()
	impl, ok := bundle.LookupProvider(classpath)
	require.True(t, ok, "%s is not registered", classpath)
	return impl
}

func TestDefineRegistersTheOperatorUnderItsClassPath(t *testing.T) {
	impl := lookup(t, "airflow.providers.go.test.operators.greet.GreetOperator")

	assert.Equal(t, []string{"name"}, impl.TemplateFields())
	assert.Equal(
		t,
		[]bundle.ProviderParam{{Name: "name", Required: true}, {Name: "times"}},
		impl.Params(),
	)
}

func TestDefineRejectsWhatCannotBeAnOperator(t *testing.T) {
	noop := func(airflow.Context, greetArgs) (string, error) { return "", nil }
	tests := map[string]func(){
		"a class name with a dot": func() {
			Define(Meta{Module: "airflow.providers.x", ClassName: "a.B"}, noop)
		},
		"a module outside the providers": func() {
			Define(Meta{Module: "os.path", ClassName: "B"}, noop)
		},
		"no function": func() {
			Define[greetArgs, string](Meta{Module: "airflow.providers.x", ClassName: "B"}, nil)
		},
		"arguments that are not a struct": func() {
			Define(Meta{Module: "airflow.providers.x", ClassName: "B"},
				func(airflow.Context, string) (string, error) { return "", nil })
		},
		"a second operator with the class path": func() {
			Define(
				Meta{
					Module:    "airflow.providers.go.test.operators.greet",
					ClassName: "GreetOperator",
				},
				noop,
			)
		},
	}
	for name, define := range tests {
		t.Run(name, func(t *testing.T) { assert.Panics(t, define) })
	}
}

func TestTheFunctionDefineReturnsAddsATaskThatNamesTheOperator(t *testing.T) {
	dag := airflow.Dag("greetings")

	assert.NotPanics(t, func() {
		dag.Task(GreetOperator(greetArgs{Name: airflow.Of("world"), Times: airflow.Ptr(2)}),
			airflow.TaskSpec{TaskID: "greet"})
	})
	assert.PanicsWithValue(t,
		`provider: GreetOperator needs the argument "name" (Name)`,
		func() { GreetOperator(greetArgs{}) })
}

func executeGreet(t *testing.T, name string, skip func(context.Context) error) (any, error) {
	t.Helper()
	impl := lookup(t, "airflow.providers.go.test.operators.greet.GreetOperator")
	return impl.Execute(context.Background(), bundle.ProviderCall{
		Args:           map[string]any{"name": name},
		Client:         clienttest.New(),
		Logger:         slog.New(slog.DiscardHandler),
		SkipDownstream: skip,
	})
}

func TestExecuteRunsTheFunctionWithTheDecodedArguments(t *testing.T) {
	result, err := executeGreet(t, "world", nil)

	require.NoError(t, err)
	assert.Equal(t, "hello world", result)
}

func TestExecuteReturnsTheErrorOfTheFunction(t *testing.T) {
	_, err := executeGreet(t, "fail", nil)

	assert.EqualError(t, err, "cannot greet")
}

func TestExecuteFailsOnArgumentsThatDoNotDecode(t *testing.T) {
	impl := lookup(t, "airflow.providers.go.test.operators.greet.GreetOperator")

	_, err := impl.Execute(context.Background(), bundle.ProviderCall{
		Args: map[string]any{"name": "x", "times": "often"}, Client: clienttest.New(),
		Logger: slog.New(slog.DiscardHandler),
	})

	assert.ErrorContains(
		t,
		err,
		`airflow.providers.go.test.operators.greet.GreetOperator: argument "times"`,
	)
}

func TestSkipDownstreamCallsWhatTheRuntimeGave(t *testing.T) {
	skipped := false

	_, err := executeGreet(t, "nobody", func(context.Context) error { skipped = true; return nil })

	require.NoError(t, err)
	assert.True(t, skipped)
}

func TestSkipDownstreamNeedsAProviderOperatorContext(t *testing.T) {
	actx := airflow.NewContext(
		context.Background(),
		slog.New(slog.DiscardHandler),
		clienttest.New(),
		airflow.TaskInstance{},
		airflow.DagRun{},
	)

	assert.ErrorContains(t, SkipDownstream(actx), "needs the Context of a provider operator")
}

func TestAnOperatorWithNoResultPushesNothing(t *testing.T) {
	impl := lookup(t, "airflow.providers.go.test.operators.greet.SilentOperator")

	result, err := impl.Execute(context.Background(), bundle.ProviderCall{
		Args: map[string]any{
			"name": "x",
		}, Client: clienttest.New(), Logger: slog.New(slog.DiscardHandler),
	})

	require.NoError(t, err)
	assert.Nil(t, result)
}
