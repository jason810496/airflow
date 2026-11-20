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

// Package provider is what a provider written in Go imports to implement operators, and what the
// plugin executable of that provider serves them with.
//
// An operator is a Go function over a struct of its arguments, with the Python class path that names
// it. [Define] registers it and returns the function that a Dag calls to add the task:
//
//	type GreetArgs struct {
//		Name airflow.Templated[string] `airflow:"name,required"`
//	}
//
//	var GreetOperator = provider.Define(provider.Meta{
//		Module:               "airflow.providers.go.example.operators.greet",
//		ClassName:            "GreetOperator",
//		ImplementationModule: "airflow-provider-go-example",
//	}, func(actx airflow.Context, args GreetArgs) (string, error) {
//		return "hello " + args.Name.Value(), nil
//	})
//
// A Go Dag that links the package adds the task with dag.Task(GreetOperator(...), airflow.TaskSpec{...}),
// and the Go runtime runs it from the Dag's executable. A Python Dag, or a Dag in another language SDK,
// uses the Python wrapper of the provider, and the generic Go runtime loads the operator from the
// plugin executable that [Serve] makes. Both call the same function, with the same arguments and the
// same [airflow.Context].
//
// ADR 0018 of the Lang SDK ADRs describes how a task finds its operator.
package provider

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// Meta holds what an operator is besides its arguments and its function.
type Meta struct {
	// Description says what the operator does, in a sentence. The operator spec and the Python wrapper
	// carry it.
	Description string
	// Module is the Python module of the operator class, such as
	// airflow.providers.go.localfile.operators.file. It must start with airflow.providers.
	Module string
	// ClassName is the name of the Python class. The Python wrapper that the spec generates has it.
	ClassName string
	// ImplementationModule is the name of the plugin executable that serves the operator, such as
	// airflow-provider-go-localfile. The generic Go runtime starts it from its plugin directory.
	ImplementationModule string
	// TemplateExt lists the file extensions that the worker reads a template from.
	TemplateExt []string
	// TemplateFieldsRenderers maps a template field to how the UI renders it.
	TemplateFieldsRenderers map[string]string
	UIColor                 string
	UIFgColor               string
	// OperatorName is the name that the UI shows when it is not ClassName.
	OperatorName string
	IsSensor     bool
	// CanSkipDownstream says that the operator may call [SkipDownstream].
	CanSkipDownstream bool
}

// Define registers the operator that Meta names and returns the function that adds a task of it to a
// Dag. A is the struct of the operator's arguments. Each exported field has an `airflow:"name"` tag
// with the Python name of the argument, and `,required` for one the task must give. A field of type
// [airflow.Templated] is a template field. A field that is not required is a pointer, a slice, a map
// or an [airflow.Templated], so that the zero value means that the task left the argument out. The
// order of the fields is the order of the operator's parameters.
//
// execute receives the arguments after Airflow has rendered the template fields and pulled the
// results of other tasks. Its result must be JSON, and it is pushed as the return_value XCom. A result
// of type struct{} pushes nothing. An error fails the task, or retries it when it has retries left.
//
// In a plugin, the Context is not cancelled when the supervisor stops the task: the runner stops the
// plugin process instead.
//
// Define panics when Meta or A is not valid, or when another operator has the same class path. Call
// it in a package-level var, so that importing the provider package registers its operators.
func Define[A, R any](
	meta Meta,
	execute func(actx airflow.Context, args A) (R, error),
) func(args A) airflow.OperatorTask {
	impl, err := newImplementation(meta, execute)
	if err != nil {
		panic(fmt.Sprintf("provider.Define: %v", err))
	}
	bundle.RegisterProvider(impl)
	return impl.newTask
}

type implementation[A, R any] struct {
	meta      Meta
	classpath string
	params    []param
	execute   func(airflow.Context, A) (R, error)
	noResult  bool
	argsType  reflect.Type
}

var _ bundle.ProviderImpl = (*implementation[struct{}, struct{}])(nil)

func newImplementation[A, R any](
	meta Meta,
	execute func(airflow.Context, A) (R, error),
) (*implementation[A, R], error) {
	if meta.ClassName == "" || strings.Contains(meta.ClassName, ".") {
		return nil, fmt.Errorf(
			"Meta.ClassName is %q; it is the name of the Python class",
			meta.ClassName,
		)
	}
	if !strings.HasPrefix(meta.Module, "airflow.providers.") {
		return nil, fmt.Errorf(
			"%s: Meta.Module is %q; it is a Python module under airflow.providers",
			meta.ClassName, meta.Module,
		)
	}
	if execute == nil {
		return nil, fmt.Errorf("%s: execute is nil", meta.ClassName)
	}
	argsType := reflect.TypeFor[A]()
	params, err := readParams(argsType)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", meta.ClassName, err)
	}
	return &implementation[A, R]{
		meta:      meta,
		classpath: meta.Module + "." + meta.ClassName,
		params:    params,
		execute:   execute,
		noResult:  reflect.TypeFor[R]() == reflect.TypeFor[struct{}](),
		argsType:  argsType,
	}, nil
}

func (i *implementation[A, R]) Classpath() string { return i.classpath }

func (i *implementation[A, R]) TemplateFields() []string {
	var fields []string
	for _, p := range i.params {
		if p.templated {
			fields = append(fields, p.name)
		}
	}
	return fields
}

func (i *implementation[A, R]) Params() []bundle.ProviderParam {
	params := make([]bundle.ProviderParam, len(i.params))
	for n, p := range i.params {
		params[n] = bundle.ProviderParam{Name: p.name, Required: p.required}
	}
	return params
}

func (i *implementation[A, R]) Execute(ctx context.Context, call bundle.ProviderCall) (any, error) {
	var args A
	if err := decodeArgs(i.params, call.Args, reflect.ValueOf(&args).Elem()); err != nil {
		return nil, fmt.Errorf("%s: %w", i.classpath, err)
	}
	actx := airflow.NewContext(
		context.WithValue(ctx, skipKey{}, call.SkipDownstream),
		call.Logger, call.Client, call.TaskInstance, call.DagRun,
	)
	result, err := i.execute(actx, args)
	if err != nil || i.noResult {
		return nil, err
	}
	return result, nil
}

// newTask is the function that Define returns.
func (i *implementation[A, R]) newTask(args A) airflow.OperatorTask {
	value := reflect.ValueOf(args)
	given := make([]airflow.OperatorArg, 0, len(i.params))
	for _, p := range i.params {
		field := value.FieldByIndex(p.index)
		if isUnset(field) {
			if p.required {
				panic(
					fmt.Sprintf(
						"provider: %s needs the argument %q (%s)",
						i.meta.ClassName,
						p.name,
						p.field,
					),
				)
			}
			continue
		}
		given = append(given, airflow.Arg(p.name, field.Interface()))
	}
	return airflow.UseOperator(i.classpath, given, airflow.OperatorInfo{
		ImplementationModule:    i.meta.ImplementationModule,
		TemplateFields:          i.TemplateFields(),
		TemplateExt:             i.meta.TemplateExt,
		TemplateFieldsRenderers: i.meta.TemplateFieldsRenderers,
		UIColor:                 i.meta.UIColor,
		UIFgColor:               i.meta.UIFgColor,
		OperatorName:            i.meta.OperatorName,
		IsSensor:                i.meta.IsSensor,
		CanSkipDownstream:       i.meta.CanSkipDownstream,
	})
}

type skipKey struct{}

// SkipDownstream skips the direct downstream tasks of the running operator, as a branch operator
// does. The operator's Meta must set CanSkipDownstream, so that Airflow skips the tasks again when
// someone clears one of them. It does nothing for a task with no downstream tasks.
func SkipDownstream(actx airflow.Context) error {
	skip, ok := actx.Value(skipKey{}).(func(context.Context) error)
	if !ok || skip == nil {
		return errors.New("provider.SkipDownstream needs the Context of a provider operator")
	}
	return skip(actx)
}
