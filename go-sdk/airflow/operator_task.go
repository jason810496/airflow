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

package airflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
)

// Templated is a constructor argument of a provider operator that Airflow renders as a template.
// It holds a value of type T, which may be a Jinja template when T is a string, or the result of an
// earlier task. [Of] and [ResultOf] build one. The zero Templated is unset, so the operator applies its
// own default.
//
// An operator that a Go provider implements reads the argument with [Templated.Value], which holds
// the rendered value by then.
type Templated[T any] struct {
	value T
	ref   *TaskRef
	set   bool
}

// Of returns a Templated that holds value.
func Of[T any](value T) Templated[T] { return Templated[T]{value: value, set: true} }

// ResultOf returns a Templated that takes the result of task, which becomes an upstream task of the
// task that uses it. The result must be a T, as JSON. A result is only ever the whole argument, never
// part of a list or a map.
func ResultOf[T any](task *TaskRef) Templated[T] { return Templated[T]{ref: task, set: true} }

// Value returns the value of an argument. It is the zero T for an unset argument, and for a result of
// another task before the task has run.
func (t Templated[T]) Value() T { return t.value }

// IsSet reports whether the argument was given.
func (t Templated[T]) IsSet() bool { return t.set }

// Unwrap returns what the argument holds, for the code that serializes it. ref is non-nil when the
// argument takes the result of a task.
func (t Templated[T]) Unwrap() (value any, ref *TaskRef, set bool) { return t.value, t.ref, t.set }

// MarshalJSON writes the value.
func (t Templated[T]) MarshalJSON() ([]byte, error) { return json.Marshal(t.value) }

// UnmarshalJSON reads the value, as an operator that a provider implements receives it.
func (t *Templated[T]) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &t.value); err != nil {
		return err
	}
	t.ref, t.set = nil, true
	return nil
}

// TemplatedArg is what every [Templated] implements.
type TemplatedArg interface {
	Unwrap() (value any, ref *TaskRef, set bool)
}

// Ptr returns a pointer to value. The optional arguments of an operator are pointers, so that a
// pointer to false or to the empty string can set the argument instead of leaving its default.
func Ptr[T any](value T) *T { return &value }

// OperatorArg is one constructor argument of a provider operator, under its Python name. [Arg]
// builds one.
type OperatorArg struct {
	name     string
	value    any
	required bool
}

// Arg returns the argument name of a provider operator with the given value. The value is one of:
//   - JSON, as encoding/json writes it: a string, a number, a bool, a slice, a map with string keys or
//     a struct
//   - a time.Time, which the operator receives as a datetime, or a time.Duration, which it receives as
//     a timedelta
//   - a [Templated], which holds a value or the result of a task
//   - a *[TaskRef], which is the result of that task
//
// A nil value, a nil pointer or a Templated that is unset leaves the argument out. A pointer stands for
// what it points at.
func Arg(name string, value any) OperatorArg { return OperatorArg{name: name, value: value} }

// RequiredArg is like [Arg], but a value that leaves the argument out is an error, which the method
// that adds the task panics with. A generated wrapper uses it for an argument that the operator's
// constructor needs.
func RequiredArg(name string, value any) OperatorArg {
	return OperatorArg{name: name, value: value, required: true}
}

// OperatorInfo holds what the serialized Dag records about an operator beyond its class path and its
// arguments. A wrapper generated from an operator spec fills it in. [UseOperator] takes at most one.
type OperatorInfo struct {
	// ImplementationModule is the module of the language SDK that implements the operator. For an
	// operator that a Go provider implements it is the name of the plugin executable. It is empty
	// for an operator that a Python provider implements.
	ImplementationModule string
	// TemplateFields lists the Python names of the arguments that the worker renders as templates.
	TemplateFields []string
	// TemplateExt lists the file extensions that the worker reads a template from.
	TemplateExt []string
	// TemplateFieldsRenderers maps a template field to how the UI renders it, such as "py" or "sql".
	TemplateFieldsRenderers map[string]string
	UIColor                 string
	UIFgColor               string
	// OperatorName is the name that the UI shows when it is not the class name.
	OperatorName string
	// IsSensor marks a sensor.
	IsSensor bool
	// CanSkipDownstream says that the operator may skip its downstream tasks, as Python's SkipMixin
	// does. Airflow reads the tasks it skipped when someone clears one of them.
	CanSkipDownstream bool
}

// OperatorTask is what [UseOperator] returns. [DagRef.Task] takes it in place of a Go function and
// adds a task that runs a provider operator.
type OperatorTask struct {
	classpath string
	args      []OperatorArg
	info      OperatorInfo
	err       error
}

// UseOperator returns a value to pass to [DagRef.Task] or [TaskGroupRef.Task] in place of a Go
// function. The task runs the provider operator that classpath names, such as
// "airflow.providers.amazon.aws.operators.s3.S3CreateObjectOperator", with the constructor arguments
// args, which go by their Python names, in the order of the operator's parameters:
//
//	dag.Task(
//		airflow.UseOperator(
//			"airflow.providers.amazon.aws.operators.s3.S3CreateObjectOperator",
//			[]airflow.OperatorArg{
//				airflow.Arg("s3_bucket", "reports"),
//				airflow.Arg("s3_key", "{{ ds }}/report.json"),
//				airflow.Arg("data", report),
//			},
//		),
//		airflow.TaskSpec{TaskID: "write_report", Queue: "default"},
//	)
//
// It is the only call that writes a provider task. A generated wrapper such as
// providers/amazon/aws/operators/s3.S3CreateObjectOperator calls it with typed arguments, so call it
// directly only when no wrapper exists. Nothing checks the class path or the argument names before
// the task runs.
//
// A Python worker that has the provider installed builds and runs the operator, whichever queue the
// task takes from its [TaskSpec], or else from the [DagSpec]. An operator that a Go provider
// implements, such as the go.localfile provider, names its plugin in info.ImplementationModule, and
// runs in the Go runtime when its queue is routed to the Go coordinator.
//
// DagRef.Task needs a TaskSpec that sets TaskID, takes no [Inputs], and panics if an argument or info
// is not valid.
func UseOperator(classpath string, args []OperatorArg, info ...OperatorInfo) OperatorTask {
	task := OperatorTask{classpath: classpath, args: slices.Clone(args)}
	switch {
	case len(info) > 1:
		task.err = fmt.Errorf(
			"airflow.UseOperator got %d airflow.OperatorInfo values; set everything in one",
			len(info),
		)
	case len(info) == 1:
		task.info = copySpec(info[0])
	}
	return task
}

// operatorTask is the checked form of an OperatorTask that a task holds.
type operatorTask struct {
	module, className string
	info              OperatorInfo
	// kwargs holds the bindings of the arguments, as _operator_kwargs writes them.
	kwargs []map[string]any
	// upstream holds the tasks whose results the arguments take, without a repeat.
	upstream []*TaskRef
}

// checkOperatorTask checks a task from UseOperator and encodes its arguments.
func checkOperatorTask(op OperatorTask) (*operatorTask, error) {
	if op.err != nil {
		return nil, op.err
	}
	dot := strings.LastIndex(op.classpath, ".")
	if dot <= 0 || dot == len(op.classpath)-1 {
		return nil, fmt.Errorf(
			"airflow.UseOperator got the class path %q; give the Python module and the class, "+
				"such as \"airflow.providers.standard.operators.bash.BashOperator\"",
			op.classpath,
		)
	}
	checked := &operatorTask{
		module:    op.classpath[:dot],
		className: op.classpath[dot+1:],
		info:      op.info,
	}
	seen := map[string]bool{}
	for _, arg := range op.args {
		if arg.name == "" {
			return nil, errors.New("airflow.UseOperator got an argument with no name")
		}
		if seen[arg.name] {
			return nil, fmt.Errorf("airflow.UseOperator got the argument %q twice", arg.name)
		}
		seen[arg.name] = true
		binding, ref, err := encodeOperatorArg(arg)
		if err != nil {
			return nil, fmt.Errorf("argument %q of %s: %w", arg.name, op.classpath, err)
		}
		if binding == nil {
			if arg.required {
				return nil, fmt.Errorf(
					"%s needs the argument %q, which is unset",
					op.classpath,
					arg.name,
				)
			}
			continue
		}
		checked.kwargs = append(checked.kwargs, binding)
		if ref != nil && !slices.Contains(checked.upstream, ref) {
			checked.upstream = append(checked.upstream, ref)
		}
	}
	return checked, nil
}

const (
	durationSecondsFormat = "duration-seconds"
	dateTimeFormat        = "date-time"
)

var (
	durationType = reflect.TypeFor[time.Duration]()
	timeType     = reflect.TypeFor[time.Time]()
)

// literalBinding returns an argument binding that holds a literal.
func literalBinding(name string, value any, schemaFormat string) map[string]any {
	binding := map[string]any{"name": name, "kind": "literal", "value": value}
	if schemaFormat == durationSecondsFormat {
		binding["value_schema"] = map[string]any{"type": "number", "format": schemaFormat}
	} else if schemaFormat != "" {
		binding["value_schema"] = map[string]any{"type": "string", "format": schemaFormat}
	}
	return binding
}

// encodeOperatorArg returns the argument binding of arg, and the task whose result it takes. The
// binding is nil when the argument is unset.
func encodeOperatorArg(arg OperatorArg) (map[string]any, *TaskRef, error) {
	value := arg.value
	if templated, ok := value.(TemplatedArg); ok {
		inner, ref, set := templated.Unwrap()
		if !set {
			return nil, nil, nil
		}
		if ref != nil {
			return xcomBinding(arg.name, ref)
		}
		value = inner
	}
	if ref, ok := value.(*TaskRef); ok {
		return xcomBinding(arg.name, ref)
	}
	v := reflect.ValueOf(value)
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return nil, nil, nil
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return nil, nil, nil
	}
	switch {
	case v.Type() == durationType:
		return literalBinding(
			arg.name,
			encodeDuration(time.Duration(v.Int())),
			durationSecondsFormat,
		), nil, nil
	case v.Type() == timeType:
		moment := v.Interface().(time.Time)
		if err := checkTime("the time", moment); err != nil {
			return nil, nil, err
		}
		return literalBinding(
			arg.name, moment.UTC().Format("2006-01-02T15:04:05.999999-07:00"), dateTimeFormat,
		), nil, nil
	case (v.Kind() == reflect.Slice || v.Kind() == reflect.Map) && v.IsNil():
		return nil, nil, nil
	}
	copied, err := copyLiteral(v.Interface())
	if err != nil {
		return nil, nil, err
	}
	return literalBinding(arg.name, copied, ""), nil, nil
}

func xcomBinding(name string, ref *TaskRef) (map[string]any, *TaskRef, error) {
	if ref == nil {
		return nil, nil, errors.New("got a nil *airflow.TaskRef")
	}
	return map[string]any{"name": name, "kind": "xcom", "task_id": ref.taskID}, ref, nil
}

// checkOperatorUpstreams panics unless every task whose result an argument takes is a task of d, and
// not a task that returns no result. method names the caller in panic messages.
func (d *DagRef) checkOperatorUpstreams(method, taskID string, op *operatorTask) {
	for _, upstream := range op.upstream {
		switch {
		case upstream.dag != nil && upstream.dag != d:
			panic(fmt.Sprintf(
				"%s: task %q of Dag %q cannot take the result of task %q of another Dag, %q",
				method, taskID, d.dagID, upstream.taskID, upstream.dag.dagID,
			))
		case d.tasksByID[upstream.taskID] != upstream:
			panic(fmt.Sprintf(
				"%s: task %q of Dag %q takes a *airflow.TaskRef that DagRef.Task or "+
					"TaskGroupRef.Task did not return",
				method, taskID, d.dagID,
			))
		case upstream.triggerDagRun != nil:
			panic(fmt.Sprintf(
				"%s: task %q of Dag %q takes the result of task %q, "+
					"but that task comes from airflow.TriggerDagRun and returns no result",
				method, taskID, d.dagID, upstream.taskID,
			))
		case upstream.operator == nil && upstream.resultType == nil:
			panic(fmt.Sprintf(
				"%s: task %q of Dag %q takes the result of task %q, "+
					"but that task returns only an error",
				method, taskID, d.dagID, upstream.taskID,
			))
		}
	}
}

// serializeOperatorTask writes the fields of a task from UseOperator. spec is the TaskSpec of the
// task, after the Dag's queue has filled in.
func serializeOperatorTask(data map[string]any, op *operatorTask, spec TaskSpec) {
	data["task_type"] = op.className
	data["_task_module"] = op.module
	data["language"] = goLanguage
	// Python writes template_fields for every operator, and an empty list is not null.
	data["template_fields"] = append([]string{}, op.info.TemplateFields...)
	if len(op.info.TemplateExt) > 0 {
		data["template_ext"] = slices.Clone(op.info.TemplateExt)
	}
	if len(op.info.TemplateFieldsRenderers) > 0 {
		renderers := make(map[string]any, len(op.info.TemplateFieldsRenderers))
		for field, renderer := range op.info.TemplateFieldsRenderers {
			renderers[field] = renderer
		}
		data["template_fields_renderers"] = renderers
	}
	if op.info.UIColor != "" {
		data["ui_color"] = op.info.UIColor
	}
	if op.info.UIFgColor != "" {
		data["ui_fgcolor"] = op.info.UIFgColor
	}
	if op.info.IsSensor {
		data["_is_sensor"] = true
	}
	if op.info.OperatorName != "" && op.info.OperatorName != op.className {
		data["_operator_name"] = op.info.OperatorName
	}
	if op.info.CanSkipDownstream {
		data["_can_skip_downstream"] = true
	}
	if op.info.ImplementationModule != "" {
		data["_operator_module"] = op.info.ImplementationModule
	}
	kwargs := make([]any, 0, len(op.kwargs))
	given := make(map[string]bool, len(op.kwargs))
	for _, kwarg := range op.kwargs {
		kwargs = append(kwargs, kwarg)
		given[kwarg["name"].(string)] = true
	}
	for _, kwarg := range specKwargs(spec) {
		if !given[kwarg["name"].(string)] {
			kwargs = append(kwargs, kwarg)
		}
	}
	data["_operator_kwargs"] = kwargs
}

// specKwargs returns the fields of a TaskSpec that it sets as BaseOperator constructor arguments, so
// that the worker builds the operator with the retries, the pool and the other options the task
// declares. A field named in taskSpecRules.skip is not an argument of the operator.
func specKwargs(spec TaskSpec) []map[string]any {
	var kwargs []map[string]any
	value := reflect.ValueOf(spec)
	for i := range value.NumField() {
		name := value.Type().Field(i).Name
		field, ok := taskSpecRules.fields[name]
		if !ok {
			panic(fmt.Sprintf("airflow: the serializer has no schema field for TaskSpec.%s", name))
		}
		if taskSpecRules.skip[name] {
			continue
		}
		encoded, set := encodeSpecValue(value.Field(i), taskSpecRules.set[name])
		if !set || (field.schemaDefault != nil && isSameJSON(encoded, field.schemaDefault)) {
			continue
		}
		fieldType := value.Type().Field(i).Type
		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}
		switch fieldType {
		case durationType:
			kwargs = append(kwargs, literalBinding(field.key, encoded, durationSecondsFormat))
		case timeType:
			moment := reflect.Indirect(value.Field(i)).Interface().(time.Time)
			kwargs = append(kwargs, literalBinding(
				field.key, moment.UTC().Format("2006-01-02T15:04:05.999999-07:00"), dateTimeFormat,
			))
		default:
			kwargs = append(kwargs, literalBinding(field.key, encoded, ""))
		}
	}
	return kwargs
}
