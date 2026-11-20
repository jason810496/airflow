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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const s3Operator = "airflow.providers.amazon.aws.operators.s3.S3CreateObjectOperator"

func buildReport(Context) (string, error)                     { return "", nil }
func summarizeReport(Context, string, string) (string, error) { return "", nil }
func finish(Context, string) error                            { return nil }
func logOnly(Context) error                                   { return nil }

// kwargLiteral is the argument binding of a literal.
func kwargLiteral(name string, value any) map[string]any {
	return map[string]any{"name": name, "kind": "literal", "value": value}
}

func TestUseOperatorWritesAProviderTask(t *testing.T) {
	dag := Dag("publish", DagSpec{Queue: "golang"})
	report := dag.Task(buildReport)

	dag.Task(
		UseOperator(s3Operator, []OperatorArg{
			Arg("s3_bucket", "reports"),
			Arg("s3_key", "{{ ds }}/report.json"),
			Arg("data", report),
			Arg("replace", ptr(true)),
			Arg("encrypt", (*bool)(nil)),
		}, OperatorInfo{
			TemplateFields: []string{"s3_bucket", "s3_key", "data"},
			UIColor:        "#ff0000",
			IsSensor:       true,
		}),
		TaskSpec{TaskID: "write_report", Queue: "python", Retries: 2},
	)
	task := serializedTask(t, serializedDag(t, dag), "write_report")

	assert.Equal(t, "S3CreateObjectOperator", task["task_type"])
	assert.Equal(t, "airflow.providers.amazon.aws.operators.s3", task["_task_module"])
	assert.Equal(t, true, task["is_stub"])
	assert.Equal(t, "provider_operator", task["stub_kind"])
	assert.Equal(t, "python", task["queue"])
	assert.Equal(t, []any{"s3_bucket", "s3_key", "data"}, task["template_fields"])
	assert.Equal(t, "#ff0000", task["ui_color"])
	assert.Equal(t, true, task["_is_sensor"])
	assert.NotContains(t, task, "_operator_module", "a Python operator names no module")
	assert.NotContains(t, task, "_arg_bindings")
	assert.Equal(t, []any{
		kwargLiteral("s3_bucket", "reports"),
		kwargLiteral("s3_key", "{{ ds }}/report.json"),
		map[string]any{"name": "data", "kind": "xcom", "task_id": "buildReport"},
		kwargLiteral("replace", true),
		// The task's own options follow the operator's arguments, as BaseOperator arguments.
		kwargLiteral("queue", "python"),
		kwargLiteral("retries", 2.0),
	}, task["_operator_kwargs"])
}

func TestUseOperatorAddsAnEdgeToTheTaskItTakesAResultFrom(t *testing.T) {
	dag := Dag("publish")
	report := dag.Task(buildReport)
	other := dag.Task(summarizeReport, Inputs(report, Literal("s3://out")))

	write := dag.Task(
		UseOperator(s3Operator, []OperatorArg{Arg("data", ResultOf[string](other))}),
		TaskSpec{TaskID: "write"},
	)

	assert.Equal(t, []*TaskRef{other}, write.upstreams)
	assert.Equal(t, []*TaskRef{write}, other.downstreams)
}

func TestUseOperatorNamesTheModuleOfAnOperatorGoImplements(t *testing.T) {
	dag := Dag("files")
	dag.Task(
		UseOperator(
			"airflow.providers.go.localfile.operators.file.HasFileOperator",
			[]OperatorArg{Arg("path", "/tmp/x")},
			OperatorInfo{
				ImplementationModule: "airflow-provider-go-localfile",
				CanSkipDownstream:    true,
			},
		),
		TaskSpec{TaskID: "has"},
	)
	task := serializedTask(t, serializedDag(t, dag), "has")

	assert.Equal(t, "airflow-provider-go-localfile", task["_operator_module"])
	assert.Equal(t, true, task["_can_skip_downstream"])
	assert.Equal(t, "HasFileOperator", task["task_type"])
}

func TestUseOperatorEncodesTimesAndDurations(t *testing.T) {
	dag := Dag("times")
	dag.Task(
		UseOperator(s3Operator, []OperatorArg{
			Arg("timeout", 90*time.Second+500*time.Millisecond),
			Arg("since", time.Date(2026, 11, 20, 9, 30, 0, 0, time.FixedZone("x", 3600))),
			Arg("tags", []string{"a", "b"}),
			Arg("options", map[string]any{"n": 1}),
			Arg("missing", []string(nil)),
		}),
		TaskSpec{TaskID: "times", RetryDelay: ptr(2 * time.Minute)},
	)
	task := serializedTask(t, serializedDag(t, dag), "times")

	kwargs := task["_operator_kwargs"].([]any)
	require.Len(t, kwargs, 5)
	assert.Equal(t, map[string]any{
		"name": "timeout", "kind": "literal", "value": 90.5,
		"value_schema": map[string]any{"type": "number", "format": "duration-seconds"},
	}, kwargs[0])
	assert.Equal(t, map[string]any{
		"name": "since", "kind": "literal", "value": "2026-11-20T08:30:00+00:00",
		"value_schema": map[string]any{"type": "string", "format": "date-time"},
	}, kwargs[1])
	assert.Equal(t, kwargLiteral("tags", []any{"a", "b"}), kwargs[2])
	assert.Equal(t, kwargLiteral("options", map[string]any{"n": 1.0}), kwargs[3])
	assert.Equal(t, map[string]any{
		"name": "retry_delay", "kind": "literal", "value": 120.0,
		"value_schema": map[string]any{"type": "number", "format": "duration-seconds"},
	}, kwargs[4])
}

func TestUseOperatorLeavesOutAnUnsetTemplated(t *testing.T) {
	dag := Dag("unset")
	dag.Task(
		UseOperator(s3Operator, []OperatorArg{
			Arg("a", Templated[string]{}),
			Arg("b", Of("x")),
			Arg("c", nil),
		}),
		TaskSpec{TaskID: "unset"},
	)

	assert.Equal(
		t,
		[]any{kwargLiteral("b", "x")},
		serializedTask(t, serializedDag(t, dag), "unset")["_operator_kwargs"],
	)
}

func TestUseOperatorRejectsWhatItCannotWrite(t *testing.T) {
	other := Dag("other").Task(buildReport)
	tests := []struct {
		name string
		task OperatorTask
		opts []TaskOption
		want string
	}{
		{
			"a class path with no module",
			UseOperator("S3Operator", nil),
			nil,
			`airflow.UseOperator got the class path "S3Operator"`,
		},
		{
			"a twice-given argument",
			UseOperator(s3Operator, []OperatorArg{Arg("a", 1), Arg("a", 2)}),
			nil,
			`airflow.UseOperator got the argument "a" twice`,
		},
		{
			"an argument with no name",
			UseOperator(s3Operator, []OperatorArg{Arg("", 1)}),
			nil,
			"airflow.UseOperator got an argument with no name",
		},
		{
			"two infos",
			UseOperator(s3Operator, nil, OperatorInfo{}, OperatorInfo{}),
			nil,
			"airflow.UseOperator got 2 airflow.OperatorInfo values",
		},
		{
			"a value JSON cannot hold",
			UseOperator(s3Operator, []OperatorArg{Arg("a", make(chan int))}),
			nil,
			`argument "a" of ` + s3Operator,
		},
		{
			"a task result inside a list",
			UseOperator(s3Operator, []OperatorArg{Arg("a", []any{other})}),
			nil,
			"is a *airflow.TaskRef, which is an edge and not data",
		},
		{
			"a required argument that is unset",
			UseOperator(s3Operator, []OperatorArg{RequiredArg("a", Templated[string]{})}),
			nil,
			`needs the argument "a", which is unset`,
		},
		{
			"a result of another Dag",
			UseOperator(s3Operator, []OperatorArg{Arg("a", other)}),
			nil,
			`cannot take the result of task "buildReport" of another Dag, "other"`,
		},
		{
			"Inputs",
			UseOperator(s3Operator, nil),
			[]TaskOption{Inputs()},
			"comes from airflow.UseOperator and takes no airflow.Inputs",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := append([]TaskOption{TaskSpec{TaskID: "write"}}, tt.opts...)
			defer func() {
				assert.Contains(t, recover(), tt.want)
			}()
			Dag("publish").Task(tt.task, opts...)
		})
	}
}

func TestUseOperatorNeedsATaskID(t *testing.T) {
	assert.PanicsWithValue(t,
		`airflow.DagRef.Task: Dag "publish": a task from airflow.UseOperator with the class path `+
			`"`+s3Operator+`" has no Go function to take a task_id from; `+
			`set one with airflow.TaskSpec{TaskID: ...}`,
		func() { Dag("publish").Task(UseOperator(s3Operator, nil)) },
	)
}

func TestAnOperatorIsNotACondition(t *testing.T) {
	assert.Panics(t, func() { Dag("d").If(UseOperator(s3Operator, nil), TaskSpec{TaskID: "x"}) })
	assert.Panics(
		t,
		func() { Dag("d").Switch(UseOperator(s3Operator, nil), TaskSpec{TaskID: "x"}) },
	)
}

func TestAGoTaskTakesTheResultOfAnOperator(t *testing.T) {
	dag := Dag("mixed")
	run := dag.Task(
		UseOperator("airflow.providers.standard.operators.bash.BashOperator",
			[]OperatorArg{Arg("bash_command", "echo hi")}),
		TaskSpec{TaskID: "run"},
	)

	assert.NotPanics(t, func() { dag.Task(finish, Inputs(run)) })
}

func TestATaskCannotTakeInputsFromATriggerOrAnErrorOnlyTask(t *testing.T) {
	dag := Dag("mixed")
	trigger := dag.Task(TriggerDagRun(TriggerDagRunSpec{DagID: "x"}), TaskSpec{TaskID: "t"})
	failing := dag.Task(logOnly)

	for _, upstream := range []*TaskRef{trigger, failing} {
		assert.Panics(t, func() {
			dag.Task(
				UseOperator(s3Operator, []OperatorArg{Arg("a", upstream)}),
				TaskSpec{TaskID: "w" + upstream.taskID},
			)
		})
	}
}

func TestTemplatedHoldsAValueOrTheResultOfATask(t *testing.T) {
	task := &TaskRef{taskID: "build"}

	var unset Templated[string]
	assert.False(t, unset.IsSet())
	assert.Empty(t, unset.Value())

	value := Of("x")
	assert.True(t, value.IsSet())
	assert.Equal(t, "x", value.Value())
	got, ref, set := value.Unwrap()
	assert.Equal(t, "x", got)
	assert.Nil(t, ref)
	assert.True(t, set)

	result := ResultOf[string](task)
	assert.True(t, result.IsSet())
	_, ref, set = result.Unwrap()
	assert.Same(t, task, ref)
	assert.True(t, set)
}

func TestTemplatedReadsItsValueFromJSON(t *testing.T) {
	var count Templated[[]int]

	require.NoError(t, json.Unmarshal([]byte(`[1,2]`), &count))

	assert.True(t, count.IsSet())
	assert.Equal(t, []int{1, 2}, count.Value())
	data, err := json.Marshal(count)
	require.NoError(t, err)
	assert.JSONEq(t, `[1,2]`, string(data))
	assert.Error(t, json.Unmarshal([]byte(`"x"`), &count))
}

func TestPtrPointsToACopy(t *testing.T) {
	original := false
	copied := Ptr(original)

	*copied = true

	assert.False(t, original)
}

func TestAProviderTaskTakesTheQueueOfTheDagUnlessItSetsItsOwn(t *testing.T) {
	dag := Dag("queues", DagSpec{Queue: "golang"})
	dag.Task(UseOperator(s3Operator, nil), TaskSpec{TaskID: "inherits"})
	dag.Task(UseOperator(s3Operator, nil), TaskSpec{TaskID: "overrides", Queue: "python"})
	serialized := serializedDag(t, dag)

	assert.Equal(t, "golang", serializedTask(t, serialized, "inherits")["queue"])
	assert.Equal(t, "python", serializedTask(t, serialized, "overrides")["queue"])
}
