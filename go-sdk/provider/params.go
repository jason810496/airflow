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
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/apache/airflow/go-sdk/airflow"
)

// tagKey is the struct tag that names the Python argument that a field of an operator's arguments
// struct holds. Its value is the Python name, then the options: required.
//
//	Path airflow.Templated[string] `airflow:"path,required"`
//
// Two more tags describe the argument in the operator spec. default holds the value that the operator
// applies when the task leaves the argument out, as JSON, or as the text itself for a string. enum
// lists the values of a string argument that takes only some. doc is the description that the operator
// spec and the Python wrapper give the argument.
const (
	tagKey     = "airflow"
	defaultTag = "default"
	enumTag    = "enum"
	docTag     = "doc"
)

var (
	durationType     = reflect.TypeFor[time.Duration]()
	timeType         = reflect.TypeFor[time.Time]()
	templatedArgType = reflect.TypeFor[airflow.TemplatedArg]()
)

// param is one field of an operator's arguments struct.
type param struct {
	// field is the name of the struct field.
	field string
	index []int
	// name is the Python name of the argument.
	name     string
	required bool
	// templated is true for a field of type airflow.Templated.
	templated bool
	typ       reflect.Type
	// valueType is the type of the argument's value: typ without the pointer, and without
	// airflow.Templated.
	valueType reflect.Type
	// defaultText is the raw text of the default tag, or empty.
	defaultText string
	enum        []string
	doc         string
}

// readParams reads the fields of the arguments struct of an operator.
func readParams(args reflect.Type) ([]param, error) {
	if args.Kind() != reflect.Struct {
		return nil, fmt.Errorf("the arguments of an operator are a struct, not %s", args)
	}
	var params []param
	seen := map[string]string{}
	for i := range args.NumField() {
		field := args.Field(i)
		if !field.IsExported() {
			continue
		}
		tag, ok := field.Tag.Lookup(tagKey)
		if !ok {
			return nil, fmt.Errorf(
				"field %s has no `%s:\"name\"` tag that names its Python argument",
				field.Name,
				tagKey,
			)
		}
		options := strings.Split(tag, ",")
		p := param{field: field.Name, index: field.Index, name: options[0], typ: field.Type}
		if p.name == "" {
			return nil, fmt.Errorf("field %s has an empty Python argument name", field.Name)
		}
		for _, option := range options[1:] {
			if option != "required" {
				return nil, fmt.Errorf(
					"field %s: unknown option %q in its %s tag",
					field.Name,
					option,
					tagKey,
				)
			}
			p.required = true
		}
		if other, dup := seen[p.name]; dup {
			return nil, fmt.Errorf(
				"fields %s and %s both name the argument %q",
				other,
				field.Name,
				p.name,
			)
		}
		seen[p.name] = field.Name

		p.valueType = field.Type
		if p.valueType.Kind() == reflect.Pointer {
			p.valueType = p.valueType.Elem()
		}
		if reflect.PointerTo(p.valueType).Implements(templatedArgType) {
			p.templated = true
			inner, ok := p.valueType.FieldByName("value")
			if !ok {
				return nil, fmt.Errorf(
					"field %s: cannot read the type of its airflow.Templated",
					field.Name,
				)
			}
			p.valueType = inner.Type
			if p.valueType == durationType {
				return nil, fmt.Errorf(
					"field %s is a airflow.Templated[time.Duration], but a duration is not a template field",
					field.Name,
				)
			}
		}
		p.defaultText = field.Tag.Get(defaultTag)
		p.doc = field.Tag.Get(docTag)
		if enum := field.Tag.Get(enumTag); enum != "" {
			p.enum = strings.Split(enum, ",")
		}
		params = append(params, p)
	}
	return params, nil
}

// decodeArgs fills the struct dst from the arguments by their Python names. An argument the task left
// out takes the default tag of its field. The values are JSON, but a duration, which is a number of
// seconds.
func decodeArgs(params []param, args map[string]any, dst reflect.Value) error {
	for _, p := range params {
		field := dst.FieldByIndex(p.index)
		raw, given := args[p.name]
		if !given || raw == nil {
			if p.defaultText == "" {
				continue
			}
			value, err := parseDefault(p)
			if err != nil {
				return err
			}
			raw = value
		}
		if err := checkEnum(p, raw); err != nil {
			return err
		}
		if err := decodeValue(p, raw, field); err != nil {
			return fmt.Errorf("argument %q: %w", p.name, err)
		}
	}
	return nil
}

func checkEnum(p param, raw any) error {
	if len(p.enum) == 0 {
		return nil
	}
	text, ok := raw.(string)
	if !ok || !slices.Contains(p.enum, text) {
		return fmt.Errorf("argument %q is %v, and it takes one of %q", p.name, raw, p.enum)
	}
	return nil
}

func decodeValue(p param, raw any, field reflect.Value) error {
	if p.valueType == durationType {
		seconds, err := toSeconds(raw)
		if err != nil {
			return err
		}
		duration := time.Duration(seconds * float64(time.Second))
		if field.Kind() == reflect.Pointer {
			field.Set(reflect.ValueOf(&duration))
		} else {
			field.SetInt(int64(duration))
		}
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, field.Addr().Interface())
}

func toSeconds(raw any) (float64, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8, int16, int32, int64:
		return float64(reflect.ValueOf(v).Int()), nil
	case uint, uint8, uint16, uint32, uint64:
		return float64(reflect.ValueOf(v).Uint()), nil
	case json.Number:
		return v.Float64()
	}
	return 0, fmt.Errorf("%v is not a number of seconds", raw)
}

// parseDefault returns the value of the default tag of p. A string takes the text as it is, and any
// other type takes it as JSON.
func parseDefault(p param) (any, error) {
	if p.valueType.Kind() == reflect.String {
		return p.defaultText, nil
	}
	var value any
	if err := json.Unmarshal([]byte(p.defaultText), &value); err != nil {
		return nil, fmt.Errorf(
			"field %s: the default tag %q is not JSON: %w",
			p.field,
			p.defaultText,
			err,
		)
	}
	return value, nil
}

// isUnset reports whether the field of an arguments struct holds no value, so the task leaves the
// argument out.
func isUnset(value reflect.Value) bool {
	if templated, ok := value.Interface().(airflow.TemplatedArg); ok {
		_, _, set := templated.Unwrap()
		return !set
	}
	switch value.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
		return value.IsNil()
	}
	return false
}
