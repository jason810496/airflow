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
	"fmt"
	"reflect"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// ParamDescription describes one argument of a registered operator, as the operator spec records it.
type ParamDescription struct {
	// Field is the name of the field in the arguments struct.
	Field     string
	Name      string
	Required  bool
	Templated bool
	// Type is the language-neutral type, as lang_sdk_provider_operators.schema.json defines it.
	Type map[string]any
	// Default is the value that the operator applies, or nil when it has none.
	Default     any
	Description string
}

// Description describes a registered operator, as the operator spec records it.
type Description struct {
	Module               string
	Description          string
	ClassName            string
	ImplementationModule string
	// ArgsType is the name of the arguments struct.
	ArgsType                string
	IsSensor                bool
	TemplateFields          []string
	TemplateExt             []string
	TemplateFieldsRenderers map[string]string
	UIColor                 string
	UIFgColor               string
	OperatorName            string
	CanSkipDownstream       bool
	Params                  []ParamDescription
}

type describer interface{ describe() (Description, error) }

// Describe returns the operators that this binary registered, in registration order. A generator
// reads them to write the operator spec of the provider.
func Describe() ([]Description, error) {
	var described []Description
	for _, impl := range bundle.ListProviders() {
		d, ok := impl.(describer)
		if !ok {
			continue
		}
		description, err := d.describe()
		if err != nil {
			return nil, err
		}
		described = append(described, description)
	}
	return described, nil
}

func (i *implementation[A, R]) describe() (Description, error) {
	description := Description{
		Module:                  i.meta.Module,
		Description:             i.meta.Description,
		ClassName:               i.meta.ClassName,
		ImplementationModule:    i.meta.ImplementationModule,
		ArgsType:                i.argsType.Name(),
		IsSensor:                i.meta.IsSensor,
		TemplateFields:          i.TemplateFields(),
		TemplateExt:             i.meta.TemplateExt,
		TemplateFieldsRenderers: i.meta.TemplateFieldsRenderers,
		UIColor:                 i.meta.UIColor,
		UIFgColor:               i.meta.UIFgColor,
		OperatorName:            i.meta.OperatorName,
		CanSkipDownstream:       i.meta.CanSkipDownstream,
	}
	for _, p := range i.params {
		specType, err := p.describeType()
		if err != nil {
			return Description{}, fmt.Errorf("%s.%s: %w", i.meta.ClassName, p.field, err)
		}
		described := ParamDescription{
			Field: p.field, Name: p.name, Required: p.required, Templated: p.templated,
			Type: specType, Description: p.doc,
		}
		if p.defaultText != "" {
			if described.Default, err = parseDefault(p); err != nil {
				return Description{}, err
			}
		}
		description.Params = append(description.Params, described)
	}
	return description, nil
}

// describeType returns the language-neutral type of the argument.
func (p param) describeType() (map[string]any, error) {
	if len(p.enum) > 0 {
		if p.valueType.Kind() != reflect.String {
			return nil, fmt.Errorf("the enum tag is for a string, not %s", p.valueType)
		}
		literals := make([]any, len(p.enum))
		for n, value := range p.enum {
			literals[n] = value
		}
		return map[string]any{"kind": "literal", "literals": literals}, nil
	}
	return describeGoType(p.valueType)
}

func describeGoType(t reflect.Type) (map[string]any, error) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch {
	case t == durationType:
		return map[string]any{"kind": "timedelta"}, nil
	case t == timeType:
		return map[string]any{"kind": "datetime"}, nil
	}
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"kind": "string"}, nil
	case reflect.Bool:
		return map[string]any{"kind": "boolean"}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"kind": "integer"}, nil
	case reflect.Float32, reflect.Float64:
		return map[string]any{"kind": "number"}, nil
	case reflect.Interface:
		if t.NumMethod() == 0 {
			return map[string]any{"kind": "any"}, nil
		}
	case reflect.Slice:
		items, err := describeGoType(t.Elem())
		if err != nil {
			return nil, err
		}
		return map[string]any{"kind": "array", "items": items}, nil
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("a map needs string keys, not %s", t.Key())
		}
		values, err := describeGoType(t.Elem())
		if err != nil {
			return nil, err
		}
		return map[string]any{"kind": "object", "values": values}, nil
	}
	return nil, fmt.Errorf(
		"the type %s has no language-neutral type; use a string, a number, a bool, a slice, a map or any",
		t,
	)
}
