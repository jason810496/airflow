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
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// SpecVersion is the version of the operator spec format that [Info.RenderSpec] writes.
const SpecVersion = "1.0"

// Info identifies a provider, as its operator spec records it.
type Info struct {
	// ID is the provider id, such as go.localfile. The Python package of the wrapper lives in
	// providers/<id as a path>, and an operator's Module must start with airflow.providers.<ID>.
	ID string
	// Package is the Python distribution of the wrapper, such as apache-airflow-providers-go-localfile.
	Package string
	// Version is the version of the provider, which the wrapper packages follow.
	Version string
}

// SpecPath returns where the spec of the provider goes under the directory that holds the providers:
// go.localfile is go/localfile/_generated/lang_sdk_operators.json.
func (i Info) SpecPath() string {
	return strings.ReplaceAll(i.ID, ".", "/") + "/_generated/lang_sdk_operators.json"
}

// RenderSpec returns the operator spec of the operators that this binary registered for the provider,
// as providers/<id>/_generated/lang_sdk_operators.json holds it. Every SDK generates its wrapper of
// the provider from the spec, and the Python wrapper is generated from it too. The format is
// airflow-core/src/airflow/lang_sdk_provider_operators.schema.json. The text is the same for every
// run, so a test can compare it with the committed file.
func (i Info) RenderSpec() ([]byte, error) {
	described, err := Describe()
	if err != nil {
		return nil, err
	}
	prefix := "airflow.providers." + i.ID + "."
	var operators []Description
	for _, d := range described {
		if strings.HasPrefix(d.Module+".", prefix) {
			operators = append(operators, d)
		}
	}
	if len(operators) == 0 {
		return nil, fmt.Errorf(
			"no operator of the provider %s is registered; import its package",
			i.ID,
		)
	}
	slices.SortFunc(operators, func(a, b Description) int {
		return cmp.Or(cmp.Compare(a.Module, b.Module), cmp.Compare(a.ClassName, b.ClassName))
	})
	rendered := make([]any, len(operators))
	for n, operator := range operators {
		if operator.ImplementationModule == "" {
			return nil, fmt.Errorf(
				"%s.%s has no ImplementationModule",
				operator.Module,
				operator.ClassName,
			)
		}
		if rendered[n], err = specOperator(operator); err != nil {
			return nil, err
		}
	}
	spec := object{
		{"spec_version", SpecVersion},
		{"provider", object{{"id", i.ID}, {"package", i.Package}, {"version", i.Version}}},
		{"implemented_in", []any{"Go"}},
		{"operators", rendered},
		{"skipped", []any{}},
	}
	var out bytes.Buffer
	if err := writePretty(&out, spec, 0); err != nil {
		return nil, err
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

func specOperator(d Description) (any, error) {
	kind := "operator"
	if d.IsSensor {
		kind = "sensor"
	}
	operator := object{
		{"name", d.ClassName},
		{"module", d.Module},
		{"implementation_module", d.ImplementationModule},
		{"kind", kind},
	}
	if d.Description != "" {
		operator = append(operator, entry{"description", d.Description})
	}
	templateFields := make([]any, len(d.TemplateFields))
	for n, field := range d.TemplateFields {
		templateFields[n] = field
	}
	operator = append(operator, entry{"template_fields", templateFields})
	if len(d.TemplateExt) > 0 {
		ext := make([]any, len(d.TemplateExt))
		for n, e := range d.TemplateExt {
			ext[n] = e
		}
		operator = append(operator, entry{"template_ext", ext})
	}
	if len(d.TemplateFieldsRenderers) > 0 {
		var renderers object
		for _, field := range sortedKeys(d.TemplateFieldsRenderers) {
			renderers = append(renderers, entry{field, d.TemplateFieldsRenderers[field]})
		}
		operator = append(operator, entry{"template_fields_renderers", renderers})
	}
	for _, optional := range []entry{
		{"ui_color", d.UIColor}, {"ui_fgcolor", d.UIFgColor}, {"operator_name", d.OperatorName},
	} {
		if optional.value != "" {
			operator = append(operator, optional)
		}
	}
	if d.CanSkipDownstream {
		operator = append(operator, entry{"can_skip_downstream", true})
	}
	params := make([]any, len(d.Params))
	for n, p := range d.Params {
		param := object{{"name", p.Name}, {"type", typeObject(p.Type)}}
		if p.Required {
			param = append(param, entry{"required", true})
		}
		if p.Templated {
			param = append(param, entry{"templated", true})
		}
		if p.Default != nil {
			param = append(param, entry{"default", p.Default})
		}
		if p.Description != "" {
			param = append(param, entry{"description", p.Description})
		}
		params[n] = inline{param}
	}
	return append(operator, entry{"params", params}), nil
}

// typeObject returns a type of Description in the key order of the spec.
func typeObject(t map[string]any) object {
	order := []string{"kind", "literals", "items", "values"}
	var out object
	for _, key := range order {
		if value, ok := t[key]; ok {
			if nested, ok := value.(map[string]any); ok {
				value = typeObject(nested)
			}
			out = append(out, entry{key, value})
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// entry is one key of an object. An object keeps the order of its entries, which a Go map does not.
type entry struct {
	key   string
	value any
}

type object []entry

// inline is a value that writePretty writes on one line, as Python's json.dumps does.
type inline struct{ value any }

func writeString(out *bytes.Buffer, value any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return err
	}
	out.Write(bytes.TrimRight(buf.Bytes(), "\n"))
	return nil
}

func writeInline(out *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case object:
		out.WriteByte('{')
		for n, e := range v {
			if n > 0 {
				out.WriteString(", ")
			}
			if err := writeString(out, e.key); err != nil {
				return err
			}
			out.WriteString(": ")
			if err := writeInline(out, e.value); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	case []any:
		out.WriteByte('[')
		for n, item := range v {
			if n > 0 {
				out.WriteString(", ")
			}
			if err := writeInline(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case inline:
		return writeInline(out, v.value)
	default:
		return writeString(out, v)
	}
	return nil
}

func writePretty(out *bytes.Buffer, value any, depth int) error {
	pad := strings.Repeat("  ", depth+1)
	closing := strings.Repeat("  ", depth)
	switch v := value.(type) {
	case inline:
		return writeInline(out, v.value)
	case object:
		if len(v) == 0 {
			out.WriteString("{}")
			return nil
		}
		out.WriteString("{\n")
		for n, e := range v {
			out.WriteString(pad)
			if err := writeString(out, e.key); err != nil {
				return err
			}
			out.WriteString(": ")
			if err := writePretty(out, e.value, depth+1); err != nil {
				return err
			}
			if n < len(v)-1 {
				out.WriteByte(',')
			}
			out.WriteByte('\n')
		}
		out.WriteString(closing + "}")
	case []any:
		if len(v) == 0 {
			out.WriteString("[]")
			return nil
		}
		out.WriteString("[\n")
		for n, item := range v {
			out.WriteString(pad)
			if err := writePretty(out, item, depth+1); err != nil {
				return err
			}
			if n < len(v)-1 {
				out.WriteByte(',')
			}
			out.WriteByte('\n')
		}
		out.WriteString(closing + "]")
	default:
		return writeString(out, v)
	}
	return nil
}
