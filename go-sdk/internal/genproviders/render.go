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
	"encoding/json"
	"fmt"
	"go/format"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

const goImportRoot = "github.com/apache/airflow/go-sdk/providers/"

// initialisms are the words that Go spells in capitals in a name. They are the initialisms that
// staticcheck's ST1003 checks, which the repository's hooks run, and a few of AWS.
var initialisms = map[string]string{
	"acl": "ACL", "amqp": "AMQP", "api": "API", "arn": "ARN", "ascii": "ASCII", "cpu": "CPU",
	"css": "CSS", "db": "DB", "dns": "DNS", "eof": "EOF", "gid": "GID", "gpu": "GPU", "guid": "GUID",
	"html": "HTML", "http": "HTTP", "https": "HTTPS", "iam": "IAM", "id": "ID", "ip": "IP",
	"json": "JSON", "kms": "KMS", "qps": "QPS", "ram": "RAM", "rpc": "RPC", "rtp": "RTP", "sip": "SIP",
	"sla": "SLA", "smtp": "SMTP", "sql": "SQL", "ssh": "SSH", "tcp": "TCP", "tls": "TLS", "ttl": "TTL",
	"udp": "UDP", "ui": "UI", "uid": "UID", "uri": "URI", "url": "URL", "utf8": "UTF8", "uuid": "UUID",
	"vm": "VM", "vpc": "VPC", "xml": "XML", "xmpp": "XMPP", "xsrf": "XSRF", "xss": "XSS",
}

// goFieldName turns a Python argument name into an exported Go field name: s3_bucket is S3Bucket, and
// aws_conn_id is AwsConnID.
func goFieldName(name string) string {
	var out strings.Builder
	for _, part := range strings.Split(name, "_") {
		if part == "" {
			continue
		}
		if initialism, ok := initialisms[part]; ok {
			out.WriteString(initialism)
			continue
		}
		r, size := utf8.DecodeRuneInString(part)
		out.WriteRune(unicode.ToUpper(r))
		out.WriteString(part[size:])
	}
	return out.String()
}

// goType is the Go type of an argument of the operator spec.
type goType struct {
	expr string
	// scalar is true for a type whose zero value is a value, so that an optional argument of it is a
	// pointer.
	scalar bool
	// usesTime is true when the type holds a time.Duration or a time.Time, which need the time package.
	usesTime bool
}

func scalarType(expr string) goType { return goType{expr: expr, scalar: true} }

var anyType = goType{expr: "any"}

// mapType returns the Go type of a spec type.
func mapType(t specType) (goType, error) {
	switch t.Kind {
	case "string":
		return scalarType("string"), nil
	case "boolean":
		return scalarType("bool"), nil
	case "integer":
		return scalarType("int"), nil
	case "number":
		return scalarType("float64"), nil
	case "timedelta":
		return goType{expr: "time.Duration", scalar: true, usesTime: true}, nil
	case "datetime":
		return goType{expr: "time.Time", scalar: true, usesTime: true}, nil
	case "any", "null", "struct":
		return anyType, nil
	case "literal":
		return mapLiteral(t.Literals), nil
	case "array":
		items, err := mapType(*t.Items)
		return goType{expr: "[]" + items.expr, usesTime: items.usesTime}, err
	case "tuple":
		first, err := mapType(t.Tuple[0])
		if err != nil {
			return goType{}, err
		}
		for _, item := range t.Tuple[1:] {
			other, err := mapType(item)
			if err != nil {
				return goType{}, err
			}
			if other.expr != first.expr {
				return goType{expr: "[]any"}, nil
			}
		}
		return goType{expr: "[]" + first.expr, usesTime: first.usesTime}, nil
	case "object":
		values, err := mapType(*t.Values)
		return goType{expr: "map[string]" + values.expr, usesTime: values.usesTime}, err
	case "union":
		return mapUnion(t.Options)
	}
	return goType{}, fmt.Errorf("unknown type kind %q in an operator spec", t.Kind)
}

// mapLiteral returns string, int or bool when every literal is one, and any otherwise.
func mapLiteral(literals []any) goType {
	kinds := map[string]bool{}
	for _, literal := range literals {
		switch literal.(type) {
		case string:
			kinds["string"] = true
		case float64:
			kinds["int"] = true
		case bool:
			kinds["bool"] = true
		default:
			kinds["any"] = true
		}
	}
	if len(kinds) != 1 {
		return anyType
	}
	for kind := range kinds {
		if kind == "any" {
			return anyType
		}
		return scalarType(kind)
	}
	return anyType
}

// mapUnion returns the type that every option except null maps to, and any when they differ.
func mapUnion(options []specType) (goType, error) {
	var mapped []goType
	for _, option := range options {
		if option.Kind == "null" {
			continue
		}
		m, err := mapType(option)
		if err != nil {
			return goType{}, err
		}
		mapped = append(mapped, m)
	}
	if len(mapped) == 0 {
		return anyType, nil
	}
	for _, m := range mapped[1:] {
		if m.expr != mapped[0].expr {
			return anyType, nil
		}
	}
	return mapped[0], nil
}

// field is one argument of an operator, as a Go struct field.
type field struct {
	goName, pythonName string
	// typeExpr is the type of the field.
	typeExpr string
	required bool
	doc      string
	usesTime bool
}

func buildField(p param, op operator) (field, error) {
	base, err := mapType(p.Type)
	if err != nil {
		return field{}, fmt.Errorf("%s.%s: %w", op.Name, p.Name, err)
	}
	f := field{
		goName: goFieldName(
			p.Name,
		), pythonName: p.Name, required: p.Required, usesTime: base.usesTime,
		typeExpr: base.expr,
	}
	switch {
	case p.Templated:
		f.typeExpr = "airflow.Templated[" + base.expr + "]"
	case !p.Required && base.scalar:
		f.typeExpr = "*" + base.expr
	}
	var notes []string
	if base.expr == "time.Duration" {
		notes = append(notes, "A duration.")
	}
	if p.Type.Kind == "literal" {
		notes = append(notes, "One of "+listLiterals(p.Type.Literals)+".")
	}
	if p.Templated {
		notes = append(notes, "Templated.")
	}
	if len(p.Default) > 0 && string(p.Default) != "null" {
		notes = append(notes, "Defaults to `"+compactJSON(p.Default)+"`.")
	}
	description := strings.TrimSpace(p.Description)
	if description != "" && !strings.HasSuffix(description, ".") &&
		!strings.HasSuffix(description, ":") {
		description += "."
	}
	f.doc = strings.TrimSpace(strings.Join(append([]string{description}, notes...), " "))
	return f, nil
}

func listLiterals(literals []any) string {
	items := make([]string, len(literals))
	for i, literal := range literals {
		encoded, _ := json.Marshal(literal)
		items[i] = "`" + string(encoded) + "`"
	}
	return strings.Join(items, ", ")
}

func compactJSON(raw json.RawMessage) string {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return string(raw)
	}
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

// wrapComment wraps text into // lines that fit the width, after the indent.
func wrapComment(text, indent string) string {
	var lines []string
	line := ""
	limit := docWidth - len(indent) - 3
	for _, word := range strings.Fields(strings.ReplaceAll(text, "*/", "* /")) {
		if line != "" && len(line)+1+len(word) > limit {
			lines = append(lines, line)
			line = word
		} else if line == "" {
			line = word
		} else {
			line += " " + word
		}
	}
	if line != "" {
		lines = append(lines, line)
	}
	var out strings.Builder
	for _, l := range lines {
		out.WriteString(indent + "// " + l + "\n")
	}
	return out.String()
}

func wrapParagraphs(paragraphs []string, indent string) string {
	var out strings.Builder
	for i, paragraph := range paragraphs {
		if strings.TrimSpace(paragraph) == "" {
			continue
		}
		if i > 0 && out.Len() > 0 {
			out.WriteString(indent + "//\n")
		}
		out.WriteString(wrapComment(paragraph, indent))
	}
	return out.String()
}

func goStringSlice(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = fmt.Sprintf("%q", v)
	}
	return "[]string{" + strings.Join(quoted, ", ") + "}"
}

func renderOperator(op operator, prov provider) (string, bool, error) {
	fields := make([]field, 0, len(op.Params))
	seen := map[string]string{}
	usesTime := false
	for _, p := range op.Params {
		f, err := buildField(p, op)
		if err != nil {
			return "", false, err
		}
		if other, dup := seen[f.goName]; dup {
			return "", false, fmt.Errorf(
				"%s.%s: the arguments %q and %q both become %q",
				op.Module,
				op.Name,
				other,
				p.Name,
				f.goName,
			)
		}
		seen[f.goName] = p.Name
		usesTime = usesTime || f.usesTime
		fields = append(fields, f)
	}

	argsName := op.Name + "Args"
	var out strings.Builder
	out.WriteString(
		wrapComment(fmt.Sprintf("%s are the arguments of [%s].", argsName, op.Name), ""),
	)
	if len(fields) == 0 {
		fmt.Fprintf(&out, "type %s struct{}\n\n", argsName)
	} else {
		fmt.Fprintf(&out, "type %s struct {\n", argsName)
		for _, f := range fields {
			out.WriteString(wrapComment(f.goName+" "+lowerFirst(f.doc), "\t"))
			fmt.Fprintf(&out, "\t%s %s\n", f.goName, f.typeExpr)
		}
		out.WriteString("}\n\n")
	}

	omitted := make([]string, len(op.OmittedParams))
	for i, p := range op.OmittedParams {
		omitted[i] = "`" + p.Name + "`"
	}
	paragraphs := []string{
		op.Name + " adds a task that runs `" + op.Module + "." + op.Name + "` of `" + prov.Package +
			"` on a Python worker. " + strings.TrimSpace(op.Description),
	}
	if len(omitted) > 0 {
		paragraphs = append(
			paragraphs,
			"Not available from Go, because JSON cannot carry them: "+strings.Join(
				omitted,
				", ",
			)+".",
		)
	}
	out.WriteString(wrapParagraphs(paragraphs, ""))
	fmt.Fprintf(&out, "func %s(args %s) airflow.OperatorTask {\n", op.Name, argsName)
	fmt.Fprintf(
		&out,
		"\treturn airflow.UseOperator(\n\t\t%q,\n\t\t[]airflow.OperatorArg{\n",
		op.Module+"."+op.Name,
	)
	for _, f := range fields {
		call := "Arg"
		if f.required {
			call = "RequiredArg"
		}
		fmt.Fprintf(&out, "\t\t\tairflow.%s(%q, args.%s),\n", call, f.pythonName, f.goName)
	}
	out.WriteString("\t\t},\n\t\tairflow.OperatorInfo{\n")
	out.WriteString(renderInfo(op))
	out.WriteString("\t\t},\n\t)\n}\n")
	return out.String(), usesTime, nil
}

func lowerFirst(text string) string {
	if text == "" {
		return text
	}
	r, size := utf8.DecodeRuneInString(text)
	// Keep an acronym or a name as it is: only a capital that starts a word is lowered.
	rest := text[size:]
	if len(rest) > 0 && unicode.IsUpper([]rune(rest)[0]) {
		return text
	}
	return string(unicode.ToLower(r)) + rest
}

// renderInfo returns the OperatorInfo fields of an operator.
func renderInfo(op operator) string {
	var out strings.Builder
	if len(op.TemplateFields) > 0 {
		fmt.Fprintf(&out, "\t\t\tTemplateFields: %s,\n", goStringSlice(op.TemplateFields))
	}
	if len(op.TemplateExt) > 0 {
		fmt.Fprintf(&out, "\t\t\tTemplateExt: %s,\n", goStringSlice(op.TemplateExt))
	}
	if len(op.TemplateFieldsRenderers) > 0 {
		keys := make([]string, 0, len(op.TemplateFieldsRenderers))
		for key := range op.TemplateFieldsRenderers {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		entries := make([]string, len(keys))
		for i, key := range keys {
			entries[i] = fmt.Sprintf("%q: %q", key, op.TemplateFieldsRenderers[key])
		}
		fmt.Fprintf(
			&out,
			"\t\t\tTemplateFieldsRenderers: map[string]string{%s},\n",
			strings.Join(entries, ", "),
		)
	}
	if op.UIColor != "" {
		fmt.Fprintf(&out, "\t\t\tUIColor: %q,\n", op.UIColor)
	}
	if op.UIFgColor != "" {
		fmt.Fprintf(&out, "\t\t\tUIFgColor: %q,\n", op.UIFgColor)
	}
	if op.OperatorName != "" {
		fmt.Fprintf(&out, "\t\t\tOperatorName: %q,\n", op.OperatorName)
	}
	if op.Kind == "sensor" {
		out.WriteString("\t\t\tIsSensor: true,\n")
	}
	if op.CanSkipDownstream {
		out.WriteString("\t\t\tCanSkipDownstream: true,\n")
	}
	return out.String()
}

const licenseHeader = `// Licensed to the Apache Software Foundation (ASF) under one
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
`

// renderModule returns the Go source of the package for one Python module.
func renderModule(module string, operators []operator, prov provider, pkg string) (string, error) {
	var body strings.Builder
	usesTime := false
	for i, op := range operators {
		if i > 0 {
			body.WriteString("\n")
		}
		src, opUsesTime, err := renderOperator(op, prov)
		if err != nil {
			return "", err
		}
		usesTime = usesTime || opUsesTime
		body.WriteString(src)
	}
	var out strings.Builder
	// The marker has to be the first line, so that golines leaves the file as it is.
	fmt.Fprintf(
		&out,
		"// Code generated by go-sdk/internal/genproviders from %s. DO NOT EDIT.\n",
		specPath(prov.ID),
	)
	out.WriteString(licenseHeader + "\n")
	out.WriteString(wrapComment(fmt.Sprintf(
		"Package %s holds the Go wrappers of the operators of %s, from %s %s. Each one runs the "+
			"Python operator on a Python worker that has the provider installed.",
		pkg, module, prov.Package, prov.Version,
	), ""))
	fmt.Fprintf(&out, "package %s\n\nimport (\n", pkg)
	if usesTime {
		out.WriteString("\t\"time\"\n\n")
	}
	out.WriteString("\t\"github.com/apache/airflow/go-sdk/airflow\"\n)\n\n")
	out.WriteString(body.String())
	formatted, err := format.Source([]byte(out.String()))
	if err != nil {
		return "", fmt.Errorf(
			"%s: the generated code does not format: %w\n%s",
			module,
			err,
			out.String(),
		)
	}
	return string(formatted), nil
}

func specPath(providerID string) string {
	return "providers/" + strings.ReplaceAll(
		providerID,
		".",
		"/",
	) + "/_generated/lang_sdk_operators.json"
}

const mdLicense = `<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->
`

func renderReadme(s spec, modules []moduleEntry) string {
	prov := s.Provider
	slug := packageSlug(prov.ID)
	var out strings.Builder
	out.WriteString(mdLicense + "\n")
	fmt.Fprintf(&out, "# %s%s\n\n", goImportRoot, slug)
	fmt.Fprintf(
		&out,
		"The operators and sensors of `%s` %s, for a Dag written with the Go SDK.\n\n",
		prov.Package,
		prov.Version,
	)
	out.WriteString(
		"A wrapper has no task body. It records the Python operator class and its arguments in the\n",
	)
	out.WriteString(
		"Dag, and a Python worker that has the provider installed runs the operator. Such a task takes\n",
	)
	out.WriteString(
		"the Dag's queue like any other task: give it a `Queue` in its `TaskSpec` when the Dag's queue\n",
	)
	out.WriteString("is not served by Python workers.\n\n")
	if len(modules) > 0 {
		first := modules[0]
		example := first.operators[0]
		fmt.Fprintf(&out, "```go\nimport \"%s%s/%s\"\n\n", goImportRoot, slug, first.dir)
		fmt.Fprintf(
			&out,
			"dag.Task(\n    %s.%s(%s.%sArgs{ /* ... */ }),\n    airflow.TaskSpec{TaskID: \"my_task\", Queue: \"default\"},\n)\n```\n\n",
			lastSegment(first.dir),
			example.Name,
			lastSegment(first.dir),
			example.Name,
		)
	}
	out.WriteString(
		"A package mirrors the Python module of the same path, without the underscores, and an\n",
	)
	out.WriteString(
		"argument is the Python argument in Go case. An argument that is not required is a pointer\n",
	)
	out.WriteString(
		"or a slice, a map or `any`, so that `airflow.Ptr` sets it and nil leaves it out. A templated\n",
	)
	out.WriteString(
		"argument is an `airflow.Templated`: it takes a Jinja template, or the result of another task\n",
	)
	out.WriteString("with `airflow.ResultOf`. A duration is a `time.Duration`.\n\n")
	out.WriteString(
		"This package is generated. To change it, regenerate the operator specs and the packages:\n\n",
	)
	fmt.Fprintf(
		&out,
		"```bash\npython3 scripts/ci/lang_sdk_providers/generate_operator_specs.py --allow-list %s\n```\n\n",
		prov.ID,
	)
	out.WriteString("```bash\njust --justfile go-sdk/Justfile generate-providers\n```\n\n")
	out.WriteString("## Operators\n\n| Operator | Package |\n| --- | --- |\n")
	for _, m := range modules {
		for _, op := range m.operators {
			fmt.Fprintf(&out, "| `%s` | `%s%s/%s` |\n", op.Name, goImportRoot, slug, m.dir)
		}
	}
	out.WriteString("\n")
	var notAvailable []skipped
	for _, entry := range s.Skipped {
		if entry.Name != "" && entry.Reason != "base class" {
			notAvailable = append(notAvailable, entry)
		}
	}
	if len(notAvailable) > 0 {
		out.WriteString("## Not available\n\n| Operator | Reason |\n| --- | --- |\n")
		for _, entry := range notAvailable {
			fmt.Fprintf(
				&out,
				"| `%s` | %s |\n",
				entry.Name,
				strings.ReplaceAll(entry.Reason, "|", "\\|"),
			)
		}
		out.WriteString("\n")
	}
	return strings.TrimRight(out.String(), "\n") + "\n"
}

func lastSegment(dir string) string { return dir[strings.LastIndex(dir, "/")+1:] }
