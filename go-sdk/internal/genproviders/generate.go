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
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"unicode"
)

const (
	supportedSpecMajor = "1"
	specScript         = "scripts/ci/lang_sdk_providers/generate_operator_specs.py"
	docWidth           = 96
)

// spec is the part of an operator spec that the Go wrappers read.
type spec struct {
	SpecVersion string   `json:"spec_version"`
	Provider    provider `json:"provider"`
	// ImplementedIn is empty for a provider implemented in Python.
	ImplementedIn []string   `json:"implemented_in"`
	Operators     []operator `json:"operators"`
	Skipped       []skipped  `json:"skipped"`
}

type provider struct {
	ID      string `json:"id"`
	Package string `json:"package"`
	Version string `json:"version"`
}

type skipped struct {
	Module string `json:"module"`
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type operator struct {
	Name                    string            `json:"name"`
	Module                  string            `json:"module"`
	Kind                    string            `json:"kind"`
	Description             string            `json:"description"`
	TemplateFields          []string          `json:"template_fields"`
	TemplateExt             []string          `json:"template_ext"`
	TemplateFieldsRenderers map[string]string `json:"template_fields_renderers"`
	UIColor                 string            `json:"ui_color"`
	UIFgColor               string            `json:"ui_fgcolor"`
	OperatorName            string            `json:"operator_name"`
	CanSkipDownstream       bool              `json:"can_skip_downstream"`
	Params                  []param           `json:"params"`
	OmittedParams           []omittedParam    `json:"omitted_params"`
}

type param struct {
	Name        string          `json:"name"`
	Type        specType        `json:"type"`
	Required    bool            `json:"required"`
	Templated   bool            `json:"templated"`
	Default     json.RawMessage `json:"default"`
	Description string          `json:"description"`
}

type omittedParam struct {
	Name string `json:"name"`
}

// specType is a language-neutral argument type of an operator spec.
type specType struct {
	Kind     string     `json:"kind"`
	Literals []any      `json:"literals"`
	Items    *specType  `json:"items"`
	Tuple    []specType `json:"-"`
	Values   *specType  `json:"values"`
	Options  []specType `json:"options"`
}

// UnmarshalJSON reads a type. A tuple holds a list of types in "items", where an array holds one.
func (t *specType) UnmarshalJSON(data []byte) error {
	var raw struct {
		Kind     string          `json:"kind"`
		Literals []any           `json:"literals"`
		Items    json.RawMessage `json:"items"`
		Values   *specType       `json:"values"`
		Options  []specType      `json:"options"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*t = specType{Kind: raw.Kind, Literals: raw.Literals, Values: raw.Values, Options: raw.Options}
	if len(raw.Items) == 0 {
		return nil
	}
	if raw.Kind == "tuple" {
		return json.Unmarshal(raw.Items, &t.Tuple)
	}
	t.Items = new(specType)
	return json.Unmarshal(raw.Items, t.Items)
}

// generate writes the wrapper packages of the providers in ids, or with check only returns the paths
// that differ. A provider implemented in a language SDK gets no package, and its directory is left
// alone. Paths are relative to outDir.
func generate(providersDir, outDir string, ids []string, check bool) ([]string, error) {
	var changed []string
	for _, id := range ids {
		specFile := filepath.Join(
			providersDir,
			filepath.FromSlash(strings.ReplaceAll(id, ".", "/")),
			"_generated",
			"lang_sdk_operators.json",
		)
		data, err := os.ReadFile(specFile)
		if err != nil {
			return nil, fmt.Errorf("no operator spec for %s; run %s: %w", id, specScript, err)
		}
		var s spec
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, fmt.Errorf("%s: %w", specFile, err)
		}
		if len(s.ImplementedIn) > 0 {
			return nil, fmt.Errorf(
				"%s is implemented in %v, so it has no generated Go wrapper",
				id,
				s.ImplementedIn,
			)
		}
		files, err := renderProvider(s)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", id, err)
		}
		providerDir := filepath.Join(outDir, packageSlug(s.Provider.ID))
		diff, err := writeFiles(providerDir, files, check)
		if err != nil {
			return nil, err
		}
		changed = append(changed, diff...)
	}
	return changed, nil
}

// packageSlug is the directory of a provider: common.ai is common-ai.
func packageSlug(providerID string) string { return strings.ReplaceAll(providerID, ".", "-") }

// writeFiles writes files, keyed by their path under dir, and removes the files under dir that are
// not among them. With check it only returns what would change.
func writeFiles(dir string, files map[string]string, check bool) ([]string, error) {
	var changed []string
	for path, content := range files {
		target := filepath.Join(dir, filepath.FromSlash(path))
		current, err := os.ReadFile(target)
		if err == nil && string(current) == content {
			continue
		}
		changed = append(changed, target)
		if check {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			return nil, err
		}
	}
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if _, ok := files[filepath.ToSlash(rel)]; !ok {
			changed = append(changed, path)
			if !check {
				return os.Remove(path)
			}
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	sort.Strings(changed)
	return changed, nil
}

// renderProvider returns the files of one provider, by their path under its directory.
func renderProvider(s spec) (map[string]string, error) {
	major, _, _ := strings.Cut(s.SpecVersion, ".")
	if major != supportedSpecMajor {
		return nil, fmt.Errorf(
			"the operator spec has version %s; this generator reads version %s.x",
			s.SpecVersion,
			supportedSpecMajor,
		)
	}
	byModule := map[string][]operator{}
	for _, op := range s.Operators {
		byModule[op.Module] = append(byModule[op.Module], op)
	}
	modules := make([]string, 0, len(byModule))
	for module := range byModule {
		modules = append(modules, module)
	}
	slices.Sort(modules)

	files := map[string]string{}
	var index []moduleEntry
	for _, module := range modules {
		dir, err := packagePath(module, s.Provider.ID)
		if err != nil {
			return nil, err
		}
		name := filepath.Base(dir)
		path := dir + "/" + name + ".go"
		if _, dup := files[path]; dup {
			return nil, fmt.Errorf("the modules %s and another one both become %s", module, path)
		}
		src, err := renderModule(module, byModule[module], s.Provider, name)
		if err != nil {
			return nil, err
		}
		files[path] = src
		index = append(index, moduleEntry{module: module, dir: dir, operators: byModule[module]})
	}
	files["README.md"] = renderReadme(s, index)
	return files, nil
}

type moduleEntry struct {
	module, dir string
	operators   []operator
}

// packagePath is the directory of the Go package of a Python module: aws.operators.s3 is
// aws/operators/s3, and latest_only is latestonly.
func packagePath(module, providerID string) (string, error) {
	prefix := "airflow.providers." + providerID + "."
	rest, ok := strings.CutPrefix(module, prefix)
	if !ok {
		return "", fmt.Errorf("the module %s is not part of the provider %s", module, providerID)
	}
	parts := strings.Split(rest, ".")
	for i, part := range parts {
		parts[i] = strings.ReplaceAll(part, "_", "")
	}
	if name := parts[len(parts)-1]; !isIdentifier(name) || isKeyword(name) {
		return "", fmt.Errorf(
			"the module %s becomes the package name %q, which Go cannot use",
			module,
			name,
		)
	}
	return strings.Join(parts, "/"), nil
}

func isIdentifier(name string) bool {
	for i, r := range name {
		if !unicode.IsLetter(r) && (i == 0 || !unicode.IsDigit(r)) {
			return false
		}
	}
	return name != ""
}

var keywords = []string{
	"break", "case", "chan", "const", "continue", "default", "defer", "else", "fallthrough", "for",
	"func", "go", "goto", "if", "import", "interface", "map", "package", "range", "return", "select",
	"struct", "switch", "type", "var",
}

func isKeyword(name string) bool { return slices.Contains(keywords, name) }
