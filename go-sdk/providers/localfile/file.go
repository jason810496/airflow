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

// Package localfile implements the operators of apache-airflow-providers-go-localfile, which create,
// write, check and delete local files, in Go.
//
// A Go Dag that links this package adds a task of an operator with [airflow.DagRef.Task], and the Go runtime
// runs it from the Dag's executable:
//
//	dag.Task(
//		localfile.WriteFileOperator(localfile.WriteFileOperatorArgs{
//			Path:    airflow.Of("/tmp/report.txt"),
//			Content: airflow.ResultOf[string](content),
//		}),
//		airflow.TaskSpec{TaskID: "write"},
//	)
//
// A Python Dag, or a Dag in another language SDK, uses the generated Python wrapper
// airflow.providers.go.localfile.operators.file instead, and routes the task to the Go coordinator
// with its queue. The coordinator then runs the generic provider runner, which loads the operators
// from the plugin executable in cmd/airflow-provider-go-localfile.
//
// ADR 0018 of the Lang SDK ADRs describes both ways.
package localfile

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/provider"
)

// Info identifies the provider. The operator spec and the Python wrapper are generated from it.
var Info = provider.Info{
	ID:      "go.localfile",
	Package: "apache-airflow-providers-go-localfile",
	Version: "1.0.0",
}

const (
	// module is the Python module of the operator classes.
	module = "airflow.providers.go.localfile.operators.file"
	// implementationModule is the plugin executable that serves the operators.
	implementationModule = "airflow-provider-go-localfile"
)

func orDefault[T any](value *T, fallback T) T {
	if value == nil {
		return fallback
	}
	return *value
}

// CreateFileOperatorArgs are the arguments of [CreateFileOperator].
type CreateFileOperatorArgs struct {
	Path airflow.Templated[string] `airflow:"path,required" doc:"The file to create. Missing parent directories are created too."`
	// ExistOk makes the task succeed when the file exists already.
	ExistOk *bool `airflow:"exist_ok"      doc:"Succeed when the file exists already. When false, an existing file fails the task." default:"true"`
}

// CreateFileResult is what [CreateFileOperator] returns.
type CreateFileResult struct {
	Path string `json:"path"`
	// Created is false when the file existed already.
	Created bool `json:"created"`
}

// CreateFileOperator adds a task that creates an empty file. An existing file is left as it is.
var CreateFileOperator = provider.Define(provider.Meta{
	Module:               module,
	ClassName:            "CreateFileOperator",
	ImplementationModule: implementationModule,
	Description:          "Create an empty file.",
}, func(_ airflow.Context, args CreateFileOperatorArgs) (CreateFileResult, error) {
	path := args.Path.Value()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return CreateFileResult{}, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	switch {
	case err == nil:
		return CreateFileResult{Path: path, Created: true}, file.Close()
	case errors.Is(err, fs.ErrExist) && orDefault(args.ExistOk, true):
		return CreateFileResult{Path: path}, nil
	case errors.Is(err, fs.ErrExist):
		return CreateFileResult{}, fmt.Errorf("%s exists already", path)
	}
	return CreateFileResult{}, err
})

// WriteMode says whether [WriteFileOperator] replaces what a file holds or adds to its end.
type WriteMode string

const (
	WriteModeOverwrite WriteMode = "overwrite"
	WriteModeAppend    WriteMode = "append"
)

// Encoding is how [WriteFileOperator] encodes text in a file.
type Encoding string

const (
	EncodingUTF8   Encoding = "utf-8"
	EncodingASCII  Encoding = "ascii"
	EncodingLatin1 Encoding = "latin1"
)

// WriteFileOperatorArgs are the arguments of [WriteFileOperator].
type WriteFileOperatorArgs struct {
	Path    airflow.Templated[string] `airflow:"path,required"    doc:"The file to write. Missing parent directories are created."`
	Content airflow.Templated[string] `airflow:"content,required" doc:"The text to write."`
	Mode    *WriteMode                `airflow:"mode"             doc:"Replace what the file holds, or add to its end."            enum:"overwrite,append"   default:"overwrite"`
	// Encoding is how the text is encoded in the file. A character that the encoding cannot hold fails
	// the task.
	Encoding *Encoding `airflow:"encoding"         doc:"How the text is encoded in the file."                       enum:"utf-8,ascii,latin1" default:"utf-8"`
}

// WriteFileResult is what [WriteFileOperator] and [WriteVariableOperator] return, read from the file
// after the write.
type WriteFileResult struct {
	Path string `json:"path"`
	// Bytes is the size of the whole file.
	Bytes int `json:"bytes"`
	// SHA256 is the SHA-256 of the whole file, in hex.
	SHA256 string `json:"sha256"`
}

// WriteFileOperator adds a task that writes text to a file. The result describes the file as it is on
// disk after the write.
var WriteFileOperator = provider.Define(provider.Meta{
	Module:               module,
	ClassName:            "WriteFileOperator",
	ImplementationModule: implementationModule,
	Description:          "Write text to a file.",
}, func(_ airflow.Context, args WriteFileOperatorArgs) (WriteFileResult, error) {
	return writeFile(
		args.Path.Value(), args.Content.Value(),
		orDefault(args.Mode, WriteModeOverwrite), orDefault(args.Encoding, EncodingUTF8),
	)
})

func writeFile(path, content string, mode WriteMode, encoding Encoding) (WriteFileResult, error) {
	data, err := encodeText(content, encoding)
	if err != nil {
		return WriteFileResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return WriteFileResult{}, err
	}
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if mode == WriteModeAppend {
		flags = os.O_WRONLY | os.O_CREATE | os.O_APPEND
	}
	file, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return WriteFileResult{}, err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return WriteFileResult{}, err
	}
	if err := file.Close(); err != nil {
		return WriteFileResult{}, err
	}
	written, err := os.ReadFile(path)
	if err != nil {
		return WriteFileResult{}, err
	}
	sum := sha256.Sum256(written)
	return WriteFileResult{Path: path, Bytes: len(written), SHA256: hex.EncodeToString(sum[:])}, nil
}

func encodeText(content string, encoding Encoding) ([]byte, error) {
	var limit rune
	switch encoding {
	case EncodingUTF8:
		return []byte(content), nil
	case EncodingASCII:
		limit = 0x7f
	case EncodingLatin1:
		limit = 0xff
	default:
		return nil, fmt.Errorf("unknown encoding %q", encoding)
	}
	data := make([]byte, 0, len(content))
	for _, r := range content {
		if r > limit {
			return nil, fmt.Errorf("the character %q cannot be encoded as %s", r, encoding)
		}
		data = append(data, byte(r))
	}
	return data, nil
}

// WriteVariableOperatorArgs are the arguments of [WriteVariableOperator].
type WriteVariableOperatorArgs struct {
	Path airflow.Templated[string] `airflow:"path,required" doc:"The file to write. Missing parent directories are created."`
	Key  airflow.Templated[string] `airflow:"key,required"  doc:"The key of the Airflow Variable whose value is written."`
}

// WriteVariableOperator adds a task that reads an Airflow Variable and writes its value to a file,
// replacing what the file holds. It reads the Variable through the client of the task's
// [airflow.Context], so it shows that an operator can call back to Airflow, whether it runs in the
// executable of a Go Dag or in a plugin. The result is that of [WriteFileOperator].
var WriteVariableOperator = provider.Define(provider.Meta{
	Module:               module,
	ClassName:            "WriteVariableOperator",
	ImplementationModule: implementationModule,
	Description:          "Write the value of an Airflow Variable to a file.",
}, func(actx airflow.Context, args WriteVariableOperatorArgs) (WriteFileResult, error) {
	actx.Logger().InfoContext(actx, "Writing a Variable to a file",
		"key", args.Key.Value(), "path", args.Path.Value())
	value, err := actx.Client().GetVariable(actx, args.Key.Value())
	if err != nil {
		return WriteFileResult{}, fmt.Errorf("reading the Variable %q: %w", args.Key.Value(), err)
	}
	return writeFile(args.Path.Value(), value, WriteModeOverwrite, EncodingUTF8)
})

// HasFileOperatorArgs are the arguments of [HasFileOperator].
type HasFileOperatorArgs struct {
	Path airflow.Templated[string] `airflow:"path,required"              doc:"The path to look for."`
	// SkipDownstreamIfMissing skips the direct downstream tasks when nothing is at the path.
	SkipDownstreamIfMissing *bool `airflow:"skip_downstream_if_missing" doc:"Skip the direct downstream tasks when nothing is at the path." default:"false"`
}

// HasFileOperator adds a task that checks that a file exists. The result is true when something is at
// the path, and false when not.
var HasFileOperator = provider.Define(provider.Meta{
	Module:               module,
	ClassName:            "HasFileOperator",
	ImplementationModule: implementationModule,
	Description:          "Check that a file exists.",
	CanSkipDownstream:    true,
}, func(actx airflow.Context, args HasFileOperatorArgs) (bool, error) {
	_, err := os.Stat(args.Path.Value())
	switch {
	case err == nil:
		return true, nil
	case !errors.Is(err, fs.ErrNotExist):
		return false, err
	}
	if orDefault(args.SkipDownstreamIfMissing, false) {
		if err := provider.SkipDownstream(actx); err != nil {
			return false, err
		}
	}
	return false, nil
})

// DeleteFileOperatorArgs are the arguments of [DeleteFileOperator].
type DeleteFileOperatorArgs struct {
	Path airflow.Templated[string] `airflow:"path,required" doc:"The file to delete."`
	// MissingOk makes the task succeed when the file is missing.
	MissingOk *bool `airflow:"missing_ok"    doc:"Succeed when the file is missing. When false, a missing file fails the task." default:"true"`
}

// DeleteFileResult is what [DeleteFileOperator] returns.
type DeleteFileResult struct {
	Path string `json:"path"`
	// Deleted is false when the file was missing.
	Deleted bool `json:"deleted"`
}

// DeleteFileOperator adds a task that deletes a file.
var DeleteFileOperator = provider.Define(provider.Meta{
	Module:               module,
	ClassName:            "DeleteFileOperator",
	ImplementationModule: implementationModule,
	Description:          "Delete a file.",
}, func(_ airflow.Context, args DeleteFileOperatorArgs) (DeleteFileResult, error) {
	path := args.Path.Value()
	err := os.Remove(path)
	switch {
	case err == nil:
		return DeleteFileResult{Path: path, Deleted: true}, nil
	case errors.Is(err, fs.ErrNotExist) && orDefault(args.MissingOk, true):
		return DeleteFileResult{Path: path}, nil
	case errors.Is(err, fs.ErrNotExist):
		return DeleteFileResult{}, fmt.Errorf("%s is missing", path)
	}
	return DeleteFileResult{}, err
})
