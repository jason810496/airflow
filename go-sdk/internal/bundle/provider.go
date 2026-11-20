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

package bundle

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/apache/airflow/go-sdk/sdk"
)

// ProviderParam describes one constructor argument of a provider operator.
type ProviderParam struct {
	// Name is the argument's name in the Python constructor.
	Name     string
	Required bool
}

// ProviderCall is what the runtime hands to the implementation of a provider operator.
type ProviderCall struct {
	// Args holds the arguments by their Python names. Template fields are rendered and results of
	// other tasks are pulled. An argument the task did not pass is absent.
	Args map[string]any
	// TaskInstance and DagRun describe the running task.
	TaskInstance sdk.TaskInstance
	DagRun       sdk.DagRun
	Client       sdk.Client
	Logger       *slog.Logger
	// SkipDownstream skips the direct downstream tasks of the running task. It does nothing when the
	// task has none.
	SkipDownstream func(ctx context.Context) error
}

// ProviderImpl runs a provider operator. The airflow/provider package builds one for each operator it
// defines, and the runtime finds it by class path.
type ProviderImpl interface {
	// Classpath is the Python class path of the operator, such as
	// airflow.providers.go.localfile.operators.file.CreateFileOperator.
	Classpath() string
	// TemplateFields lists the Python names of the arguments that Airflow renders as templates.
	TemplateFields() []string
	Params() []ProviderParam
	// Execute runs the operator and returns its result, which must be JSON.
	Execute(ctx context.Context, call ProviderCall) (any, error)
}

// ProviderLoader finds the implementation of an operator that this binary does not link, in the
// module the task names. The returned closer releases what Load started, once the task is over.
type ProviderLoader interface {
	Load(
		ctx context.Context,
		classpath, module string,
		logger *slog.Logger,
	) (ProviderImpl, io.Closer, error)
}

var providers = struct {
	mu     sync.RWMutex
	impls  map[string]ProviderImpl
	order  []string
	loader ProviderLoader
}{impls: map[string]ProviderImpl{}}

// RegisterProvider adds an operator that this binary links. It panics on a second operator with the
// same class path.
func RegisterProvider(impl ProviderImpl) {
	providers.mu.Lock()
	defer providers.mu.Unlock()

	classpath := impl.Classpath()
	if _, exists := providers.impls[classpath]; exists {
		panic(fmt.Sprintf("provider: operator %s is already registered", classpath))
	}
	providers.impls[classpath] = impl
	providers.order = append(providers.order, classpath)
}

// LookupProvider returns the operator that this binary links for classpath.
func LookupProvider(classpath string) (ProviderImpl, bool) {
	providers.mu.RLock()
	defer providers.mu.RUnlock()

	impl, ok := providers.impls[classpath]
	return impl, ok
}

// ListProviders returns the operators that this binary links, in registration order.
func ListProviders() []ProviderImpl {
	providers.mu.RLock()
	defer providers.mu.RUnlock()

	listed := make([]ProviderImpl, len(providers.order))
	for i, classpath := range providers.order {
		listed[i] = providers.impls[classpath]
	}
	return listed
}

// SetProviderLoader sets how the runtime loads an operator that this binary does not link. A
// generic runtime sets it before it serves.
func SetProviderLoader(loader ProviderLoader) {
	providers.mu.Lock()
	defer providers.mu.Unlock()

	providers.loader = loader
}

// GetProviderLoader returns what SetProviderLoader set, or nil.
func GetProviderLoader() ProviderLoader {
	providers.mu.RLock()
	defer providers.mu.RUnlock()

	return providers.loader
}
