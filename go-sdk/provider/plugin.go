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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/internal/providerrpc"
)

// Handshake is what a provider plugin and the runner that starts it must agree on. It makes a plugin
// refuse to run when someone starts it by hand, and the runner refuse an executable that is not a
// provider plugin.
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "AIRFLOW_GO_PROVIDER_PLUGIN",
	MagicCookieValue: "7a1b0c32-airflow-go-sdk-provider-operators",
}

// OperatorsPlugin is the go-plugin plugin that serves provider operators over net/rpc. A plugin
// executable passes the operators it implements, and the runner passes none.
type OperatorsPlugin struct {
	impls []bundle.ProviderImpl
}

var _ plugin.Plugin = (*OperatorsPlugin)(nil)

// Server returns the RPC server that runs the operators of this plugin.
func (p *OperatorsPlugin) Server(broker *plugin.MuxBroker) (any, error) {
	return providerrpc.NewServer(broker, p.impls), nil
}

// Client returns the client that the runner calls the plugin through.
func (p *OperatorsPlugin) Client(broker *plugin.MuxBroker, c *rpc.Client) (any, error) {
	return providerrpc.NewClient(broker, c), nil
}

// Serve serves the operators that the executable's imports registered with [Define], as a provider
// plugin. The generic runner starts the executable and calls them. Serve returns when the runner stops
// the plugin, and an executable started by hand fails with a message that says what it is.
//
//	func main() {
//		if err := provider.Serve(); err != nil {
//			log.Fatal(err)
//		}
//	}
func Serve() error {
	impls := bundle.ListProviders()
	if len(impls) == 0 {
		return errors.New("provider.Serve: no operator is registered; import the provider package")
	}
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: Handshake,
		Plugins:         plugin.PluginSet{providerrpc.PluginName: &OperatorsPlugin{impls: impls}},
	})
	return nil
}

// UsePlugins makes the runtime load an operator that its executable does not link from a plugin
// executable. The module of the task names the plugin, a file name in the directory that dir returns.
// dir is called for each task, so a command line flag that is parsed after UsePlugins can set it. The
// generic provider runner calls UsePlugins before it serves.
func UsePlugins(dir func() string) {
	bundle.SetProviderLoader(pluginLoader{dir: dir})
}

// pluginStartTimeout bounds how long the runner waits for a plugin to say that it is up.
const pluginStartTimeout = 30 * time.Second

type pluginLoader struct{ dir func() string }

var _ bundle.ProviderLoader = pluginLoader{}

// Load starts the plugin executable that module names and returns the operator classpath of it. The
// closer stops the plugin.
func (l pluginLoader) Load(
	ctx context.Context,
	classpath, module string,
	logger *slog.Logger,
) (bundle.ProviderImpl, io.Closer, error) {
	path, err := l.findExecutable(module)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot load %s: %w", classpath, err)
	}
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  Handshake,
		Plugins:          plugin.PluginSet{providerrpc.PluginName: &OperatorsPlugin{}},
		Cmd:              exec.CommandContext(ctx, path),
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolNetRPC},
		StartTimeout:     pluginStartTimeout,
		Logger: hclog.New(&hclog.LoggerOptions{
			Name:   "plugin",
			Level:  hclog.Debug,
			Output: &lineWriter{logger: logger, level: slog.LevelDebug},
		}),
		SyncStdout: &lineWriter{logger: logger, level: slog.LevelInfo},
		SyncStderr: &lineWriter{logger: logger, level: slog.LevelWarn},
	})
	closer := closerFunc(func() error { client.Kill(); return nil })

	rpcClient, err := client.Client()
	if err != nil {
		_ = closer.Close()
		return nil, nil, fmt.Errorf("cannot start the plugin %s for %s: %w", module, classpath, err)
	}
	raw, err := rpcClient.Dispense(providerrpc.PluginName)
	if err != nil {
		_ = closer.Close()
		return nil, nil, fmt.Errorf("cannot reach the plugin %s: %w", module, err)
	}
	impl, err := providerrpc.NewImpl(raw.(*providerrpc.Client), classpath)
	if err != nil {
		_ = closer.Close()
		return nil, nil, fmt.Errorf("the plugin %s cannot run %s: %w", module, classpath, err)
	}
	return impl, closer, nil
}

// findExecutable returns the path of the plugin executable that module names. The module comes from
// a serialized Dag, so it is a file name, never a path out of the plugin directory.
func (l pluginLoader) findExecutable(module string) (string, error) {
	dir := l.dir()
	if dir == "" {
		return "", errors.New(
			"no plugin directory is set: set go_providers_path of the Go coordinator, " +
				"which holds the plugins in its plugins directory",
		)
	}
	if module != filepath.Base(module) || module == "." || module == ".." ||
		strings.ContainsAny(module, `/\`) {
		return "", fmt.Errorf("the module %q is not a plugin file name", module)
	}
	path := filepath.Join(dir, module)
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "", fmt.Errorf("the plugin %s is not in %s; install it there", module, dir)
	case err != nil:
		return "", err
	case !info.Mode().IsRegular():
		return "", fmt.Errorf("%s is not a file", path)
	}
	return path, nil
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// lineWriter writes each line that a plugin prints to the task's log.
type lineWriter struct {
	logger *slog.Logger
	level  slog.Level
	mu     sync.Mutex
	buf    []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buf = append(w.buf, p...)
	for {
		end := bytes.IndexByte(w.buf, '\n')
		if end < 0 {
			return len(p), nil
		}
		if line := strings.TrimRight(string(w.buf[:end]), "\r"); line != "" {
			w.logger.Log(context.Background(), w.level, line, "source", "provider plugin")
		}
		w.buf = w.buf[end+1:]
	}
}
