<!--
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

# Draft: Loading Provider Operators in Go and Java

Extracted from [ADR-0018](0018-provider-operators-across-sdks.md) as a backup for a follow-up ADR. It assumes the generic runtime of ADR-0018, which the coordinator starts for a task whose `stub_kind` is `provider_operator`.

## Dynamic loading in Go and Java

The TypeScript SDK can load an operator it was not built with because Node imports modules at run time. Go and Java need the same three pieces, each in their own way:

1. **A generic runtime** that the coordinator starts for a `provider_operator` task.
2. **A way to load an implementation by `_operator_module`** at run time.
3. **An install location** that the administrator fills with provider packages and gives to the coordinator.

| | TypeScript | Go | Java |
| --- | --- | --- | --- |
| Loads code at run time | Yes, `import()` | No | Yes, class loaders |
| Generic runtime | Provider runner in `@apache-airflow/ts-sdk` | Open: a provider host the administrator builds, or a go-plugin host in the SDK | A runner main class in the SDK JAR |
| `_operator_module` | npm module specifier | Go import path, or a plugin executable name | Fully qualified class name |
| Administrator installs | `npm install --prefix <dir>` | `go build` of the host, or plugin executables into a directory | Provider JARs into a directory |
| Coordinator setting | `node_modules_path` | A path to the host, or to the plugin directory | A path to the JAR directory |

**Go.** Go links everything into one binary at build time, so Go needs a design of its own. There are three candidates.

- **The standard `plugin` package: rejected.** It works only on Linux and macOS, and a plugin must be built with the same toolchain and the same versions of every shared dependency as the program that loads it. Provider packages released on their own cadence cannot meet that.
- **A provider host built by the administrator.** The administrator builds a small `main` package that blank-imports the provider modules the deployment needs. Each module registers its implementations in `init()`, keyed by the Python class path, and the host serves an empty Dag bundle. Discovery is a map lookup inside one process. Adding a provider means rebuilding the host.
- **[hashicorp/go-plugin](https://github.com/hashicorp/go-plugin).** The Go SDK ships a generic host, and each provider ships its own plugin executable. The host starts the plugin as a subprocess and calls it over gRPC, so plugins and host can be built and released separately. Adding a provider means copying its executable into a directory.

With the provider host, a provider module and the host look like this:

```go
// Provider module: github.com/apache/airflow/go-providers/example/operators
func init() {
    provider.Register("airflow.providers.example.operators.text.GenerateTextOperator", generateText)
}

// Host, built by the administrator
package main

import (
    "github.com/apache/airflow/go-sdk/provider"
    _ "github.com/apache/airflow/go-providers/example/operators"
)

func main() { provider.ServeHost() }
```

With go-plugin, the provider serves its operators and the host dispenses them by the module name:

```go
// Provider executable
func main() {
    plugin.Serve(&plugin.ServeConfig{
        HandshakeConfig: provider.Handshake,
        Plugins: map[string]plugin.Plugin{
            "operators": &provider.OperatorsPlugin{Impl: provider.Operators{
                "airflow.providers.example.operators.text.GenerateTextOperator": generateText,
            }},
        },
        GRPCServer: plugin.DefaultGRPCServer,
    })
}

// Generic host in the Go SDK, after it reads TIRunContext.provider_operator
client := plugin.NewClient(&plugin.ClientConfig{
    HandshakeConfig:  provider.Handshake,
    Plugins:          map[string]plugin.Plugin{"operators": &provider.OperatorsPlugin{}},
    Cmd:              exec.Command(filepath.Join(pluginDir, path.Base(operator.Module))),
    AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
})
rpc, _ := client.Client()
raw, _ := rpc.Dispense("operators")
result, err := raw.(provider.Operators).Execute(ctx, operator.Classpath, operator.Kwargs)
```

`provider.Register`, `provider.ServeHost`, `provider.Handshake` and `provider.OperatorsPlugin` would be new Go SDK APIs; `plugin.*` is go-plugin's API. For go-plugin, discovery maps `_operator_module` to an executable in the configured directory, and the host can ask each plugin for the class paths it serves. The cost is a second process boundary: the operator's calls back to Airflow, such as reading a connection or pushing an XCom, must cross from the plugin to the host, which go-plugin supports through its gRPC broker. The provider host keeps everything in one process at the price of a rebuild per provider change. This ADR leaves the choice open.

**Java.** The JVM loads classes at run time, so Java can follow TypeScript closely. The SDK JAR gains a runner main class that serves an empty bundle. The administrator copies the provider JARs into a directory, for example with Maven's `dependency:copy`, and the coordinator adds that directory to the runner's classpath. `_operator_module` is the implementation's class name, which the runner loads with `Class.forName` and checks against the class path. `ServiceLoader` could list the installed implementations for error messages.

In both SDKs the arguments decode from the bindings into a typed struct or record, the same as for a Dag declared in that SDK. Neither is implemented yet, and the coordinator settings are named here only to show the shape.
