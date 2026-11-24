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

# ADR-0016: Mapped Tasks Across Lang SDKs

## Status

Proposed.

Amended on 2026-11-25: the supervisor's Python sidecar resolves each instance's bindings on the worker, with the Python task runner's own code, instead of the API server at `ti_run`.
It reads the task from the worker's own parse, so the serialized Dag carries nothing for it beyond `_arg_bindings`.
Decisions 5 and 6 and the sections on what the Execution API sends and the read path changed, and the first version's design is under *Rejected alternatives*.

Depends on the supervisor running `[core] xcom_backend` in a per-task Python sidecar, added in [#75184](https://github.com/apache/airflow/pull/75184).

Amends [ADR-0007](0007-taskflow-across-language-boundary.md): it defines the bindings of a mapped stub task, and supports three forms 0007 rejects,
a `.map()`, `.zip()` or `.concat()` result as an expand argument, a mapped upstream's aggregated output, and a stub task with arguments inside a mapped task group.

## Terms

| Term | Means |
|---|---|
| Expansion | Turning one mapped task into N task instances while the Dag run is in flight. |
| Expand input | The serialized description of what a task expands over. One per mapped task. |
| Map length | How many instances an expansion produces. |
| Map index | An instance's position in its expansion, `0..N-1`. `-1` is the placeholder that has not expanded yet. |
| Region | The row that scopes one expansion, so the same mapped task expands independently per enclosing iteration. |
| Producer | The task whose output is expanded over. |
| Consumer | The mapped task itself. |
| Binding | One resolved call argument delivered to a runtime. |
| Reduce | Reading a mapped task's whole output from a downstream task, as a sequence rather than one value. |

## Decision

1. **Expansion stays in the scheduler for every language.** Map length, instance creation, regions, `REMOVED` handling and trigger rules are core's, not an SDK's.
   An SDK emits an expand input and consumes one value per map index; it never decides how many instances exist.
2. **The expand input keeps Python's `{__type, __var}` encoding.** An SDK emits the same form the Dag processor already writes for a Python mapped task, so core parses one format and no SDK invents a second.
3. **A Lang-SDK producer's map length is computed once in the supervisor, instead of in each SDK.** The server tells the supervisor whether the length is needed; the supervisor measures the return value and rejects one that is absent or not mappable.
   A Python task keeps measuring in its own runtime, so its `ti_run` still loads no Dag, and both apply the same `is_mappable_value` rule.
4. **Every Python mapping primitive is in scope, and the supervisor's Python sidecar resolves all of them.** A primitive an SDK has not implemented yet is a capability flag, not a core limit.
   For a Lang-SDK task, four shapes fail at parse time in both authoring modes: a `map()` that is not the outermost call of an expand argument, a `zip()` inside a `zip()`,
   a `map()`, `zip()` or `concat()` result passed whole, to an unmapped task or through `partial()` to a mapped one, and an argument that a task in a mapped task group takes from a task in a more deeply nested mapped group.
5. **A runtime never resolves the expand input itself; the supervisor's Python sidecar does, from the Dag the worker parses.** For a Python Dag the sidecar imports the Dag file and resolves the real task, as a Python task does.
   For a Dag definition the runtime sends the serialized Dag it already writes for the Dag processor, and the sidecar rebuilds the task from it.
   Either way the sidecar runs the Python task runner's code and reads values through the XCom backend.
   No SDK re-implements element selection or cross-product arithmetic, the server neither resolves nor reads an XCom value, and Python and Lang-SDK tasks share one implementation.
6. **Only a Dag definition's `map()` callables run outside the sidecar, applied on top of the element it picked.** Selection and transformation separate: [`_MapResult`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/task-sdk/src/airflow/sdk/definitions/xcom_arg.py#L398) indexes first, then applies the callables.
   A Python Dag's callables are Python, so the sidecar applies them while it resolves, as a Python task does.
   A Dag definition's callables are its runtime's, so the sidecar leaves the outermost `map()` to the runtime, which applies its own closures.
   No language reads another's callable.
7. **The map-index label is a runtime call, not a template.** `map_index_template` is Jinja and no other language renders it; an SDK sends `SetRenderedMapIndex` instead.
8. **Reduce is a lazy sequence, never a materialized value.** A downstream consumer reads length and elements on demand, as [`LazyXComSequence`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/task-sdk/src/airflow/sdk/execution_time/lazy_sequence.py#L64) does.

## Context

Mapped task behaviour spans four layers: the authoring API builds it, serialization carries it,
the scheduler materializes it, and the worker resolves one element. Python implements all four. An
SDK that copied that split would re-implement the scheduler's half in its own language, and the two
would diverge.

An SDK owns *what to expand over* and *what to do with one element*, and core owns *how many* and
*which*. The same split holds inside a task loop: a region scopes one expansion to one iteration,
so a mapped task in a loop expands once per iteration with no extra work in the SDK. Task loops
therefore need no mapping design of their own.

This ADR changes only the Lang-SDK path: a Dag definition, and a Python Dag with stub TaskHandler.
A Python task keeps resolving its arguments, measuring its map length and storing its XComs in its own runtime, as today.

## Who runs what

The coordinator only starts the SDK runtime subprocess; the supervisor handles every request that runtime sends.

| Process | Language | Its share of mapping |
|---|---|---|
| Scheduler | Python | Counts the expansion and turns the `-1` placeholder into N instances. |
| API server, Execution API | Python | Forwards a stub task's `_arg_bindings` at `ti_run`. Never resolves a binding or reads an XCom value. |
| Supervisor | Python, in the worker process | Measures a Lang-SDK producer's map length; for a Python Dag, has the sidecar resolve the bindings before the runtime starts; for a Dag definition, hands the runtime's `ResolveArgBindings` to the sidecar; forwards every XCom request. |
| Python sidecar | Python, one per Lang-SDK task instance, started on the first request that needs it | Resolves a mapped stub's bindings with the Python task runner's code, from the imported Dag file or from the serialized Dag the runtime sends. Runs `[core] xcom_backend`, so a runtime only sends and receives plain values. |
| SDK runtime | the SDK's own language | Runs the handler. For a Dag definition, sends its serialized Dag to have the bindings resolved and applies its own `map()` callables. Holds the reduce sequence. |

## Authoring interface

- Each SDK keeps Python's spelling as far as its argument binding allows. Where an SDK cannot, the
  substitute is the same in every SDK that needs it, never one per language.
- `expand` takes the arguments that vary per instance; `partial` takes the arguments that stay the same at every instance.
  An SDK that binds arguments by position cannot split them by keyword, so it marks each expanded
  argument in place instead, under one marker name shared by every such SDK, and every unmarked
  argument is a `partial` one.
- A mapped task's reference goes wherever a task reference goes on an edge, and into a later `expand`.
  Where the type system can tell it from an unmapped one, it has its own type, so passing it to an
  unmapped task type-checks as a reduce: only where the parameter is a sequence of its element type.
- `zip`, `concat` and `map` are methods on a reference, not free functions, so they chain in declaration order.
- A reduce consumer receives a sequence of the element type, not the element type.
- Expanding over a list of `T` gives each instance a `T`; expanding over a dict gives each instance a `[key, value]` pair, as a Python task gets `(key, value)`.

The running example, in the TypeScript SDK. Every later section follows this same `extract →
transform → report`.

```ts
async function extract(): Promise<string[]>; // ["uk ", " fr", "de "]
async function transform(args: { region: string; row: string }): Promise<number>;
async function report(args: { totals: XComSequence<number> }): Promise<void>;
```

```ts
const rows = dag.task(extract)();
const totals = dag.task(transform).partial({ region: "eu" }).expand({ row: rows });
dag.task(report)({ totals });
```

`transform` runs once per element of `extract`'s output and receives one `row`; `report` runs once
and receives every result, as decision 8's lazy sequence.

## What the Dag serialization carries

The expand input holds the items themselves for a literal, and only a reference for another task's
output. A literal therefore needs no producer: the scheduler creates every instance with the Dag
run.

```jsonc
// .expand({ row: ["uk ", " fr", "de "] })
"expand_input": {"type": "dict-of-lists", "value": {"__type": "dict", "__var": {
  "row": ["uk ", " fr", "de "]}}}

// .expand({ row: rows })
"expand_input": {"type": "dict-of-lists", "value": {"__type": "dict", "__var": {
  "row": {"__type": "xcomref", "__var": {"task_id": "extract", "key": "return_value"}}}}}

// .expand({ row: rows.map(clean) })
"expand_input": {"type": "dict-of-lists", "value": {"__type": "dict", "__var": {
  "row": {"__type": "xcomref", "__var": {
    "type": "map",
    "arg": {"task_id": "extract", "key": "return_value"},
    "callables": ["(row) => row.trim()"]}}}}}

// .expand({ row: rows.zip(lookup, { fillValue: "" }) })
"expand_input": {"type": "dict-of-lists", "value": {"__type": "dict", "__var": {
  "row": {"__type": "xcomref", "__var": {
    "type": "zip",
    "args": [{"task_id": "extract", "key": "return_value"},
             {"task_id": "lookup", "key": "return_value"}],
    "fillvalue": ""}}}}}
```

Two details an SDK emitter has to get right:

- The outer `type` is the expand input's: `expand()` writes `dict-of-lists`, and `expand_kwargs()` writes `list-of-dicts`, whose value can be an `xcomref`. The inner `type` is the XCom argument's, one of `map`, `zip` and `concat`; a plain argument omits it.
- Only the outermost argument is wrapped in `xcomref`. A nested one — `map`'s `arg`, `zip`'s and `concat`'s `args` — is the bare dict, because [`serialize_xcom_arg`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/task-sdk/src/airflow/sdk/definitions/xcom_arg.py#L590) recurses without re-wrapping.

A `map()` callable is stored as source text, one string per callable in call order: Python writes
`inspect.getsource(f)`, an SDK writes its own equivalent — `fn.toString()` in TypeScript. Core never
deserializes these strings; they exist so the UI can show the transform. Execution always re-enters
the real Dag, so no binding carries a callable or a reference to one.

### Stub-task arg bindings

Serialization drops `python_callable`, so a stub task's signature is lost, and a stub runtime never
parses the Dag file. `_arg_bindings` carries what resolution needs instead: one plain-JSON entry per
parameter, in declaration order, whose `kind` says how far parse time got. Three kinds leave the
resolver unchanged; `param` is the one the sidecar resolves, because that parameter's value differs
per map index.

| `kind` | Means | Other keys |
|---|---|---|
| `literal` | the value itself | `value`, `from_default` |
| `xcom` | an upstream task's output | `task_id` |
| `sequence` | a mapped upstream's whole output, read as a lazy sequence | `task_id` |
| `param` | the sidecar resolves it per map index | `default`, `group_arg` |
| `kwargs` | a Dag definition's own task only: this index's `expand_kwargs()` dict, which the runtime spreads | none |

```jsonc
// @task.stub def transform(region: str, row: str, scale: int = 2) -> int: ...
// transform.partial(region="eu").expand(row=rows)
"_arg_bindings": [
  {"kind": "param", "name": "region", "value_schema": {"type": "string"}},
  {"kind": "param", "name": "row", "value_schema": {"type": "string"}},
  {"kind": "param", "name": "scale", "default": 2,
   "value_schema": {"type": "integer", "format": "int64"}}
]
```

Every kind but `kwargs` carries `name` and `value_schema`: the parameter's name, and the JSON Schema of its
annotation for a runtime that decodes into machine types. Declaration order is load-bearing — an SDK
that binds by position, as Go does, has nothing else to go on.

A `param` entry has a `default` key exactly when the Dag author gives the parameter a default and
the call does not set it through `partial()` or `expand()`. `"default": null` is a default of `None`,
or its equivalent in the runtime's language, not a missing default: a parameter that defaults to
`None` is common in Python. So a writer emits `default` whenever the sidecar may read it, `null`
included, a reader tells the two cases apart by the key and not by its value, and anything that
reads and rewrites an entry keeps the key's presence. For a stub paired with a task handler, only
the stub's default counts. One declared only in the handler does not, even where the two are written
in the same language, because the stub is the signature the call is checked against. A Dag
definition's own task has no stub, so its handler is the signature and its defaults apply, which the
`kwargs` entry below relies on.

`expand_kwargs()` is the one form that needs every parameter's real name when the Dag is written,
because each instance's dict is matched against `name` by key. A stub always has them. A Dag
definition's own task may not: a TypeScript handler's argument type is erased, so a call whose dicts
come from an upstream names no keys. Such a task writes a single `kwargs` entry in place of a `param`
per expanded parameter. The sidecar still picks this index's dict, and the runtime spreads it into the
handler's arguments beside the `partial()` ones. A key the dict leaves out takes the handler's own
default; a key `partial()` also sets fails the instance, as Python's `mapping already partial` does;
and a key the handler does not take is passed on with the rest, since the runtime has no parameter
list to check it against. A runtime that binds by position cannot spread a dict either, so an SDK
that writes synthetic names, as Go writes `arg0`, `arg1`, … since a Go function keeps no parameter
names at run time, offers `expandKwargs` only for a handler whose names it can read, such as the
fields of a struct argument.

A `param` entry is the signature, not the call. The mapped values stay in the fields Python already
writes: `op_kwargs_expand_input`, a decorated task's name for `expand_input`, and
`partial_kwargs.op_kwargs`. The sidecar resolves the entry from `group_arg` first —
`{group_id, key}`, naming an expand argument of an enclosing mapped task group — then from those
fields by `name`, then from `default` if the entry has one; a parameter still unbound fails the
task. Only `expand_kwargs()` can leave a parameter unbound until run time, so only it needs that
fallback. A `kwargs` entry resolves from the same expand input to this index's dict, sent with
`spread` set.

Parse time emits `param` just twice: for a mapped stub's parameters, and for a group member's
argument that names its group. A Dag definition's own task called through `expand_kwargs()` writes
`param` for its `partial()` arguments and one `kwargs` entry, last, for the rest. A member's other arguments are ordinary `literal`, `xcom` or `sequence`
entries, and its unbound parameters take their defaults at parse time, because that
fallback does not vary by index.

A Python Dag's runtime never receives `param` or `kwargs`: the sidecar resolves them before the runtime starts.
A Dag definition's runtime does receive them, and sends `ResolveArgBindings` before it binds anything, getting back a `kwargs` entry as a binding with `spread`.
Nor is the kind in the Dag always the kind on the wire: an argument naming a task in
the same mapped group is `sequence`, that producer being expanded itself, and the sidecar narrows it
to this index's row. The runtime of a task the sidecar resolves never sees `xcom`: the sidecar reads it and sends a `literal`.

With no `_arg_bindings` at all, a stub task keeps its older behaviour of ignoring call arguments.

## What the Execution API sends

The server resolves nothing per map index, and sends no Dag structure. For a stub task, `ti_run` forwards its `_arg_bindings` entries as the Dag processor wrote them, `param` and `kwargs` included.

```python
class TIRunContext(BaseModel):
    ...
    arg_bindings: list[TaskArgBinding] | None = None
    """The task's ``_arg_bindings`` entries, as the Dag processor wrote them."""

    needs_mapped_length: bool | None = None
    """Set only for a Lang-SDK task: True when its return value is expanded over downstream, so the
    supervisor has to measure it. Derived from ``not ti.is_mapped and any(task.iter_mapped_dependants())``.
    None for a Python task, which measures its own."""
```

## What the sidecar resolves

The sidecar needs the task, its task groups and the tasks it reads. It gets them from the worker, never from the server:

| Authoring mode | Where the task comes from |
|---|---|
| Python Dag with stub TaskHandler | The sidecar imports the Dag file from the pinned bundle, as a Python task's runtime does, and takes the real task. The supervisor asks for it before the runtime starts. |
| Dag definition | The runtime sends `ResolveArgBindings` with its serialized Dag, the JSON it writes for the Dag processor. The sidecar rebuilds the task as Task SDK objects, with a placeholder operator for each upstream task and the task groups around them. |

The supervisor tells the two apart as the coordinator does when it launches the task: a file that a coordinator's Dag importer claims is a Dag definition.
The sidecar then runs the Python task runner's own resolution, `render_template_fields`, which resolves the expand input as it does for a Python task.
It reads values with the requests a Python task sends, `GetXCom`, `GetXComCount`, `GetXComSequenceItem` and `GetTICount`, and through the configured XCom backend.
An argument the sidecar cannot resolve, such as a missing key or an index past the end, fails the task with the error a Python task would raise.

### A Python Dag

The sidecar resolves the real task, `map()` callables included, so the runtime receives finished values in `TIRunContext.arg_bindings`: `literal`, or `sequence` for a reduce.
A callable that raises `AirflowSkipException` skips the instance before the runtime starts.

### A Dag definition

The runtime sends one request before it binds anything:

```python
class ResolveArgBindings(BaseModel):
    """Have the Python sidecar resolve a Dag definition's task arguments at this map index."""

    type: Literal["ResolveArgBindings"] = "ResolveArgBindings"

    dag: dict[str, JsonValue]
    """The serialized Dag, as the runtime writes it for the Dag processor."""


class ArgBindingsResult(BaseModel):
    type: Literal["ArgBindingsResult"] = "ArgBindingsResult"

    bindings: list[ResolvedLiteralArgBinding | XComSequenceArgBinding]
```

The callables are the runtime's, so the sidecar takes an outermost `map()` off before it resolves, and tells the runtime what is left to do on the binding:

```python
class ResolvedLiteralArgBinding(LiteralArgBinding):
    apply_map: bool = False
    """Run this argument's ``map()`` callables on the value (decision 6)."""

    kwarg: str | None = None
    """``expand_kwargs()`` only: take this key of the value, after any ``map()`` callables."""

    fallback: LiteralValue | None = None
    """The value when ``kwarg`` is missing: the ``partial()`` value or the signature default."""

    spread: bool = False
    """``expand_kwargs()`` only: the value, after any ``map()`` callables, is a dict the runtime
    spreads into the handler's arguments. ``name`` is then ``**``."""
```

| The resolved argument | The runtime receives |
|---|---|
| a value | `literal` |
| a mapped upstream's whole output, which Python resolves to a lazy sequence | `sequence`, unread |
| an element under an outermost `map()` | `literal` of the element before the callables, with `apply_map` |
| a key of an `expand_kwargs()` dict that `map()` builds | `literal` of the element, with `apply_map`, `kwarg` and `fallback` |
| this index's `expand_kwargs()` dict, for a `kwargs` entry | `literal` with `spread` |

`map()` is elementwise, so taking it off cannot move an index.
Python's `expand_kwargs()` needs a dict after the callables, so over a `map()` the sidecar resolves the wrapped element instead and leaves the key to the runtime, through `kwarg` and `fallback`.
A failure comes back as an `ErrorResponse`, and fails the task.

### Both modes

`zip` and `concat` need no binding kind of their own: the sidecar reads their element as a Python task does.
A dict's element is its `(key, value)` pair, which the runtime receives as a two-item array.
A stub task that is neither mapped nor in a mapped task group keeps its plain `literal` and `xcom` bindings, and its runtime reads an `xcom` one itself, as before.

`serialization/schema.json` also gains `expand_input` as a declared property, which exists today only as a `dependencies` entry, so
the key validates by accident.

Each change ships behind an Execution API version with a mirrored supervisor wire-schema version.

## Write path

```
 AUTHOR (SDK)                 DAG PROCESSOR          SCHEDULER                  PRODUCER'S SDK RUNTIME
 ────────────                 ─────────────          ─────────                  ──────────────────────
 .expand({ row: rows }) ──▶ serialize:                                          result = extract()
                              _is_mapped                                        SetXCom(return_value) ─┐
                              partial_kwargs                                                           │
                              expand_input     ──▶ literal items:           SUPERVISOR                 │
                                                     N instances at once      length needed? ◀─────────┘
                                                   reference:                   len(value) ──▶ xcom.mapped_length
                                                     one placeholder (-1)
                                                     ↓ deps met
                                                   get_task_map_length ──▶ (region, 0..N-1)
```

[`get_task_map_length`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/airflow-core/src/airflow/serialization/definitions/xcom_arg.py#L150)
reads `xcom.mapped_length` for an unmapped producer and counts rows for a mapped one. `zip` takes
the `min` of its arguments, or the `max` when a fill value is set; `concat` takes the sum; `map`
delegates to the argument it wraps, because it is elementwise and cannot change the count.

### Acting on `needs_mapped_length`

| Process | What it does |
|---|---|
| API server | Sets the flag at `ti_run` for a Lang-SDK task, off the serialized task it already loads for the bindings when `ti.is_stub`. A Python task's `ti_run` is unchanged and leaves it `None`. |
| Supervisor | On `SetXCom` for `return_value`, checks the value with `is_mappable_value` and passes `len(value)` as `mapped_length`, before the XCom backend stores it. A value that is not mappable, such as a string, fails the task. No `return_value` push by task end also fails it, since the consumer would otherwise be `UPSTREAM_FAILED` with no reason. Drops the field before forwarding the context, as it already does for `xcom_keys_to_clear`. |
| SDK runtime | Nothing. It never receives the flag and never sends a length. |
| Python task | Unchanged: its runtime measures in `_push_xcom_if_needed` before the value is stored. |

## Read path

The sidecar resolves every binding to this map index's value. Only where it gets the task, and who applies `map()` callables, differ by authoring mode.

```
 API SERVER     ti_run ──▶ TIRunContext{ ti.map_index, expanded_ti_count, arg_bindings }
                  │
 SUPERVISOR       ├─ Python Dag? ──▶ PYTHON SIDECAR: import the Dag file, resolve the real task,
                  │                  map() callables included ──▶ literal and sequence bindings
                  │
 SDK RUNTIME      ├─ Dag definition with param or kwargs bindings?
                  │     └─ ResolveArgBindings{ dag } ──▶ PYTHON SIDECAR: rebuild the task, resolve it
                  │        ◀── literal and sequence bindings, keeping apply_map
                  │        └─ apply_map? ──▶ the runtime applies its own callables
                  └─ transform({ region: "eu", row: … })
```

What the sidecar reads for each authoring form, and what the runtime receives.

| Authoring form | The sidecar reads | The runtime receives |
|---|---|---|
| `.expand({ row: ["a", "b"] })` | nothing: the element is in the expand input | `literal` |
| `.expand({ row: rows })` | `rows`, then element `i` | `literal`: element `i` of the row |
| `.expand({ row: mappedUpstream })` | row `i` | `literal`: that row |
| `.expand({ a: …, b: … })` | each argument's element, picked as Python picks a cross product | one `literal` per argument |
| `.partial({ region: … })` | the literal, or the upstream's whole value | the same value at every index |
| `.expandKwargs(rows)` | element `i`, then each parameter's key, else its fallback | one `literal` per parameter |
| `.expandKwargs(rows)`, from a `kwargs` entry | element `i` | `literal` with `spread` |
| `.zip(a, b)` | each part's element, the fill value where a part runs out | `literal`: the parts as one array |
| `.concat(a, b)` | the element of whichever argument owns this index | `literal` |
| `.map(f)` in a Python Dag | the element, then `f` of it | `literal` |
| `.map(f)` in a Dag definition | the element the `map()` wraps | `literal` with `apply_map` |
| Mapped task group | the member's value, taken through the innermost common mapped ancestor | `literal` |
| Reduce, in a downstream task | nothing | `sequence`, from which it builds a lazy handle and fetches nothing yet |

An element of a dict is its `(key, value)` pair in the stored value's order, the pair a Python task reading the same XCom gets.
A runtime receives it as a two-item array, such as `["uk", 3]`.
With `.expandKwargs(…)`, a parameter missing from this index's dict takes its `partial()` value or its default, and fails the task with neither, as a missing argument does in Python.
With `spread`, a key missing from the dict is left to the handler's own default.

### Acting on `expanded_ti_count`

| Process | What it does |
|---|---|
| API server | Counts the live instances in this instance's region, so inside a loop it counts one iteration. |
| Supervisor | Forwards it, and gives it to the sidecar for resolution. |
| SDK runtime | Exposes it to the handler read-only, so a handler can say "index 3 of 10". It is **not** used to resolve arguments. |

Python also uses it to pick which upstream map indexes a mapped-task-group member pulls. The sidecar
runs that same code for a Lang-SDK task, so no SDK implements it.

### map()

`map()` must be the outermost call of an expand argument, so its callables always apply to the whole picked value.
`rows.zip(lookup).map(f)` is allowed; `rows.map(f).zip(lookup)` and `a.map(f).concat(b)` fail at parse time.
Chained `map()` calls are one wrapper with several callables, applied in order.

**Case A: a Dag definition, so the callable is the SDK's.** The running example, with the rows trimmed on the way in:

```ts
const clean = (row: string) => row.trim();
dag.task(transform).partial({ region: "eu" }).expand({ row: rows.map(clean) });
```

```
   ti_run  ──▶  arg_bindings: [{ name: "region", kind: "param" }, { name: "row", kind: "param" }]
                     │
   SDK RUNTIME       ├─ param bindings: send ResolveArgBindings{ dag: <its serialized Dag> }
                     │
   PYTHON SIDECAR    ├─ rebuild transform, take map() off row, resolve it at index 2
                     │    GetXCom(extract)                   ──▶  ["uk ", " fr", "de "]
                     └─ ArgBindingsResult{ region: "eu", row: "de " with apply_map }
                     │
   SDK RUNTIME       ├─ find row's map() closures in its OWN Dag definition, by the binding's name
                     ├─ apply them in order                 ──▶  clean("de ") == "de"
                     └─ transform({ region: "eu", row: "de" })
```

Every SDK follows one order: the callables, then `kwarg`, then `fallback`.
With `.expandKwargs(rows.map(toKwargs))` the callables build each instance's dict, and each parameter takes its `kwarg` from it.
`toKwargs` runs once per instance: a runtime reuses the result across parameters.

**Case B: a Python Dag, so the callable is Python.** The same Dag, authored in Python with the body in TypeScript:

```python
# Dag file. The transform is Python; the task body is not.
def clean(row: str) -> str:
    return row.strip()


@task.stub(queue="typescript")
def transform(region: str, row: str) -> int: ...


transform.partial(region="eu").expand(row=extract().map(clean))
```

```ts
// TaskHandler. It sees a finished value and no map() at all.
export async function transform({ region, row }: { region: string; row: string }): Promise<number>;
```

```
   ti_run  ──▶  arg_bindings: [{ name: "region", kind: "param" }, { name: "row", kind: "param" }]
                     │
   SUPERVISOR        ├─ a Python Dag: hand the bindings to the Python sidecar before the runtime starts
                     │
   PYTHON SIDECAR    ├─ import the Dag file from the pinned bundle
                     ├─ resolve transform at index 2, as a Python task does
                     │    GetXCom(extract), then clean("de ") ──▶  "de"
                     └─ literal bindings: region "eu", row "de"
                     │
   SDK RUNTIME       └─ transform({ region: "eu", row: "de" })
```

Either way the handler receives a finished value, and no callable crosses the wire.
A `map()` callable may skip its instance, as a Python one does by raising `AirflowSkipException`.
A Python callable's skip skips the instance before the runtime starts; a runtime handles a skip from its own callables like a skip raised by the handler.
Any other error fails the task.

### Reduce

`report` is not mapped, so it reads every `transform` result instead of one.

1. The sidecar delivers a `sequence` binding naming `transform`. Nothing is fetched.
2. The runtime hands the handler a handle, not a value.
3. `length()` sends `GetXComCount`, which the supervisor turns into `HEAD /execution/xcoms/{dag}/{run}/{task}/{key}`.
4. `at(i)` sends `GetXComSequenceItem` → `GET …/{key}/item/{offset}`.
5. `slice(a, b)` sends `GetXComSequenceSlice` → `GET …/{key}/slice`.
6. The server decides which producer rows are live and orders them iteration first, then map index. The runtime sends only the offset it wants.
7. With a configured `[core] xcom_backend`, the supervisor's Python sidecar decodes each value on the way back.

### A handler's own read of a mapped producer

A handler can also read an upstream's XCom itself, through its SDK client's `getXCom`. A map index
means the same there in every SDK. The server decides whether the producer is mapped from its
serialized task, as it does for bindings, not from its rows, so a producer that expanded to nothing
still reads as mapped:

| Producer | Map index | The read returns |
|---|---|---|
| Not mapped | none | its value |
| Not mapped | any | an error |
| Mapped | none | every instance's value, as a list in map-index order; `[]` when it expanded to nothing |
| Mapped | `0 <= i <` instance count | instance `i`'s value |
| Mapped | any other, negative included | an error |

An error is not a missing value: a read in range with nothing pushed still returns null.
Unlike a reduce binding, the whole-output read is materialized, because the handler asked for the value.
It is an Execution API mode of its own; Python's `XCom.get_one`, where no map index still means the `-1` row, is unchanged.

### Python sidecar subprocess for the XCom backend

The supervisor runs `[core] xcom_backend` for a Lang-SDK task, so a runtime only sends and receives plain values.
The backend runs in the supervisor's Python sidecar, one process per task instance, started the way a Python task process is, on the first call the configured backend handles.
With the default backend no sidecar starts for XComs.
The sidecar reads connections and variables through the supervisor's API client, as a Python task does.
The supervisor measures a value before the backend stores it, and every value read back goes through the backend first, so nothing measures or indexes a storage path.
A Python task keeps running the backend in its own process.
Resolving a mapped stub's bindings is a second job for the same sidecar, which starts for it whatever the XCom backend.
Only for a Python Dag does it import the Dag file.

## Runtime steps

What every SDK implements, in order. Nothing here is language-specific.

1. Read `ti.map_index` and `ti_context.expanded_ti_count` from `StartupDetails`. A `map_index` of `-1` means the task is not mapped.
2. If a binding is `param` or `kwargs`, the task is a Dag definition's: send `ResolveArgBindings` with the serialized Dag, and use the bindings it returns. A stub TaskHandler never receives either kind.
3. Take each binding for this map index: a `literal`, or a `sequence` for a reduce. A kind the SDK does not implement fails the task before the handler runs.
4. For a binding with `apply_map`, apply the Dag definition's own callables. Then take `kwarg`, or `fallback` when the key is missing; with `spread`, merge the value's keys into the handler's arguments, failing the instance on one a `partial()` binding also sets. Run the callables once per instance, even when several parameters share them. A skip from them skips the instance.
5. Optionally send `SetRenderedMapIndex` to label the instance.
6. Run the handler with that one element.
7. Push the return value. The supervisor adds the map length; the SDK sends none.

## Consequences

- **A Python Dag's mapped stub task costs a Dag-file import per instance.** Every instance of a stub task that is mapped or in a mapped task group pays it, not only one that consumes a `map()`;
  any other stub TaskHandler keeps the no-parse path bindings were built to give it. The sidecar executes Dag-file Python on the worker for a task whose body is not Python: the same trust boundary as any Python task there, but a new one for a stub TaskHandler.
- **A worker running a Python Dag's mapped stub task needs the Dag file's Python dependencies.** The sidecar imports the Dag file, so every provider and library it imports must be installed on the workers serving that queue,
  which a Lang-SDK worker image did not need before. A missing one fails those instances at run time.
- **A Dag definition's mapped task serializes its Dag once per instance**, in memory and with no extra process, and sends it to the sidecar.
- **The sidecar resolves the Dag the worker runs.** The scheduler counts from the serialized Dag, and the sidecar resolves from the bundle version the task runs, the pairing a Python task already has.
- **`map()` callables are serialized as source text for display only.** Core deliberately does not deserialize them ([`SchedulerMapXComArg`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/airflow-core/src/airflow/serialization/definitions/xcom_arg.py#L105-L115)), so an SDK emits its own source the same way and no language has to read another's.
- **One resolution path serves Python and Lang-SDK tasks.** The sidecar runs the Python task runner's resolution, so a fix to it reaches every SDK.
  A conformance test resolves every authoring form from the Python Dag and from its serialized form, as a Dag definition sends it, and compares what each instance receives.
- **The sidecar starts for every Lang-SDK instance whose bindings differ per map index**, with the default XCom backend too.
  A stub that is neither mapped nor in a mapped task group does not start it.

## Rejected alternatives

**The API server resolves each instance's bindings at `ti_run`**, this ADR's first version. The server would send coordinates,
the row and element each binding reads, and the supervisor would read and narrow them.
It is a second implementation of Python's mapping arithmetic, in core, that has to track the Task SDK's:
the SDK's `concat()` length was fixed in [#77159](https://github.com/apache/airflow/pull/77159) on the SDK side only.
It also adds Dag and database work to `ti_run`, which [#60803](https://github.com/apache/airflow/pull/60803) moved out of it
for API server memory, by computing `upstream_map_indexes` in the Task SDK.

**A mapping spec stored in the serialized Dag**, this amendment's first draft. The Dag processor wrote the part of the serialized Dag resolution reads under each mapped stub task, and `ti_run` forwarded it.
It is a second schema for data the serialized Dag already holds, copied into every SDK's Dag schema, and it needed the keys of each expand input listed again, because MySQL's JSON column may return them sorted.

**`ti_run` slices the stored serialized Dag for the sidecar.** It adds no schema, but reads the whole serialized Dag on every mapped stub instance start, the load #60803 removed.

**Running a Dag definition's runtime twice, once to parse its Dag and once to run the task.** Each instance would pay a second process start and bundle load, when the runtime running the task already holds the Dag.

**`ApplyMap`: a stub TaskHandler asks the sidecar to run a Python Dag's callables after resolution.** It is a round trip per argument, and once the sidecar resolves from the imported Dag file it already has the callables.

**A second field beside `_arg_bindings` for the entries the sidecar resolves.** Splitting one list by
whether parse time could finish it gives a reader two shapes to learn, duplicates `name` and
`value_schema` across both, and leaves `value` meaning the delivered value in one and the unresolved
call argument in the other. A `param` kind says the same thing in one field, and the resolver already
has the discriminator it needs in `is_mapped` and `get_closest_mapped_task_group`. The cost is
widening `arg_binding.kind` on a field that already ships, which the serialized-Dag versioning work
covers.

**`{__type, __var}` encoding for those entries.** Nothing in a binding needs it. An XCom reference is
`kind` plus `task_id`, because a stub argument may only name an upstream task's `return_value`, so
there is no key to record. A `map()`, `zip()` or `concat()` expand argument is a
`param`, resolved from the expand input Python already writes.
Every `value` and `default` is checked JSON-serializable when it is written, so it crosses
the language boundary as it stands. Plain JSON also keeps the stored Dag readable, where the encoded
form wrapped every `value_schema` and every dict-valued `default` in a second object.

**An explicit flag beside `default` for whether a parameter has one.** It would let a writer leave
out a `None` default, where a typed model's natural spelling cannot tell an absent key from `null`.
The flag says nothing the key's presence does not, every SDK's JSON library can write a present
`null` with one idiom, and a second field admits entries where the two disagree.

**A Dag definition lists the keys of an `expand_kwargs()` input, with Dag-side defaults.** It
would repeat the handler's signature in the Dag for the one form that needs it, where a Dag
definition's handler already is the signature. A type checker can hold the list to the handler's
type, but not check that it is complete.

**Handler defaults for a stub paired with a task handler.** The call is checked against the stub, so
a default only the handler declares would let one Dag bind differently depending on which handler
serves the task.

**The server reads the XCom to pick an element.** Under an offloading XCom backend the row holds only a storage path,
and every `ti_run` of a mapped Lang-SDK task would read XCom values. The supervisor already reads through the backend.

**Each runtime picks its own element.** Every SDK would re-implement list indexing, dict pairs, `zip` assembly and the `expand_kwargs` split,
and Go cannot recover a JSON object's key order once it decodes one into a map.

**Recording where each `map()` sits in the argument tree.** A `map()` inside a `zip()` or `concat()` applies to one part only,
so the binding would carry a path into the tree and every runtime would walk it. Keeping `map()` outermost leaves one rule: apply the callables to the picked value.

## Related work, out of scope

- **Moving Python tasks onto the supervisor path.** A Python task could send plain values and let the supervisor measure, store and narrow them,
  as it does for a Lang-SDK task, so one implementation serves every runtime. It changes Python task behaviour, so it is a separate follow-up.
- **The shapes decision 4 rejects for a Lang-SDK task.** A `map()` inside a `zip()` or `concat()`, a `zip()` inside a `zip()`,
  and a `map()`, `zip()` or `concat()` result passed whole, to an unmapped task or through `partial()` to a mapped one. Each needs a binding that nests, or a resolver for the whole argument.
  An argument from a task in a more deeply nested mapped group reads a range of that producer's rows, and no binding carries a subset of a producer's rows.
