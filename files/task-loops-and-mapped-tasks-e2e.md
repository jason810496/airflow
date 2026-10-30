# Task loops and mapped tasks: end to end

This walkthrough follows both features from the Dag author's API to the database and back. It covers the authoring API, how a Dag is parsed and serialized, how the scheduler creates task instances, what the Execution API does, and what the Task SDK runtime does on the worker.

Line numbers refer to the checkout at `0f2563f2`.
Paths are shortened as follows:

| Prefix | Means |
|---|---|
| `sdk/` | `task-sdk/src/airflow/sdk/` |
| `core/` | `airflow-core/src/airflow/` |
| `exec_api/` | `core/api_fastapi/execution_api/` |
| `core_api/` | `core/api_fastapi/core_api/` |

---

## 0. One idea runs through both features: a region coordinate

Both loops and mapping create task instances while the Dag runs, not when it is parsed. This branch handles both with one coordinate system:

| Concept | Storage |
|---|---|
| A runtime-created "slot space" (one loop execution, or one mapped expansion) | row in `dynamic_region` (`core/models/dynamic_region.py:125`) |
| A task instance's position | `task_instance.region_id` + `task_instance.region_index` (`core/models/taskinstance.py:1119-1121`) |
| Legacy `map_index` | `map_index = synonym("region_index")` |
| Plain task | sentinel region `UUID(int=0)`, index `-1` |
| Loop body task | `(loop region, iteration)`; iterations count from **1** |
| Mapped task | `(its expansion region, map index)`; map indexes count from **0** (`-1` = not yet expanded placeholder) |

The unique keys now include `region_id`:
- `task_instance_current_key (dag_id, task_id, run_id, region_id, region_index, working_set)`
- the matching try key

The migrations are:
- `0143_3_4_0_add_dynamic_region_storage.py`
- `0144_3_4_0_rename_stored_map_index.py`
- `0136_3_4_0_fold_task_map_into_xcom_mapped_length.py`. This one removes the `task_map` table; a map length now lives on the XCom row as `mapped_length`.

Region tree for `example_task_loops.py::mapped_task_loop`. The loop body is `choose_items → process.expand(...) → gate`:

```
dag_run
└── dynamic_region L   node_id="process_batch"   (loop region, family_root=L, trajectory 1)
    │
    ├── iteration 1 ── choose_items           TI (L, 1)
    │                  process_batch.batch_ready (gate)  TI (L, 1)
    │                  └── dynamic_region M1  node_id="process_batch.process"  parent=(L, 1)
    │                        process  TI (M1,-1) ──expand──▶ (M1,0) (M1,1)
    │
    ├── iteration 2 ── choose_items (L, 2), gate (L, 2)
    │                  └── dynamic_region M2  parent=(L, 2)
    │                        process  (M2,0) (M2,1)
    └── ...
```

Because each iteration gets its own expansion region, a mapped task's count, `expanded_ti_count`, re-expansion and REMOVED handling are all scoped to one iteration. Iteration 1 / map 1 is `(M1,1)` and iteration 2 / map 1 is `(M2,1)`; they never collide.

---

## 1. The authoring API (the "user interface")

### 1.1 Loops

```python
from airflow.sdk import LoopDecision, dag, task, task_group

@dag(schedule=None)
def refine_estimate():
    @task_group
    def refine():
        @task
        def improve(*, loop):                       # `loop` must be keyword-only
            estimate = 1.0 if loop.index == 1 else loop.previous["estimate"]
            return (estimate + 2.0 / estimate) / 2.0

        @task
        def evaluate(estimate):                     # the single terminal task
            return {"estimate": estimate, "error": abs(estimate * estimate - 2.0)}

        evaluate(improve())

    def accurate_enough(*, loop):                   # becomes gate task "refine.accurate_enough"
        error = loop.result["error"]
        return LoopDecision(stop=error < 1e-6, reason=f"error {error:.2g}")

    refinement = refine.loop(max_iterations=10, until=accurate_enough)
    refinement >> finished()                        # waits for the *gate*, i.e. the whole loop
    report(refinement["evaluate"].output)           # gets LazyXComSequence of every iteration
```

These are the parts of the authoring API:

| API | Defined at | Notes |
|---|---|---|
| `@task_group` fn `.loop(*, max_iterations, until=None)` | `sdk/definitions/decorators/task_group.py:136` | Returns the `LoopTaskGroup`, not an XComArg. |
| `.partial(**kwargs)` / `.override(**tg_kwargs)` before `.loop()` | `task_group.py:153`, `:128` | `partial` supplies the body's arguments; `override` sets things like `group_id`. |
| `until(**context) -> bool \| LoopDecision` | evaluated in `sdk/execution_time/loop.py:99` | `True` means stop. It must be sync. It is **not serialized**: the worker re-parses the Dag file. |
| `LoopDecision(stop: bool, reason: str \| None)` | `sdk/definitions/loop.py:22` | The reason is at most 512 bytes of UTF-8, has no NUL, and is redacted by the secrets masker. |
| `loop` context: `index`, `max_iterations`, `previous`, `result`, `region_id`, `trajectory`, `forked_from_region_id`, `resumes_from_index` | `LoopContextAccessor`, `sdk/execution_time/loop.py:46-96` | Also exposed as `ti.loop`. The gate's decision is available as `ti.loop_decision`. |
| `refinement["task"].output` | ordinary `TaskGroup.__getitem__` + `BaseOperator.output` | Read from outside the loop it resolves to `LazyXComSequence`, ordered iteration first, then map index. |

The SDK rejects these cases at parse time:

| Rule | Where | Error |
|---|---|---|
| `max_iterations` must be an `int > 0` and not a `bool` | `sdk/definitions/_internal/loop.py:42` | validator |
| `until` must be callable and not async | `_internal/loop.py:186-191` | `TypeError` |
| The body has exactly one terminal (leaf) task | `_internal/loop.py:201-203` | `ValueError` |
| No nested loops; no loop inside a mapped task group | `_internal/loop.py:56-64` | `NotImplementedError` |
| A `loop` parameter must be keyword-only | `sdk/bases/decorator.py:343-347` | `ValueError` |
| `loop` cannot be `expand()`ed or `partial()`ed | `bases/decorator.py:590-599` | `ValueError` |
| A task outside the loop cannot `expand` over a loop task's output | `check_expansions_over_loop_tasks`, `_internal/loop.py:136` (called from `DAG.validate`, `sdk/definitions/dag.py:785`) | `ValueError` |
| A Dag result (`dag_result`) cannot be a loop member | `check_dag_result_outside_loop`, `_internal/loop.py:116` | `ValueError` |
| The XCom backend must implement `store_value` (keyed by `ti_id`) | `check_xcom_backend_supports_loops`, `sdk/execution_time/task_runner.py:1124` | `TypeError` at parse time and again at task start |

### 1.2 Mapped tasks

| API | Defined at | Builds |
|---|---|---|
| `Op.partial(**static).expand(**lists)` | `sdk/bases/operator.py:350`, `sdk/definitions/mappedoperator.py:196` | `OperatorPartial` → `MappedOperator(expand_input=DictOfListsExpandInput)` |
| `Op.partial(...).expand_kwargs(xcomarg_or_list_of_dicts, strict=True)` | `mappedoperator.py:205` | `ListOfDictsExpandInput` |
| `@task` fn `.partial(...).expand(...)` / `.expand_kwargs(...)` | `sdk/bases/decorator.py:753`, `:603`, `:622` | `DecoratedMappedOperator(op_kwargs_expand_input=...)`, returns `XComArg` |
| `@task_group` fn `.expand(...)` / `.expand_kwargs(...)` | `decorators/task_group.py:162`, `:179` | `MappedTaskGroup`; the function body receives `MappedArgument` stubs |
| `xcomarg.map(f)` / `.zip(*others, fillvalue=)` / `.concat(*others)` | `sdk/definitions/xcom_arg.py:172-178` | `MapXComArg` / `ZipXComArg` / `ConcatXComArg` |
| `map_index_template="{{ ... }}"` (partial kwarg) | stored in `partial_kwargs` | Rendered on the worker and shown in the UI as the instance label |
| A downstream consumer of a mapped output | `PlainXComArg.resolve`, `xcom_arg.py:333` | `LazyXComSequence` (the "reduce" step) |

The SDK rejects these cases:
- Expanding over a non-mappable literal (`validate_mapping_kwargs`, `mappedoperator.py:76`).
- Mapping over a custom XCom key (`ensure_xcomarg_return_value`, `:117`).
- A mapped operator inside a mapped task group (`MappedOperator.__attrs_post_init__`, `:348`).
- `trigger_rule="always"` combined with an XComArg expand input.
- Mapping context keys.

### 1.3 Combining the two

- **Supported:** mapped tasks inside a loop iteration, including a mapped *terminal* task. In that case `loop.result` and `loop.previous` are `LazyXComSequence`s.
- **Not supported:** mapping a whole loop, nested loops, or a loop inside a mapped task group.
- **Zero-length expansion:** a zero-length expansion of the terminal task skips it. Under `all_success` the gate is then skipped and no next iteration is created.

### 1.4 What `.loop()` builds (the Dag shape the scheduler sees)

```
             ┌──────────────── LoopTaskGroup "refine" (max_iterations=10, has_until=True) ───┐
 upstream ──▶│ improve ──▶ evaluate (terminal) ──▶ refine.accurate_enough  (LoopGateOperator)  │──▶ finished
             └───────────────────────────────────────────────────────────────────────────────────┘
                                                    ▲
       every edge from a body task to an outside task is rewired so it leaves from the gate
```

There is no back edge, so the graph stays acyclic. The "loop" exists only at run time: the gate creates the next iteration's task instances.

---

## 2. End-to-end path (both features)

```
 AUTHOR (Dag file)                 DAG PROCESSOR                         METADATA DB
 ─────────────────                 ─────────────                         ───────────
 tg.loop(...) / op.expand(...) ──▶ parse → LoopTaskGroup + LoopGateOperator
                                          MappedOperator / MappedTaskGroup
                                   DAG.validate()  (loop/mapping checks)
                                   serialize: task_group["loop"] = {max_iterations, has_until,
                                              terminal_task_id, gate_task_id}
                                              operator["_is_mapped"], expand_input  ──────────▶ serialized_dag

 SCHEDULER
   create_dagrun → verify_integrity → _create_initial_tasks
       · loop:   DynamicRegion L, every body task at (L, 1)
       · mapped: DynamicRegion M, placeholder (M, -1) [literal input → (M,0..N-1) at once]  ──▶ task_instance
   scheduling loop: task_instance_scheduling_decisions → _get_ready_tis
       · placeholder ready → ti.expand_mapped_task → (M,0..N-1) | SKIPPED | UPSTREAM_FAILED
       · ready TIs → executor

 WORKER  (supervisor ⇄ task process over comms; supervisor ⇄ API server over HTTP)
   supervisor: PATCH /execution/task-instances/{id}/run  ──▶ TIRunContext{loop, expanded_ti_count}
   task process:
       · mapped TI : render → ExpandInput.resolve(map_index) → unmap() → execute
       · body task : loop.previous → GetXCom(previous_iteration=True) → GET /execution/xcoms/...
       · producer  : SetXCom(mapped_length=len(value)) → POST /execution/xcoms/... (max_map_length check)
       · gate      : execute_loop_gate → until(**ctx) → SucceedTask(loop_decision) / TaskState(FAILED, loop_decision)
   supervisor: PATCH /execution/task-instances/{id}/state

 API SERVER (Execution API)
   ti_update_state (gate) → lock DagRun + gate → DagRun.complete_loop_gate
       · validate_loop_decision · store reason XCom · if continue:
         _create_tasks(body, parent_region=(successor(L), k+1))      (same transaction)  ──▶ task_instance
   … the scheduler picks the new iteration up like any other set of TIs …
```

---

## 3. Task loops: call chains

### 3.1 Authoring → Dag object (`sdk/definitions/_internal/loop.py`)

```
_TaskGroupFactory.loop(max_iterations, until)                          decorators/task_group.py:136
└─ create_loop(factory, max_iterations, until)                         _internal/loop.py:176
   ├─ LoopTaskGroup(**factory.tg_kwargs, max_iterations, has_until)    :192   (nesting check :56)
   ├─ with group: factory.function(**factory.partial_kwargs)           :199   body tasks built
   ├─ terminals = group.get_leaves(); must be exactly one              :201
   ├─ gate_name = until.__name__ if identifier else "__loop_gate"      :205
   ├─ gate = LoopGateOperator(task_id=get_unique_task_id(gate_name), until=until, doc_md=<until docstring>)  :208
   ├─ terminal >> gate; group.terminal_task_id / gate_task_id = ...    :213
   └─ redirect_outside_edges_to_gate(group, gate)                      :102   edges made while building the body
```

Edges created later (`>>`, `set_downstream`, XComArg arguments) go through `DependencyMixin._set_relatives` (`_internal/node.py:87-147`). That calls `get_edge_source(source, target)` (`_internal/loop.py:85`). If `source` is in a loop and `target` is not in the same loop, the edge is attached to the **gate** instead.

As a result, `report(refinement["evaluate"].output)` depends on the gate, so it runs only after the last iteration.

### 3.2 Serialization

- `TaskGroupSerialization.serialize_task_group` adds `encoded["loop"] = {...}` (`core/serialization/serialized_objects.py:2253-2259`). The JSON schema for it is at `serialization/schema.json:414-424`.
- `deserialize_task_group` → `SerializedLoopTaskGroup(max_iterations, has_until, terminal_task_id, gate_task_id)` (`core/serialization/definitions/taskgroup.py:352`).
- The gate is serialized as a plain operator. Core recognizes it by `ti.operator == "LoopGateOperator"` (`LOOP_GATE_OPERATOR`, `core/models/task_coordinates.py:60`).
- `until` is never stored. Only the worker, which re-parses the Dag file, can run it.

### 3.3 Iteration 1 is created with the Dag run

```
SerializedDAG.create_dagrun → _create_orm_dagrun → run.verify_integrity      serialization/definitions/dag.py:614,1436,1505
└─ DagRun.verify_integrity                                                    core/models/dagrun.py:2166
   └─ _create_initial_tasks                                                   :2532
      ├─ ordinary tasks → _create_tasks(expand_literals=True)
      └─ per loop: no live gate yet → DynamicRegion.get_or_create(node_id=loop) :2593
                   coordinates = [(region.id, 1)]                             :2597
                   _create_tasks(members, parent_region=(L, 1))               :2601
                     · unmapped member → (L, 1)
                     · mapped member   → child region M (parent=(L,1)), placeholder (M,-1)   :2627-2642
         _create_task_instances (bulk insert)                                 :2647
```

### 3.4 A body task runs and reads `loop.previous`

```
supervisor.start → client.task_instances.start → PATCH /execution/task-instances/{id}/run   sdk/api/client.py:259
   server ti_run: context.loop = TaskCoordinateResolver.get_loop_context(ti)               exec_api/routes/task_instances.py:376-382
                  context.expanded_ti_count = live TIs in the same region                  core/models/task_coordinates.py:294,344
supervisor → StartupDetails(ti, ti_context) → task process                                 sdk/execution_time/supervisor.py:1766
task_runner.parse → RuntimeTaskInstance(_ti_context_from_server=...)                       task_runner.py:1215
RuntimeTaskInstance.loop → LoopContextAccessor(ti_context.loop, ti)                        task_runner.py:271
get_template_context: context["loop"] = ...  → injected into the keyword-only `loop` argument   :386

loop.previous  (index==1 → None, no call)                                                  execution_time/loop.py:75
└─ _result(previous_iteration=True)                                                        :84
   ├─ terminal unmapped: XCom.get_one(..., _previous_iteration=True)                       sdk/bases/xcom.py:477
   │    → comms GetXCom(previous_iteration=True) → handle_get_xcom → XComOperations.get
   │    → GET /execution/xcoms/{dag}/{run}/{terminal}/return_value?previous_iteration=true
   └─ terminal mapped:   LazyXComSequence(PlainXComArg(terminal), previous_iteration=True)
        → GetXComCount / GetXComSequenceItem / GetXComSequenceSlice → HEAD / GET .../item/{i} / .../slice

server: _build_xcom_read(caller = TI from JWT token.id)                                    exec_api/routes/xcoms.py:180
└─ TaskCoordinateResolver.select_producer_ids(caller, previous_iteration)                  core/models/task_coordinates.py:762
   └─ _build_candidate_query: loop_position(caller) → (family root, k); k-1 for previous   core/models/dynamic_region.py:731
      build_loop_family_legs(family, k-1): finds iteration k-1 even in an earlier trajectory region   :590
```

The request never carries an iteration number. The server works out "my iteration" from the caller's token (`token.id`) and its `(region_id, region_index)`. The only wire flag is the boolean `previous_iteration`.

### 3.5 The gate runs `until`

```
_run_task_and_map_outcome → _execute_task → LoopGateOperator.execute                       task_runner.py:1615,2365; _internal/loop.py:169
└─ execute_loop_gate(gate, context)                                                       execution_time/loop.py:99
   ├─ no until:  stop = loop.index >= loop.max_iterations                                 :103
   ├─ until:     result = until(**determine_kwargs(until, (), context))                   :106
   │             match bool | LoopDecision(stop=bool, reason=str|None)  else AirflowFailException  :107-122
   ├─ reason: redact() → UTF-8 → ≤512 bytes → no NUL   else AirflowFailException          :123-136
   ├─ ti._loop_decision = LoopDecisionPayload(stop, reason)                               :137
   └─ not stop and index >= max_iterations → raise LoopMaxIterationsExceeded              :139   (non-convergence)
```

How the outcome is reported:

| Outcome | Comms message | HTTP call |
|---|---|---|
| Success | `SucceedTask(loop_decision=...)` (`task_runner.py:1869`) | `client.task_instances.succeed` → `PATCH /execution/task-instances/{id}/state` with `TISuccessStatePayload.loop_decision` |
| Non-convergence or invalid decision | `AirflowFailException` branch (`task_runner.py:1729`) → `TaskState(FAILED, loop_decision=...)` | `PATCH .../state` with `TITerminalStatePayload.loop_decision` |

Invalid decisions and non-convergence never retry: `AirflowFailException` skips the retry path, because a retry would see the same result. An ordinary exception raised inside `until` does retry, according to `retries`.

### 3.6 The server applies the decision and creates the next iteration

The scheduler loop does not create the next iteration. The Execution API request that reports the gate's state does, in one transaction:

```
ti_update_state                                                        exec_api/routes/task_instances.py:459
├─ gate? (TI.operator == LoopGateOperator)                             :518-525
├─ lock DagRun FOR UPDATE, then the gate TI                            :527-535
├─ resolver.loop_context(gate)  (gate missing in pinned Dag → non-retry failure; mismatch → 409)   :538-552
├─ SUCCESS: dag_run.complete_loop_gate(gate, group, SUCCESS, stop, reason)   :657-672
│     InvalidLoopDecision → _build_rejected_gate_payload → UP_FOR_RETRY / FAILED   :673-679, :803
├─ other terminal: complete_loop_gate(..., stop=None, reason)  (records the reason only)   :757-768
└─ commit                                                              :797

DagRun.complete_loop_gate                                              core/models/dagrun.py:2480
├─ gate.has_later_loop_pass(group) → return   (idempotent against stale or duplicate reports)
├─ validate_loop_decision(stop, region_index, group)                   :157
│     · decision required · cannot continue at the cap · fixed-count must stop exactly at the cap
├─ reason → XCom "_airflow_loop_decision_reason" on the gate's try
├─ state != SUCCESS or stop → return
└─ _create_tasks(group.iter_tasks(),
                 parent_region=(gate.get_loop_successor_region_id(), gate.region_index + 1))
   _create_task_instances(..., propagate_errors=True)
```

Mapped body tasks in iteration k+1 get a fresh child region and a `-1` placeholder. This call does not pass `expand_literals`, so the scheduler expands them later.

### 3.7 Tasks outside the loop

- **Dependencies:**
  - `resolve_dependency` (`core/models/task_coordinates.py:597`): when the upstream is a loop's gate and the caller is outside that loop, only the **live gate with the highest `region_index`** counts.
  - Because the next iteration's gate is inserted in the same transaction that marks the current gate SUCCESS, an outside `all_success` downstream keeps seeing an unfinished gate until the loop stops.
  - The trigger-rule code that filters upstreams by region is in `ti_deps/deps/trigger_rule_dep.py:143-148,245,276-284` and `ti_deps/dep_context.py:119-173`.
- **Reading a loop task's output from outside:**
  - `is_loop_task_read_from_outside` (`_internal/loop.py:76`) makes it a `LazyXComSequence`. On the server, `all_iterations=True` → `select_loop_producer_ids` (`dynamic_region.py:952`) returns every live pass and drops passes that a later trajectory replaced.
  - Results are ordered iteration first, then map index (`build_loop_sequence_order`, `dynamic_region.py:991`).
- **`ti.get_previous_ti()` inside a loop:** returns the same iteration in the closest earlier run (`exec_api/routes/task_instances.py:1617-1719`).

### 3.8 Clearing, reruns and trajectories

```
POST /api/v2/.../clearTaskInstances {include_later_loop_iterations=true}     core_api/routes/public/task_instances.py:1030
└─ select_loop_clear_scope(later_loop_iterations=...)                         core/models/taskinstance.py:674
   · selection + same-iteration relatives → retry_ids
   · live TIs of the same loop family after the selected gate's iteration → archive_ids
└─ apply_loop_clear_scope                                                     :914
   · for each retried gate at k: DynamicRegion.fork_family(family, k+1)      dynamic_region.py:421
       → new region L' (forked_from=L, resumes_from_index=k+1, family_root=L)   = trajectory 2
   · clear_task_instances(tis, superseded_ti_ids=archive_ids)                 :431
       archived rows: working_set=NULL, archived_reason="superseded" (running ones → RESTARTING first)
```

The gate at k reruns in its original region L with its next try. If it continues, `get_successor_region_id(L, k+1)` returns L', so the replacement iterations k+1… are created in trajectory 2, and their try numbers restart at 1.

The CLI `airflow tasks clear` always clears later iterations, except with `--only-failed` or `--only-running` (`SerializedDAG.clear`, around `serialization/definitions/dag.py:1366`).

---

## 4. Mapped tasks: call chains

### 4.1 Authoring → `MappedOperator`

```
Op.partial(**kw)  → partial()  (validate, merge default_args)         sdk/bases/operator.py:350  → OperatorPartial
.expand(**lists)  → validate_mapping_kwargs → DictOfListsExpandInput   sdk/definitions/mappedoperator.py:196
  └─ _expand → MappedOperator(expand_input=..., partial_kwargs=...)   :216
       __attrs_post_init__: add to group/DAG; XComArg.apply_upstream_relationship (upstream edge)   :348
@task fn.expand(...) → DecoratedMappedOperator(op_kwargs_expand_input=...) → returns XComArg   bases/decorator.py:603,646,769
@task_group fn.expand(...) → MappedTaskGroup(expand_input) + MappedArgument stubs   decorators/task_group.py:162
```

### 4.2 Serialization

- `serialize_mapped_operator` (`core/serialization/serialized_objects.py:1005`) writes `_is_mapped=True`, the `partial_kwargs` and the encoded `expand_input`.
- `deserialize_operator` (`:1332`) builds a `SerializedMappedOperator` (`core/serialization/definitions/mappedoperator.py:90`). Its expand input becomes `SchedulerDictOfListsExpandInput` or `SchedulerListOfDictsExpandInput` (`core/models/expandinput.py:96,176`).
- Parse-time counts come from `get_parse_time_mapped_ti_count` (`expandinput.py:112`): the product of the literal lengths, or `NotFullyPopulated` when the input comes from an XCom.

### 4.3 The producer reports the length (worker → `xcom.mapped_length`)

```
run → _execute_task → _push_xcom_if_needed(result, ti)                        task_runner.py:1700, 2423
   · not mapped and has mapped dependants: value must be mappable → mapped_length = len(value)
   · None result with a mapped dependant → XComForMappingNotPushed
└─ _xcom_push(ti, "return_value", value, mapped_length) → XCom._push → SetXCom(mapped_length)   comms.py:995
   supervisor handle_set_xcom → client.xcoms.set
   → POST /execution/xcoms/{dag}/{run}/{task}/return_value?mapped_length=N    exec_api/routes/xcoms.py:696
       · [core] max_map_length check → 400 unmappable_return_value_length     :757
       · XComModel.set_for_attempt(..., mapped_length=N)                      core/models/xcom.py:151
```

A producer that is itself mapped does not send a length. Its consumer's length is the number of XCom rows with `map_index >= 0`.

### 4.4 The scheduler expands the placeholder

```
task_instance_scheduling_decisions → _get_ready_tis                          core/models/dagrun.py:1555, 1855
└─ placeholder (region_index < 0) whose deps are met → _expand_mapped_task_if_needed   :1877
   └─ ti.expand_mapped_task                                                   core/models/taskinstance.py:3184
      ├─ total = get_mapped_ti_count(task, run_id, producer_contexts=resolver.producer_contexts(ti))
      │     SchedulerPlainXComArg → get_task_map_length                        serialization/definitions/xcom_arg.py:161
      │       · unmapped producer: xcom.mapped_length
      │       · mapped producer:   COUNT(xcom rows, map_index >= 0) (None if any producer TI unfinished)
      │     producer_contexts restrict producers to the caller's loop iteration   task_coordinates.py:536
      ├─ None → UPSTREAM_FAILED (unless partial Dag) · 0 → SKIPPED
      ├─ placeholder becomes (M, 0); insert (M, 1..total-1)                    :3310-3379
      └─ rows with index >= total → REMOVED
└─ otherwise once per (task_id, region_id): _revise_map_indexes_if_mapped       dagrun.py:2700
```

When the inputs are literals, the instances are created when the run is created (`_create_tasks(expand_literals=True)`, `dagrun.py:2639`). A literal zero-length input with no upstreams is set to SKIPPED at once (`:2214-2234`).

### 4.5 A mapped task instance runs

```
_prepare → ti.render_templates → MappedOperator.render_template_fields       task_runner.py:1500; mappedoperator.py:822
└─ _expand_mapped_kwargs → ExpandInput.resolve(context)
   · DictOfListsExpandInput.resolve: resolve each XComArg, compute lengths,
     _expand_mapped_field picks the element for ti.map_index
     (cartesian product, last key varies fastest)                             _internal/expandinput.py:187,156
   · ListOfDictsExpandInput.resolve: value[map_index]                          :241
└─ unmap() → a real operator for this instance → render fields → execute
```

How an `XComArg` input resolves depends on the producer:

| Producer | `PlainXComArg.resolve` returns (`xcom_arg.py:333`) |
|---|---|
| Mapped, or a loop task read from outside its loop | `LazyXComSequence` (reduce) |
| Inside a mapped task group | Only the relevant map index or indexes, via `get_relevant_upstream_map_indexes` (`execution_time/task_mapping.py:90`): `ancestor = map_index * ancestor_count // ti_count` |
| Anything else | A plain `xcom_pull` |

`LazyXComSequence` (`execution_time/lazy_sequence.py:64`) fetches on demand:

| Access | Comms message | Request |
|---|---|---|
| `len()` | `GetXComCount` | `HEAD /xcoms/...` |
| `[i]` | `GetXComSequenceItem` | `GET .../item/{i}` |
| `[a:b]` | `GetXComSequenceSlice` | `GET .../slice` |

The `map_index_template` label is rendered in a sandboxed Jinja environment (`task_runner.py:2412`) before execute (`:1514`) and again after it (`:1677`). The worker sends it with `SetRenderedMapIndex` → `PATCH /execution/task-instances/{id}/rendered-map-index`, and it also rides on the final state payload.

`TIRunContext.expanded_ti_count` is the number of live instances in the same region, so inside a loop it is counted per iteration. `upstream_map_indexes` is no longer sent (`exec_api/versions/v2026_04_06.py:364`).

### 4.6 Trigger rules with mapped upstreams

`TriggerRuleDep._evaluate_trigger_rule` (`ti_deps/deps/trigger_rule_dep.py:120`) uses:
- `_get_expanded_ti_count` (`:157`)
- `_get_relevant_upstream_map_indexes` (`:185`) → `TaskInstance.get_relevant_upstream_map_indexes` (`core/models/taskinstance.py:3146`) → `_get_relevant_map_indexes` (`:3497`), which finds the innermost common mapped group

Inside the same mapped task group, instance *i* waits only on the matching upstream instance. A plain downstream waits on all of them. In runs with regions, the upstream TIs are also filtered by region (`dep_context.upstream_tis`).

---

## 5. How the main mechanisms work

1. **One coordinate system.**
   - `dynamic_region` plus `(region_id, region_index)` describe both a loop iteration and a map slot.
   - A mapped task inside a loop gets a child region parented at `(loop region, iteration)`.
   - That makes expansion, counts and `expanded_ti_count` per iteration automatically.
2. **The gate is the only point where iterations are created.**
   - Iterations are created lazily: only when the gate says continue.
   - The creation happens inside the Execution API state update, under a DagRun row lock and in the same transaction.
   - `has_later_loop_pass` makes a repeated or stale report change nothing.
   - No tail of skipped iterations is created up front.
3. **Shared decision rules.**
   - The worker decides (`execute_loop_gate`), and the server re-checks the decision (`validate_loop_decision`).
   - A fixed-count loop (no `until`) must stop exactly at `max_iterations`.
   - Hitting the cap without converging fails the gate and does not retry.
4. **The graph stays acyclic.**
   - There is no back edge.
   - Every edge leaving a loop is rewired to come from the gate. Outside consumers depend on the newest live gate, so they wait for the whole loop.
5. **The server picks the iteration.**
   - Reads carry only `previous_iteration: bool`.
   - The server works out the caller's iteration from its token.
   - Producers are resolved within the same iteration, in the previous iteration (`k-1`), or across all live iterations (for readers outside the loop), and replaced trajectories are filtered out.
6. **Map length is stored on the XCom row.**
   - `xcom.mapped_length` replaces the `task_map` table.
   - `max_map_length` is enforced when the XCom is written.
   - A mapped producer's length is the count of its XCom rows.
7. **Placeholder then expand.**
   - Each mapped task starts as one `-1` placeholder per region.
   - When its dependencies are met, the scheduler turns it into N instances. A count of 0 gives SKIPPED, and a missing input gives UPSTREAM_FAILED.
   - Literal inputs are expanded when the run is created, but only outside loops. Later loop iterations always go through a placeholder.
8. **Trajectories.**
   - A selective rerun with later iterations forks the loop region (`forked_from_region_id`, `resumes_from_index`).
   - The old later iterations are archived (`working_set=NULL`, not deleted). The rerun gate continues into the fork.
   - `loop.trajectory` and `region_id` expose this to tasks, and log paths show it as `pass=3.2`.

---

## 6. Web UI (brief)

| Surface | Backend | Frontend |
|---|---|---|
| Graph and Grid badges: loop icon + `ran/max`; `[N]` for mapped tasks; `[ ]` for mapped groups | `GridNodeResponse` / `NodeResponse`: `is_loop`, `loop_max_iterations`, `is_mapped` (`core_api/services/ui/task_group.py:52-177`); `ti_summaries` stream: `loop_iterations_count`, `child_states`, `latest_region_*` (`core_api/routes/ui/grid.py:414-620`) | `ui/src/components/TaskName.tsx:66-106`, `Graph/TaskNode.tsx`, `Grid/GridTI.tsx` |
| Group page **Iterations** tab: state, duration, tasks and decision reason per iteration | `GET /ui/grid/loop/{dag}/{run}/{group}` → `LoopSummaryResponse` (`core_api/services/ui/grid.py:384-512`) | `pages/GroupTaskInstance/LoopIterations/LoopIterations.tsx` |
| Iteration filter and Iteration column in Task Instances | `GET .../taskInstances?loop_id=&iteration=` (`core_api/routes/public/task_instances.py:649-708`) | `FilterBar/filters/LoopFilter.tsx`, `pages/TaskInstances/TaskInstances.tsx` |
| Gate decision in the task instance header | Looked up from the loop summary; it is not a field on the TI payload | `pages/TaskInstance/Header.tsx:79-137` |
| Clear dialog: **Clear later loop iterations** (default on) | `ClearTaskInstancesBody.include_later_loop_iterations` | `components/Clear/TaskInstance/ClearTaskInstanceDialog.tsx:161-284` |
| Mapped instances page and map-index switcher (labels from `rendered_map_index`) | `listMapped` / `taskInstances?task_id=` | `pages/MappedTaskInstance/*`, `layouts/Details/MapIndexSwitcherButton.tsx` |
| Loop history across runs (iterations per run, how often the cap was hit) | `GET /ui/grid/loop-history/{dag}/{group}` | `pages/Task/Overview/LoopHistoryChart.tsx` |

Iterations replaced by a rerun are kept in the database but not shown, except in the Gantt view, which includes archived attempts.

---

## 7. Things noticed while tracing

- **Comment and code disagree (verified).** In `core_api/routes/ui/grid.py:465-468` the comment says a looped task's state counts come only from its newest iteration. The code counts every pass: `GridNodeAgg.add_ti`, `core_api/services/ui/grid.py:94`, whose own docstring says so.
- **Possible bug (unverified).**
  - `_check_for_removed_or_restored_tasks` (`core/models/dagrun.py:2326`, `:2392`) builds its set of expanded tasks keyed by `task_id` only, across all regions.
  - Inside a loop, a literal-mapped task can be expanded in iteration 1 while still at a `-1` placeholder in iteration 2, because `complete_loop_gate` creates later iterations without `expand_literals`.
  - If `verify_integrity` runs at that moment, it might mark the later placeholder REMOVED.
- **Possible gap (unverified).** The ORM `TaskInstance.get_previous_ti` (`core/models/taskinstance.py:1925`) has no loop handling. The Execution API route that the worker uses does handle loops.
- **Possible dead read (unverified).** `xcom_arg.py:352` reads `context["expanded_ti_count"]`, but nothing appears to put that key into the template context, so the code falls back to a `GetTICount` call.
