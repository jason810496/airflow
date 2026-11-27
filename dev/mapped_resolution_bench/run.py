#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Benchmark and correctness harness for how a Python mapped task resolves its arguments (ADR-0016).

It runs the Dags in ``dags/`` through the real ``supervise_task()``, so each task process really forks (or
forks and execs), against an in-process Execution API and a SQLite database in a temporary
``AIRFLOW_HOME``. Mapped task instances are expanded by the scheduler's own code. Nothing here depends on
what the supervisor does with mapped tasks, so the same file measures any branch.

Measure a branch (``--env`` sets a variable for every run, for example a branch's feature flag)::

    uv run --project airflow-core --no-sync python dev/mapped_resolution_bench/run.py \\
        --variant baseline
    uv run --project airflow-core --no-sync python dev/mapped_resolution_bench/run.py \\
        --variant resolve-before-fork --env AIRFLOW_EXP_RESOLVE_BEFORE_FORK=1

Compare the task instance states and XCom values of two results, failing on any difference::

    uv run --project airflow-core --no-sync python dev/mapped_resolution_bench/run.py \\
        --check files/mapped_resolution_bench/a.json files/mapped_resolution_bench/b.json

How a run is laid out:

* One worker process per (shape, weight, mode, repetition), with its own copy of an initialised database.
* The worker forks a short-lived "supervisor process" for every task instance, so the supervisor's memory
  does not carry over from one task instance to the next, and the Dag file is never imported in the worker.
* The supervisor process calls ``supervise_task()`` and samples ``/proc`` while it runs: its own RSS, and
  the ``VmHWM`` (peak RSS) of its child, the task process. ``ru_maxrss`` of the children is recorded too,
  but it over-reports for fork+exec, because the exec'd child starts from the forking parent's peak.
* Each Dag file appends ``<pid> <task instance>`` to a file when it is imported, which gives the import
  count per task instance, split into supervisor and task process.
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import json
import math
import os
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
DAGS_DIR = HERE / "dags"
BUNDLE_NAME = "bench"
SHAPES = [
    "expand",
    "partial_expand",
    "expand_kwargs",
    "map",
    "zip",
    "concat",
    "mapped_group",
    "reduce",
]
WEIGHTS = ["light", "heavy"]
MODES = ["fork", "exec"]
HANDOVER_MARKER = "pre_resolved_expand_inputs installed"
"""What a branch logs from the task process when it used values the supervisor resolved."""
SAMPLE_INTERVAL = 0.01
TI_TIMEOUT = 300
REP_TIMEOUT = 1800


def build_env(home: Path, weight: str, mode: str, import_log: Path | None) -> dict[str, str]:
    """Return the environment variables a worker, and everything it starts, runs with."""
    bundle = {
        "name": BUNDLE_NAME,
        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
        "kwargs": {"path": str(DAGS_DIR)},
    }
    env = {
        "AIRFLOW_HOME": str(home),
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{home}/airflow.db",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__DAGS_FOLDER": str(DAGS_DIR),
        "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps([bundle]),
        "AIRFLOW__LOGGING__LOGGING_LEVEL": "INFO",
        "AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER": "True" if mode == "exec" else "False",
        "BENCH_WEIGHT": weight,
    }
    if import_log is not None:
        env["BENCH_IMPORT_LOG"] = str(import_log)
    return env


# Orchestrator: lays out the runs and writes the results.


def get_git_short_sha() -> str:
    return subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    ).stdout.strip()


def get_git_branch() -> str:
    return subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        check=False,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    ).stdout.strip()


def parse_env_overrides(items: list[str]) -> dict[str, str]:
    overrides = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep:
            raise SystemExit(f"--env expects KEY=VALUE, got {item!r}")
        overrides[key] = value
    return overrides


def init_template_db(root: Path, base_env: dict[str, str]) -> Path:
    """Initialise one database from Python, to copy for every repetition."""
    home = root / "template"
    home.mkdir()
    env = {**base_env, **build_env(home, "light", "fork", None)}
    subprocess.run(
        [sys.executable, str(Path(__file__)), "--init-db"],
        env=env,
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
    )
    return home


def run_rep(
    *,
    root: Path,
    template: Path,
    shape: str,
    weight: str,
    mode: str,
    rep: int,
    base_env: dict[str, str],
    keep: bool,
) -> dict[str, Any]:
    home = root / f"{shape}-{weight}-{mode}-{rep}"
    shutil.copytree(template, home)
    result_path = home / "result.json"
    env = {**base_env, **build_env(home, weight, mode, home / "imports.log")}
    spec = {"shape": shape, "weight": weight, "mode": mode, "rep": rep, "result": str(result_path)}
    try:
        subprocess.run(
            [sys.executable, str(Path(__file__)), "--rep-worker", json.dumps(spec)],
            env=env,
            cwd=REPO_ROOT,
            timeout=REP_TIMEOUT,
            check=True,
            capture_output=True,
            text=True,
        )
        result = json.loads(result_path.read_text())
    except subprocess.CalledProcessError as e:
        result = {**spec, "tis": [], "errors": [f"worker exited {e.returncode}: {e.stderr[-2000:]}"]}
    except subprocess.TimeoutExpired:
        result = {**spec, "tis": [], "errors": [f"worker timed out after {REP_TIMEOUT}s"]}
    except FileNotFoundError:
        result = {**spec, "tis": [], "errors": ["worker wrote no result"]}
    result.pop("result", None)
    if not keep:
        shutil.rmtree(home, ignore_errors=True)
    return result


def cmd_run(args: argparse.Namespace) -> int:
    overrides = parse_env_overrides(args.env)
    base_env = {**os.environ, **overrides}
    sha = get_git_short_sha()
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    root = Path(tempfile.mkdtemp(prefix="mapped_resolution_bench_"))
    started = time.monotonic()
    load_at_start = list(os.getloadavg())
    try:
        print(f"[bench] working in {root}", file=sys.stderr, flush=True)
        template = init_template_db(root, base_env)
        cells = [(s, w, m) for s in args.shapes for w in args.weights for m in args.modes]
        total = len(cells) * args.reps
        reps: list[dict[str, Any]] = []
        # Repetition is the outer loop, so a slow drift of the machine hits every cell alike.
        for rep in range(args.reps):
            for shape, weight, mode in cells:
                result = run_rep(
                    root=root,
                    template=template,
                    shape=shape,
                    weight=weight,
                    mode=mode,
                    rep=rep,
                    base_env=base_env,
                    keep=args.keep,
                )
                reps.append(result)
                wall = sum(t.get("wall_s", 0.0) for t in result["tis"])
                errors = f" ERRORS={result['errors']}" if result.get("errors") else ""
                print(
                    f"[bench] {len(reps)}/{total} {shape} {weight} {mode} rep={rep} "
                    f"tis={len(result['tis'])} supervise_total={wall:.2f}s{errors}",
                    file=sys.stderr,
                    flush=True,
                )
    finally:
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)

    meta = {
        "variant": args.variant,
        "git_sha": sha,
        "git_branch": get_git_branch(),
        "env_overrides": overrides,
        "reps": args.reps,
        "shapes": args.shapes,
        "weights": args.weights,
        "modes": args.modes,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "started_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "elapsed_s": round(time.monotonic() - started, 1),
        "loadavg_at_start": load_at_start,
        "loadavg_at_end": list(os.getloadavg()),
        "command": " ".join([sys.executable, *sys.argv]),
    }
    payload = {"meta": meta, "reps": reps}
    stem = f"{sha}-{args.variant}"
    (out_dir / f"{stem}.json").write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n")
    (out_dir / f"{stem}.md").write_text(render_markdown(payload))
    print(f"[bench] wrote {out_dir / stem}.json and .md", file=sys.stderr, flush=True)
    return 0 if all(not r.get("errors") for r in reps) else 1


# Report: renders the results as Markdown.


def percentile(values: list[float], q: float) -> float | None:
    """Return the *q* quantile of *values*, interpolating between the two nearest ranks."""
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower, upper = math.floor(position), math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def format_stat(values: list[float], scale: float = 1.0, digits: int = 0) -> str:
    if not values:
        return "-"
    median = statistics.median(values) / scale
    p90 = percentile(values, 0.9) / scale  # type: ignore[operator]
    return f"{median:.{digits}f} / {p90:.{digits}f}"


def pool_tis(reps: list[dict[str, Any]], *, mapped: bool) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    pooled: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for rep in reps:
        for ti in rep["tis"]:
            if (ti["map_index"] >= 0) == mapped and "wall_s" in ti:
                pooled[(rep["shape"], rep["weight"], rep["mode"])].append(ti)
    return pooled


def render_table(reps: list[dict[str, Any]], *, mapped: bool) -> list[str]:
    pooled = pool_tis(reps, mapped=mapped)
    lines = [
        "| shape | weight | mode | n | wall ms (med / p90) | supervisor peak MB (med / p90) "
        "| supervisor growth MB (med / p90) | task peak MB (med / p90) | imports supervisor+task (med) "
        "| XCom API reads (med) | handover used |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for shape in SHAPES:
        for weight in WEIGHTS:
            for mode in MODES:
                tis = pooled.get((shape, weight, mode))
                if not tis:
                    continue
                kb = 1024.0
                sup_peak = [t["sup_peak_rss_kb"] for t in tis if t.get("sup_peak_rss_kb") is not None]
                sup_growth = [
                    t["sup_peak_rss_kb"] - t["sup_start_rss_kb"]
                    for t in tis
                    if t.get("sup_peak_rss_kb") is not None
                ]
                task_peak = [t["task_peak_rss_kb"] for t in tis if t.get("task_peak_rss_kb") is not None]
                imports_sup = statistics.median(t["imports_supervisor"] for t in tis)
                imports_task = statistics.median(t["imports_task"] for t in tis)
                xcom_reads = statistics.median(t.get("xcom_requests", 0) for t in tis)
                handover = sum(1 for t in tis if t.get("handover_used"))
                lines.append(
                    f"| {shape} | {weight} | {mode} | {len(tis)} "
                    f"| {format_stat([t['wall_s'] * 1000 for t in tis])} "
                    f"| {format_stat(sup_peak, kb)} | {format_stat(sup_growth, kb)} "
                    f"| {format_stat(task_peak, kb)} | {imports_sup:g}+{imports_task:g} "
                    f"| {xcom_reads:g} | {handover}/{len(tis)} |"
                )
    return lines


def render_markdown(payload: dict[str, Any]) -> str:
    meta, reps = payload["meta"], payload["reps"]
    failed = [r for r in reps if r.get("errors")]
    lines = [
        f"# Mapped resolution bench: {meta['variant']}",
        "",
        f"- commit `{meta['git_sha']}` on `{meta['git_branch']}`, Python {meta['python']}, "
        f"{meta['cpu_count']} CPUs, {meta['platform']}",
        f"- env overrides: `{json.dumps(meta['env_overrides'])}`",
        f"- {meta['reps']} repetitions per cell, {len(reps)} runs in {meta['elapsed_s']} s, "
        f"started {meta['started_at']}",
        f"- command: `{meta['command']}`",
        "- Each cell pools the task instances of all repetitions. `wall` is the time inside `supervise_task()`.",
        "  `supervisor peak` is the supervisor process's peak RSS, `supervisor growth` that minus its RSS at the "
        "start of the call. `task peak` is the task process's `VmHWM`, sampled every 10 ms.",
        "  `imports` is how often the Dag file was imported for one task instance, in the supervisor process "
        "plus the task process. `XCom API reads` counts the requests for XCom values the supervisor made to the "
        "API, for itself and for the task process. `handover used` counts task processes that logged using "
        "values the supervisor resolved.",
        "",
    ]
    if failed:
        lines += [f"**{len(failed)} run(s) failed**, their task instances are missing below:", ""]
        lines += [
            f"- {r['shape']} {r['weight']} {r['mode']} rep {r['rep']}: {r['errors'][0][:200]!r}"
            for r in failed
        ]
        lines.append("")
    lines += ["## Mapped task instances", ""]
    lines += render_table(reps, mapped=True)
    lines += ["", "## Unmapped task instances (producers and reduce)", ""]
    lines += render_table(reps, mapped=False)
    lines.append("")
    return "\n".join(lines)


# Check: compares the outcomes of two result files.


def index_outcomes(payload: dict[str, Any]) -> tuple[dict[tuple, tuple[str, str]], dict[tuple, str]]:
    """Index the final state and XCom of every task instance, and the final state of every run."""
    outcomes: dict[tuple, tuple[str, str]] = {}
    run_states: dict[tuple, str] = {}
    for rep in payload["reps"]:
        cell = (rep["shape"], rep["weight"], rep["mode"], rep["rep"])
        run_states[cell] = f"{rep.get('dag_run_state')} errors={rep.get('errors') or []}"
        for ti in rep["tis"]:
            outcomes[(*cell, ti["task_id"], ti["map_index"])] = (
                str(ti.get("final_state")),
                json.dumps(ti.get("xcom"), sort_keys=True),
            )
    return outcomes, run_states


def cmd_check(first: str, second: str) -> int:
    payloads = [json.loads(Path(p).read_text()) for p in (first, second)]
    names = [f"{p['meta']['variant']}@{p['meta']['git_sha']}" for p in payloads]
    (out_a, runs_a), (out_b, runs_b) = (index_outcomes(p) for p in payloads)
    differences = []
    for key in sorted(set(out_a) | set(out_b), key=str):
        if out_a.get(key) != out_b.get(key):
            differences.append(f"{key}:\n    {names[0]}: {out_a.get(key)}\n    {names[1]}: {out_b.get(key)}")
    for key in sorted(set(runs_a) | set(runs_b), key=str):
        if runs_a.get(key) != runs_b.get(key):
            differences.append(f"run {key}: {runs_a.get(key)} != {runs_b.get(key)}")
    compared = len(set(out_a) & set(out_b))
    if differences:
        print(f"{len(differences)} difference(s) between {names[0]} and {names[1]}:")
        print("\n".join(differences))
        return 1
    print(f"OK: {compared} task instances match between {names[0]} and {names[1]}")
    return 0


# Worker: runs inside one temporary AIRFLOW_HOME.


def read_proc_kb(pid: int, field: str) -> int | None:
    """Read a ``kB`` field such as ``VmRSS`` or ``VmHWM`` from ``/proc/<pid>/status``."""
    try:
        with open(f"/proc/{pid}/status") as status:
            for line in status:
                if line.startswith(field + ":"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


class ProcessSampler(threading.Thread):
    """Sample the RSS of this process and the peak RSS of its children while a block runs."""

    def __init__(self, interval: float = SAMPLE_INTERVAL):
        super().__init__(daemon=True)
        import psutil

        self.interval = interval
        self.me = psutil.Process()
        self.child_peak_kb: dict[int, int] = {}
        self.own_rss_peak_kb = 0
        self._halt = threading.Event()

    def sample(self) -> None:
        import psutil

        self.own_rss_peak_kb = max(self.own_rss_peak_kb, read_proc_kb(os.getpid(), "VmRSS") or 0)
        try:
            children = self.me.children()
        except psutil.Error:
            return
        for child in children:
            # VmHWM only grows, so the last value read before the child exits is its best peak. Keep
            # the last one, not the maximum: a child spawned by posix_spawn shares the parent's address
            # space for an instant, and a read in that instant reports the parent's.
            if (hwm := read_proc_kb(child.pid, "VmHWM")) is not None:
                self.child_peak_kb[child.pid] = hwm

    def run(self) -> None:
        while not self._halt.is_set():
            self.sample()
            self._halt.wait(self.interval)

    def stop(self) -> None:
        self._halt.set()
        self.join()


def run_in_fork(fn: Callable[[], Any]) -> Any:
    """Run *fn* in a forked child and return its JSON-able result, or raise with the child's traceback."""
    from airflow import settings

    sys.stdout.flush()
    sys.stderr.flush()
    # A SQLite connection must not be shared with the child.
    settings.engine.dispose()
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        status = 0
        try:
            os.close(read_fd)
            payload = json.dumps({"ok": True, "result": fn()})
        except BaseException:
            payload = json.dumps({"ok": False, "error": traceback.format_exc()})
            status = 1
        with os.fdopen(write_fd, "w") as pipe:
            pipe.write(payload)
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(status)
    os.close(write_fd)
    with os.fdopen(read_fd) as pipe:
        data = pipe.read()
    os.waitpid(pid, 0)
    if not data:
        raise RuntimeError("the forked process died without a result")
    message = json.loads(data)
    if not message["ok"]:
        raise RuntimeError(message["error"])
    return message["result"]


def read_import_counts(import_log: Path, label: str, supervisor_pid: int) -> tuple[int, int]:
    """Count the imports made for task instance *label*: in the supervisor process, and elsewhere."""
    supervisor = other = 0
    if import_log.exists():
        for line in import_log.read_text().splitlines():
            pid, _, ti_label = line.partition(" ")
            if ti_label == label:
                if int(pid) == supervisor_pid:
                    supervisor += 1
                else:
                    other += 1
    return supervisor, other


def create_dag_run(shape: str) -> dict[str, str]:
    """Sync the Dag to the database and create its run, as ``dag.test()`` does. Meant for a forked child."""
    from airflow import settings
    from airflow.dag_processing.bundles.manager import DagBundlesManager
    from airflow.dag_processing.dagbag import BundleDagBag, sync_bag_to_db
    from airflow.models.dagrun import DagRun, get_or_create_dagrun
    from airflow.sdk import timezone
    from airflow.serialization.serialized_objects import DagSerialization
    from airflow.utils.types import DagRunTriggeredByType, DagRunType

    session = settings.Session()
    manager = DagBundlesManager()
    manager.sync_bundles_to_db(session=session)
    session.commit()
    bundle = manager.get_bundle(BUNDLE_NAME)
    bundle.initialize()
    dag_id = f"bench_{shape}"
    bag = BundleDagBag(
        dag_folder=str(DAGS_DIR / f"{dag_id}.py"), bundle_path=bundle.path, bundle_name=bundle.name
    )
    if bag.import_errors:
        raise RuntimeError(f"Dag import errors: {bag.import_errors}")
    sync_bag_to_db(bag, bundle.name, bundle.version)
    scheduler_dag = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(bag.dags[dag_id]))
    logical_date = timezone.utcnow()
    dag_run = get_or_create_dagrun(
        dag=scheduler_dag,
        start_date=timezone.utcnow(),
        logical_date=logical_date,
        data_interval=scheduler_dag.timetable.infer_manual_data_interval(run_after=logical_date),
        run_after=logical_date,
        run_id=DagRun.generate_run_id(
            run_type=DagRunType.MANUAL, logical_date=logical_date, run_after=logical_date
        ),
        session=session,
        conf=None,
        triggered_by=DagRunTriggeredByType.TEST,
        triggering_user_name="mapped_resolution_bench",
    )
    session.commit()
    return {"dag_id": dag_id, "run_id": dag_run.run_id}


def supervise_one(workload: Any, label: str, home: Path, ti_key: tuple[str, str, str, int]) -> dict[str, Any]:
    """
    Run one task instance through ``supervise_task()`` and measure it. Meant for a forked child.

    :param ti_key: dag_id, run_id, task_id and the map index to read the returned XCom with.
    """
    import resource
    from uuid import UUID

    log_file = os.open(home / f"{label}.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND)
    os.dup2(log_file, 1)
    os.dup2(log_file, 2)

    def on_timeout(*_: Any) -> None:
        import psutil

        for child in psutil.Process().children(recursive=True):
            child.kill()
        os._exit(3)

    signal.signal(signal.SIGALRM, on_timeout)
    signal.alarm(TI_TIMEOUT)

    os.environ["BENCH_TI"] = label
    from airflow.sdk.execution_time import supervisor

    client = supervisor.InProcessTestSupervisor._api_client(dag=True, attempt_id=UUID(str(workload.ti.id)))
    # Start the in-process API and open its connection before the clock starts.
    with contextlib.suppress(Exception):
        client.get("health")
    supervise = getattr(supervisor, "supervise_task", None) or supervisor.supervise

    # Every XCom read of the task process goes through the supervisor's client too, so this counts the
    # reads of the whole task instance, whichever process asked for them.
    api_requests = {"total": 0, "xcom": 0}
    send_request = client.request

    def count_request(method: str, url: Any, *args: Any, **kwargs: Any) -> Any:
        api_requests["total"] += 1
        api_requests["xcom"] += "xcoms/" in str(url)
        return send_request(method, url, *args, **kwargs)

    client.request = count_request

    sampler = ProcessSampler()
    start_rss_kb = read_proc_kb(os.getpid(), "VmRSS")
    sampler.start()
    started = time.monotonic()
    try:
        exit_code = supervise(
            ti=workload.ti,
            bundle_info=workload.bundle_info,
            dag_rel_path=workload.dag_rel_path,
            token="",
            client=client,
            log_path=None,
        )
    finally:
        wall = time.monotonic() - started
        sampler.stop()
    peak_kb = read_proc_kb(os.getpid(), "VmHWM")
    task_peak_kb = max(sampler.child_peak_kb.values(), default=None)

    client.request = send_request
    dag_id, run_id, task_id, map_index = ti_key
    xcom: Any = None
    with contextlib.suppress(Exception):
        xcom = client.xcoms.get(dag_id, run_id, task_id, "return_value", map_index).value

    sys.stdout.flush()
    sys.stderr.flush()
    imports_supervisor, imports_task = read_import_counts(home / "imports.log", label, os.getpid())
    log_text = (home / f"{label}.log").read_text(errors="replace")
    return {
        "wall_s": wall,
        "exit_code": exit_code,
        "sup_start_rss_kb": start_rss_kb,
        "sup_peak_rss_kb": peak_kb,
        "sup_sampled_rss_peak_kb": sampler.own_rss_peak_kb,
        "task_peak_rss_kb": task_peak_kb,
        "task_rusage_maxrss_kb": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
        "imports_supervisor": imports_supervisor,
        "imports_task": imports_task,
        "api_requests": api_requests["total"],
        "xcom_requests": api_requests["xcom"],
        "handover_used": HANDOVER_MARKER in log_text,
        "xcom": xcom,
    }


def drive_dag_run(spec: dict[str, Any], run: dict[str, str], home: Path) -> list[dict[str, Any]]:
    """Schedule the run as ``dag.test()`` does, and supervise each task instance as it becomes ready."""
    from pathlib import Path as _Path

    from sqlalchemy import select

    from airflow import settings
    from airflow.executors import workloads
    from airflow.executors.workloads import BundleInfo
    from airflow.models.dagbag import DBDagBag
    from airflow.models.dagrun import DagRun
    from airflow.sdk import timezone
    from airflow.utils.state import DagRunState, TaskInstanceState

    shape = spec["shape"]
    dag_id, run_id = run["dag_id"], run["run_id"]
    measurements: list[dict[str, Any]] = []
    for _ in range(50):
        session = settings.Session()
        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id))
        dag_run.dag = DBDagBag().get_dag_for_run(dag_run, session=session)
        if dag_run.state != DagRunState.RUNNING:
            session.close()
            break
        schedulable_tis, _ = dag_run.update_state(session=session)
        for ti in schedulable_tis:
            if ti.try_number == 0:
                ti.try_number = 1
            ti.state = TaskInstanceState.SCHEDULED
            ti.scheduled_dttm = timezone.utcnow()
        session.commit()
        scheduled = sorted(
            (
                ti
                for ti in dag_run.get_task_instances(session=session)
                if ti.state == TaskInstanceState.SCHEDULED
            ),
            key=lambda ti: (ti.task_id, ti.map_index),
        )
        if not scheduled:
            session.close()
            break
        batch = []
        for ti in scheduled:
            ti.set_state(TaskInstanceState.QUEUED)
            workload = workloads.ExecuteTask.make(
                ti,
                dag_rel_path=_Path(f"bench_{shape}.py"),
                generator=None,
                bundle_info=BundleInfo(name=BUNDLE_NAME),
            )
            batch.append((ti.task_id, ti.map_index, workload))
        session.commit()
        session.close()

        for task_id, map_index, workload in batch:
            label = f"{task_id}[{map_index}]"
            ti_key = (dag_id, run_id, task_id, int(workload.ti.map_index))
            measurement = {"task_id": task_id, "map_index": map_index}
            try:
                measurement.update(
                    run_in_fork(functools.partial(supervise_one, workload, label, home, ti_key))
                )
            except Exception:
                measurement["error"] = traceback.format_exc()
            measurements.append(measurement)
    return measurements


def collect_final_states(run: dict[str, str]) -> tuple[str, dict[tuple[str, int], str]]:
    from sqlalchemy import select

    from airflow import settings
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

    with settings.Session() as session:
        dag_run = session.scalar(
            select(DagRun).where(DagRun.dag_id == run["dag_id"], DagRun.run_id == run["run_id"])
        )
        tis = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == run["dag_id"], TaskInstance.run_id == run["run_id"]
            )
        ).all()
        return str(dag_run.state), {(ti.task_id, ti.map_index): str(ti.state) for ti in tis}


def cmd_rep_worker(spec: dict[str, Any]) -> int:
    home = Path(os.environ["AIRFLOW_HOME"])
    result: dict[str, Any] = {**spec, "tis": [], "errors": []}
    try:
        run = run_in_fork(lambda: create_dag_run(spec["shape"]))
        measurements = drive_dag_run(spec, run, home)
        dag_run_state, states = collect_final_states(run)
        result["dag_run_state"] = dag_run_state
        for measurement in measurements:
            if "error" in measurement:
                result["errors"].append(
                    f"{measurement['task_id']}[{measurement['map_index']}]: {measurement['error']}"
                )
                continue
            measurement["final_state"] = states.get((measurement["task_id"], measurement["map_index"]))
            result["tis"].append(measurement)
        missing = set(states) - {(t["task_id"], t["map_index"]) for t in result["tis"]}
        if missing:
            result["errors"].append(f"task instances never supervised: {sorted(missing)}")
    except Exception:
        result["errors"].append(traceback.format_exc())
    Path(spec["result"]).write_text(json.dumps(result))
    return 0


def cmd_init_db() -> int:
    from airflow.utils.db import initdb

    initdb()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--variant", help="Label of this run, used in the result file names")
    parser.add_argument("--reps", type=int, default=5, help="Repetitions of each cell (default 5)")
    parser.add_argument("--shapes", nargs="+", choices=SHAPES, default=SHAPES)
    parser.add_argument("--weights", nargs="+", choices=WEIGHTS, default=WEIGHTS)
    parser.add_argument("--modes", nargs="+", choices=MODES, default=MODES)
    parser.add_argument(
        "--env", action="append", default=[], metavar="KEY=VALUE", help="Set a variable for all runs"
    )
    parser.add_argument("--out-dir", default="files/mapped_resolution_bench")
    parser.add_argument("--keep", action="store_true", help="Keep the temporary AIRFLOW_HOMEs and logs")
    parser.add_argument("--check", nargs=2, metavar=("A.json", "B.json"), help="Compare two result files")
    parser.add_argument("--rep-worker", help=argparse.SUPPRESS)
    parser.add_argument("--init-db", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.check:
        return cmd_check(*args.check)
    if args.init_db:
        return cmd_init_db()
    if args.rep_worker:
        return cmd_rep_worker(json.loads(args.rep_worker))
    if not args.variant:
        parser.error("--variant is required")
    return cmd_run(args)


if __name__ == "__main__":
    sys.exit(main())
