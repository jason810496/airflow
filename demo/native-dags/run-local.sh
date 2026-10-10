#!/usr/bin/env bash
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
#
# Runs the CoC EU Retail demo on a native Airflow built from this worktree, without Breeze or Docker:
# SQLite, LocalExecutor, Simple auth manager, api-server, scheduler, dag-processor and triggerer.
#
#   demo/native-dags/run-local.sh start | stop | status | seed | logs <component>
#   demo/native-dags/run-local.sh trigger <dag_id> [conf_json]
#   demo/native-dags/run-local.sh wait <dag_id> <run_id> [timeout_seconds]
#
# Run demo/native-dags/setup.sh first. Settings (environment variables):
#   DEMO_STATE_DIR    venv, AIRFLOW_HOME, logs and pids (default demo/native-dags/.local-airflow)
#   DEMO_FILES_ROOT   replaces /files from the Breeze layout (default <worktree>/files)
#   DEMO_NODE_BIN     directory with Node.js 22, put first on PATH (default: node already on PATH)
#   DEMO_JAVA_BIN     directory with java 11 or later, put first on PATH (default: java already on PATH)
#   DEMO_PORT         api-server port (default 28080)
#   DEMO_ADMIN_USER / DEMO_ADMIN_PASSWORD   login (default admin / admin)
#
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
worktree="$(cd "${here}/../.." && pwd)"

STATE_DIR="${DEMO_STATE_DIR:-${here}/.local-airflow}"
FILES_ROOT="${DEMO_FILES_ROOT:-${worktree}/files}"
NODE_BIN="${DEMO_NODE_BIN:-}"
JAVA_BIN="${DEMO_JAVA_BIN:-}"
PORT="${DEMO_PORT:-28080}"
ADMIN_USER="${DEMO_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${DEMO_ADMIN_PASSWORD:-admin}"

VENV="${STATE_DIR}/venv"
LOG_DIR="${STATE_DIR}/logs"
PID_DIR="${STATE_DIR}/pids"
COMPONENTS=(api-server scheduler dag-processor triggerer)
BASE_URL="http://localhost:${PORT}"

die() { echo "run-local: $*" >&2; exit 1; }

load_env() {
    local env_file="${worktree}/files/airflow-breeze-config/environment_variables.env"
    [[ -f "${env_file}" ]] || die "${env_file} not found, run demo/native-dags/setup.sh first"
    mkdir -p "${FILES_ROOT}"
    FILES_ROOT="$(cd "${FILES_ROOT}" && pwd)"

    local names
    names="$(grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "${env_file}" | cut -d= -f1)"
    set -a
    # shellcheck disable=SC1090
    source "${env_file}"
    set +a
    local name
    for name in ${names}; do
        export "${name}=${!name//\/files\//${FILES_ROOT}/}"
    done

    export COCEU_LAKE_ROOT="${FILES_ROOT}/demo/lake"
    export COCEU_OUTBOX_ROOT="${FILES_ROOT}/demo/outbox"
    mkdir -p "${COCEU_LAKE_ROOT}" "${COCEU_OUTBOX_ROOT}" "${STATE_DIR}/dags" "${LOG_DIR}" "${PID_DIR}"

    export AIRFLOW_HOME="${STATE_DIR}/airflow_home"
    mkdir -p "${AIRFLOW_HOME}"
    export PATH="${NODE_BIN:+${NODE_BIN}:}${JAVA_BIN:+${JAVA_BIN}:}${VENV}/bin:${PATH}"
    export AIRFLOW__CORE__DAGS_FOLDER="${STATE_DIR}/dags"
    export AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__EXECUTOR=LocalExecutor
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///${AIRFLOW_HOME}/airflow.db"
    export AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
    export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="${ADMIN_USER}:admin"
    export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE="${AIRFLOW_HOME}/simple_auth_passwords.json"
    export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="${BASE_URL}/execution/"
    export AIRFLOW__API__BASE_URL="${BASE_URL}"
    export AIRFLOW__API__PORT="${PORT}"
    export AIRFLOW__API_AUTH__JWT_SECRET="demo-native-dags-jwt-secret-0123456789abcdef"
    export AIRFLOW__CORE__FERNET_KEY="$(fernet_key)"
    export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=10
    export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=10
    printf '{"%s": "%s"}\n' "${ADMIN_USER}" "${ADMIN_PASSWORD}" > "${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE}"
}

fernet_key() {
    local key_file="${STATE_DIR}/fernet.key"
    if [[ ! -s "${key_file}" ]]; then
        "${VENV}/bin/python" -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())' > "${key_file}"
    fi
    cat "${key_file}"
}

ensure_venv() {
    [[ -x "${VENV}/bin/airflow" ]] && return 0
    command -v uv >/dev/null || die "uv is required"
    mkdir -p "${STATE_DIR}"
    uv venv --python 3.12 "${VENV}"
    uv pip install --python "${VENV}/bin/python" \
        -e "${worktree}/airflow-core" \
        -e "${worktree}/task-sdk" \
        -e "${worktree}/providers/common/compat" \
        -e "${worktree}/providers/standard"
}

pid_file() { echo "${PID_DIR}/$1.pid"; }

is_running() {
    local file pid
    file="$(pid_file "$1")"
    [[ -f "${file}" ]] || return 1
    pid="$(cat "${file}")"
    kill -0 "${pid}" 2>/dev/null
}

start_component() {
    local name="$1"; shift
    if is_running "${name}"; then
        echo "${name} already running (pid $(cat "$(pid_file "${name}")"))"
        return 0
    fi
    setsid nohup "$@" > "${LOG_DIR}/${name}.log" 2>&1 < /dev/null &
    echo $! > "$(pid_file "${name}")"
    echo "started ${name} (pid $!)"
}

wait_for_api() {
    local i
    for i in $(seq 1 120); do
        if curl -fsS "${BASE_URL}/api/v2/monitor/health" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    die "api-server did not become healthy, see ${LOG_DIR}/api-server.log"
}

cmd_seed() {
    local seed="${FILES_ROOT}/demo/seed.sh"
    [[ -f "${seed}" ]] || die "${seed} not found, run demo/native-dags/setup.sh first"
    bash "${seed}"
}

cmd_start() {
    ensure_venv
    load_env
    airflow db migrate > "${LOG_DIR}/db-migrate.log" 2>&1 || die "db migrate failed, see ${LOG_DIR}/db-migrate.log"
    cmd_seed
    start_component api-server airflow api-server --port "${PORT}"
    start_component scheduler airflow scheduler
    start_component dag-processor airflow dag-processor
    start_component triggerer airflow triggerer
    wait_for_api
    echo "Airflow is up at ${BASE_URL} (login ${ADMIN_USER} / ${ADMIN_PASSWORD})"
    echo "state: ${STATE_DIR}, files root: ${FILES_ROOT}"
}

cmd_stop() {
    local name pid i
    for name in "${COMPONENTS[@]}"; do
        if is_running "${name}"; then
            pid="$(cat "$(pid_file "${name}")")"
            # setsid made the component a process group leader, which also covers its children.
            kill -TERM -- "-${pid}" 2>/dev/null || kill -TERM "${pid}" 2>/dev/null || true
            for i in $(seq 1 20); do
                kill -0 "${pid}" 2>/dev/null || break
                sleep 0.5
            done
            kill -KILL -- "-${pid}" 2>/dev/null || true
            echo "stopped ${name}"
        fi
        rm -f "$(pid_file "${name}")"
    done
}

cmd_status() {
    local name rc=0
    for name in "${COMPONENTS[@]}"; do
        if is_running "${name}"; then
            echo "${name}: running (pid $(cat "$(pid_file "${name}")"))"
        else
            echo "${name}: stopped"
            rc=1
        fi
    done
    if curl -fsS "${BASE_URL}/api/v2/monitor/health" >/dev/null 2>&1; then
        echo "api health: ok (${BASE_URL})"
    else
        echo "api health: unreachable (${BASE_URL})"
    fi
    return "${rc}"
}

cmd_logs() {
    local name="${1:-}"
    [[ -n "${name}" ]] || die "usage: logs <$(IFS='|'; echo "${COMPONENTS[*]}")>"
    [[ -f "${LOG_DIR}/${name}.log" ]] || die "no log for ${name}"
    tail -n 200 -f "${LOG_DIR}/${name}.log"
}

token() {
    curl -fsS -X POST "${BASE_URL}/auth/token" -H 'Content-Type: application/json' \
        -d "{\"username\": \"${ADMIN_USER}\", \"password\": \"${ADMIN_PASSWORD}\"}" |
        "${VENV}/bin/python" -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
}

cmd_trigger() {
    local dag_id="${1:-}" conf="${2:-}"
    [[ -n "${dag_id}" ]] || die "usage: trigger <dag_id> [conf_json]"
    [[ -n "${conf}" ]] || conf='{}'
    local jwt
    jwt="$(token)"
    curl -sS -X POST "${BASE_URL}/api/v2/dags/${dag_id}/dagRuns" \
        -H "Authorization: Bearer ${jwt}" -H 'Content-Type: application/json' \
        -d "{\"logical_date\": null, \"conf\": ${conf}}" |
        "${VENV}/bin/python" -c '
import json, sys
body = json.load(sys.stdin)
if "dag_run_id" not in body:
    sys.exit(f"trigger failed: {body}")
print(body["dag_run_id"])'
}

TABLE_PY='
import json, sys
tis = sorted(json.load(sys.stdin)["task_instances"], key=lambda t: t.get("start_date") or "")
print("%-40s %-16s %-12s %s" % ("task_id", "state", "queue", "try"))
for t in tis:
    print("%-40s %-16s %-12s %s" % (t["task_id"], t["state"], t.get("queue"), t["try_number"]))
'

cmd_wait() {
    local dag_id="${1:-}" run_id="${2:-}" timeout="${3:-600}"
    [[ -n "${dag_id}" && -n "${run_id}" ]] || die "usage: wait <dag_id> <run_id> [timeout_seconds]"
    local jwt deadline state
    jwt="$(token)"
    deadline=$((SECONDS + timeout))
    while :; do
        state="$(curl -fsS -H "Authorization: Bearer ${jwt}" "${BASE_URL}/api/v2/dags/${dag_id}/dagRuns/${run_id}" |
            "${VENV}/bin/python" -c 'import json,sys; print(json.load(sys.stdin)["state"])')"
        case "${state}" in
            success | failed) break ;;
        esac
        ((SECONDS < deadline)) || { echo "timed out, run state: ${state}"; break; }
        sleep 3
    done
    echo "run ${dag_id}/${run_id}: ${state}"
    curl -fsS -H "Authorization: Bearer ${jwt}" \
        "${BASE_URL}/api/v2/dags/${dag_id}/dagRuns/${run_id}/taskInstances?limit=200" |
        "${VENV}/bin/python" -c "${TABLE_PY}"
    [[ "${state}" == "success" ]]
}

main() {
    local cmd="${1:-}"
    shift || true
    case "${cmd}" in
        start) cmd_start ;;
        stop) cmd_stop ;;
        status) cmd_status ;;
        seed) ensure_venv; load_env; cmd_seed ;;
        logs) cmd_logs "$@" ;;
        trigger) cmd_trigger "$@" ;;
        wait) cmd_wait "$@" ;;
        *) sed -n '19,24p' "${BASH_SOURCE[0]}"; exit 2 ;;
    esac
}

main "$@"
