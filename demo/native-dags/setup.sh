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
# Builds the native Dag bundles of the Acme Retail demo and writes the Breeze configuration that
# runs them. Safe to run again.
#
# Run it on the host, from anywhere in the worktree (needs Node.js 22+ and pnpm):
#
#   demo/native-dags/setup.sh [storefront] [risk] [finance]
#
# With no argument it sets up every team. It can also run inside Breeze if node and pnpm are there.
#
# What it writes, all under the git-ignored files/ directory that Breeze mounts as /files:
#   files/bundles/<team>/                          the packed bundle of each team
#   files/airflow-breeze-config/environment_variables.env   Dag bundles and [sdk] coordinators
#   files/airflow-breeze-config/init.sh            seeds Connections and Variables when Breeze starts
#   files/demo/seed.sh, files/demo/seed/*.json     what init.sh runs, re-runnable by hand
#
# An existing environment_variables.env or init.sh is kept: this script owns only the block between
# its BEGIN and END markers, and backs the file up as <name>.bak.<timestamp> before changing it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
FILES_DIR="${REPO_ROOT}/files"
BUNDLES_DIR="${FILES_DIR}/bundles"
BREEZE_CONFIG_DIR="${FILES_DIR}/airflow-breeze-config"
DEMO_DIR="${FILES_DIR}/demo"

BLOCK_BEGIN="# BEGIN native-dags-demo (managed by demo/native-dags/setup.sh, edits inside are overwritten)"
BLOCK_END="# END native-dags-demo"

ALL_TEAMS=(storefront risk finance)

die() {
    echo "setup.sh: $*" >&2
    exit 1
}

require_tools() {
    command -v node >/dev/null || die "node is required (version 22 or later)"
    node -e 'process.exit(Number(process.versions.node.split(".")[0]) >= 22 ? 0 : 1)' \
        || die "node $(node --version) is too old, the TypeScript SDK needs 22 or later"
    command -v pnpm >/dev/null || die "pnpm is required (try: corepack enable pnpm)"
}

build_ts_sdk() {
    local sdk="${REPO_ROOT}/ts-sdk"
    local marker="${sdk}/dist/cli/main.js"
    if [[ ! -f "${marker}" || -n "$(find "${sdk}/src" -type f -newer "${marker}" -print -quit)" ]]; then
        echo "Building the TypeScript SDK"
        (cd "${sdk}" && pnpm install --frozen-lockfile && pnpm run build)
    else
        echo "The TypeScript SDK is up to date"
    fi
}

build_storefront() {
    echo "Building storefront (TypeScript)"
    build_ts_sdk
    local project="${SCRIPT_DIR}/storefront-ts"
    local out="${BUNDLES_DIR}/storefront"
    mkdir -p "${out}"
    (
        cd "${project}"
        # The SDK is a file: dependency, which pnpm copies, so install again to pick up SDK changes.
        pnpm install
        pnpm run typecheck
        pnpm exec airflow-ts-pack src/main.ts --outdir "${out}"
    )
}

build_risk() {
    mkdir -p "${BUNDLES_DIR}/risk"
    echo "risk (Java): not built yet"
}

build_finance() {
    mkdir -p "${BUNDLES_DIR}/finance"
    echo "finance (Go): not built yet"
}

# Replaces the managed block of $1 with stdin, keeping whatever else the file holds.
write_managed_block() {
    local target="$1"
    local block outside current updated
    block="$(cat)"
    outside=""
    current=""
    if [[ -f "${target}" ]]; then
        current="$(cat "${target}")"
        outside="$(awk -v begin="${BLOCK_BEGIN}" -v end="${BLOCK_END}" \
            '$0 == begin { skip = 1; next } $0 == end { skip = 0; next } !skip' "${target}")"
    fi
    updated="${BLOCK_BEGIN}"$'\n'"${block}"$'\n'"${BLOCK_END}"
    if [[ -n "${outside//[[:space:]]/}" ]]; then
        updated="${outside}"$'\n\n'"${updated}"
    fi
    if [[ "${current}" == "${updated}" ]]; then
        echo "Unchanged: ${target}"
        return
    fi
    if [[ -n "${outside//[[:space:]]/}" ]]; then
        local backup="${target}.bak.$(date +%Y%m%d%H%M%S)"
        cp -p "${target}" "${backup}"
        echo "Backed up ${target} to ${backup}"
    fi
    printf '%s\n' "${updated}" >"${target}"
    echo "Wrote ${target}"
}

write_environment_file() {
    # The classpaths and kwargs are those of airflow-core/docs/authoring-and-scheduling/language-sdks.
    # Native Dags ignore task_handler_bundle_name, and each coordinator class appears once, so
    # dag_bundle_to_coordinator is not needed. java and node are on PATH in the Breeze CI image.
    local bundle_class="airflow.dag_processing.bundles.local.LocalDagBundle"
    local bundles='['
    bundles+='{"name": "dags-folder", "classpath": "'"${bundle_class}"'", "kwargs": {}},'
    bundles+='{"name": "storefront", "classpath": "'"${bundle_class}"'", "kwargs": {"path": "/files/bundles/storefront", "refresh_interval": 30}},'
    bundles+='{"name": "risk", "classpath": "'"${bundle_class}"'", "kwargs": {"path": "/files/bundles/risk", "refresh_interval": 30}},'
    bundles+='{"name": "finance", "classpath": "'"${bundle_class}"'", "kwargs": {"path": "/files/bundles/finance", "refresh_interval": 30}}'
    bundles+=']'

    local coordinators='{'
    coordinators+='"ts": {"classpath": "airflow.sdk.coordinators.node.NodeCoordinator", "kwargs": {"node_executable": "node"}},'
    coordinators+='"jdk": {"classpath": "airflow.sdk.coordinators.java.JavaCoordinator", "kwargs": {"java_executable": "java"}},'
    coordinators+='"go": {"classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator", "kwargs": {}}'
    coordinators+='}'

    local queues='{"typescript": "ts", "java": "jdk", "golang": "go"}'

    mkdir -p "${BREEZE_CONFIG_DIR}"
    {
        printf "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='%s'\n" "${bundles}"
        printf "AIRFLOW__SDK__COORDINATORS='%s'\n" "${coordinators}"
        printf "AIRFLOW__SDK__QUEUE_TO_COORDINATOR='%s'\n" "${queues}"
    } | write_managed_block "${BREEZE_CONFIG_DIR}/environment_variables.env"
}

write_seed_files() {
    mkdir -p "${DEMO_DIR}/seed" "${DEMO_DIR}/lake" "${DEMO_DIR}/outbox"

    cat >"${DEMO_DIR}/seed/connections.json" <<'JSON'
{
  "storefront_api": {
    "conn_type": "http",
    "host": "storefront.internal.acme",
    "schema": "https",
    "description": "Storefront order API. The demo only reads the host, there is no real API."
  },
  "payments_gateway": {
    "conn_type": "http",
    "host": "payments.internal.acme",
    "schema": "https",
    "login": "risk-screening",
    "password": "demo-only-not-a-secret",
    "description": "Payments gateway used by the risk team."
  }
}
JSON

    cat >"${DEMO_DIR}/seed/variables.json" <<'JSON'
{
  "storefront.fx_rates": {
    "value": "{\"USD\": 1, \"EUR\": 1.08, \"GBP\": 1.27, \"JPY\": 0.0067}",
    "description": "US dollars per unit of each currency, used by the storefront validation."
  },
  "storefront.inject_fraud": {
    "value": "false",
    "description": "Set to true to make the next storefront export contain fraud patterns."
  },
  "storefront.low_stock": {
    "value": "false",
    "description": "Set to true for low stock, or stockout to also empty a promoted SKU."
  },
  "risk.model_weights": {
    "value": "{\"cross_border_high_value\": 0.45, \"card_velocity\": 0.35, \"new_account_high_value\": 0.2, \"block_threshold\": 0.6}",
    "description": "Rule weights and block threshold for risk_fraud_screening."
  },
  "finance.fx_rates": {
    "value": "{\"USD\": 1, \"EUR\": 1.085, \"GBP\": 1.265, \"JPY\": 0.00668}",
    "description": "Closing rates used by finance_revenue_close."
  },
  "finance.force_month_end": {
    "value": "false",
    "description": "Set to true to run the month-end steps of finance_revenue_close."
  }
}
JSON

    # The handoff Variables are not seeded: the Dags set them.
    cat >"${DEMO_DIR}/seed.sh" <<'SEED'
#!/usr/bin/env bash
# Seeds the demo Connections and Variables and creates the lake and outbox directories.
# Only what is missing is created, so edits made in the UI survive. Run it again after a database reset:
#   bash /files/demo/seed.sh
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "${here}/lake" "${here}/outbox"

import_variables() {
    airflow variables import --action-on-existing-key skip "${here}/seed/variables.json" 2>&1
}

# A database that is not migrated yet makes the import fail, so migrate and try once more.
if ! variables="$(import_variables)"; then
    airflow db migrate >/dev/null 2>&1 || {
        echo "demo seed: the Airflow database is not ready, run bash ${here}/seed.sh later"
        exit 0
    }
    variables="$(import_variables)"
fi
connections="$(airflow connections import "${here}/seed/connections.json" 2>&1)"

printf '%s\n%s\n' "${connections}" "${variables}" | grep -v "already exist" | sed 's/^/demo seed: /'
exit 0
SEED
    chmod +x "${DEMO_DIR}/seed.sh"
}

write_init_script() {
    write_seed_files
    cat <<'INIT' | write_managed_block "${BREEZE_CONFIG_DIR}/init.sh"
if [[ -f /files/demo/seed.sh ]]; then
    bash /files/demo/seed.sh || true
fi
INIT
}

main() {
    local teams=("$@")
    if [[ ${#teams[@]} -eq 0 ]]; then
        teams=("${ALL_TEAMS[@]}")
    fi
    local team
    for team in "${teams[@]}"; do
        case "${team}" in
            storefront | risk | finance) ;;
            -h | --help)
                sed -n '19,35p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
                exit 0
                ;;
            *) die "unknown team '${team}', expected one of: ${ALL_TEAMS[*]}" ;;
        esac
    done

    require_tools
    mkdir -p "${BUNDLES_DIR}"
    for team in "${teams[@]}"; do
        "build_${team}"
    done
    write_environment_file
    write_init_script
    echo
    echo "Done. Start Breeze with: breeze start-airflow"
}

main "$@"
