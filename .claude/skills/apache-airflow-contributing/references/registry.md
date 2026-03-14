# Airflow Registry — Development Guide

The registry is a static site (Eleventy/11ty) that indexes all Airflow providers.
Built in the `apache/airflow` repo, served at `airflow.apache.org/registry/`.

## Core Principles

- **Minimal JavaScript** — theme toggle + progressive enhancement (filters, search).
  No frameworks (React, Vue, etc.).
  Exception: swagger-ui is vendored from `node_modules/swagger-ui-dist` for the API Explorer.
- **Progressive enhancement** approach.
- **Semantic HTML** with proper elements and structure.
- **Semantic CSS** — class names describe content, not appearance. No utility classes.
- **Light/Dark mode** — full support using CSS `color-scheme` and `light-dark()`.
- **Responsive** — mobile-first, works on all screen sizes.

## CSS Architecture

### Design Tokens

Use CSS custom properties from `tokens.css`:
- Colors: `--color-navy-900`, `--color-cyan-400`, etc.
- Spacing: `--space-4` (1rem), `--space-8` (2rem), etc.
- Typography: `--text-base`, `--text-lg`, etc.
- Theme variables: `--bg-primary`, `--text-primary`, `--border-primary`

Always use CSS custom properties, never hardcoded color values.

### Semantic Class Names

Class names answer "what is this?" not "how should it look?"
- Good: `.provider-card`, `.connection-types`, `.dependencies`, `.stats`
- Bad: `.blue-box`, `.flex-row`, `.mt-4`, `.text-lg`

### No Utility Classes

Remove patterns like `.mb-2`, `.flex`, `.grid-cols-2`, `.text-center`, `.bg-gray-100`.
Instead, style contextually in CSS using cascade selectors.

### Light/Dark Mode

Theme computed variables in `tokens.css` with progressive enhancement:
```css
--bg-primary: var(--bg-primary-light);
--bg-primary: light-dark(var(--bg-primary-light), var(--bg-primary-dark));
```

Default theme is dark mode. Toggle sets `document.documentElement.style.colorScheme`.

## Deployment Architecture

1. **Build**: `registry-build.yml` extracts metadata, builds the 11ty site, syncs to S3.
   - Full build: all ~99 providers (~12 min)
   - Incremental: one provider (~30s), merges with existing S3 data
2. **S3 buckets**: `{live|staging}-docs-airflow-apache-org/registry/`
3. **Serving**: Apache HTTPD rewrites `/registry/*` to CloudFront -> S3
4. **Path prefix**: Production `REGISTRY_PATH_PREFIX=/registry/`, local dev `/`

## Data Extraction (`dev/registry/`)

`dev/registry/` is a Python package (workspace member) with shared code in `registry_tools/`.

**Module type definitions** live in `dev/registry/registry_tools/types.py` — the single source
of truth for all module types (operator, hook, sensor, trigger, etc.). The three Python
extraction scripts and the frontend data file (`types.json`) all derive from this module.
To add a new type, add it to `MODULE_TYPES` in `types.py` and run `generate_types_json.py`.

Four scripts produce the JSON data:

| Script | Runs on | Needs providers? | Purpose |
|---|---|---|---|
| `extract_metadata.py` | Host | No | Provider metadata, class names (AST), PyPI stats |
| `extract_versions.py` | Host | No | Per-version metadata from git tags |
| `extract_parameters.py` | Breeze | Yes | `__init__` parameter inspection via MRO |
| `extract_connections.py` | Breeze | Yes | Connection form metadata |

AST parsing is used instead of runtime imports so extraction works without installing
100+ provider packages. Classes are filtered by inheritance from base classes
(`BaseOperator`, `BaseHook`, `BaseSensorOperator`, etc.), not by name suffix.

Documentation URLs come from Sphinx `objects.inv` inventory files, with fallback to
manual construction for unpublished providers.

## Key Files

- `src/css/main.css` — main styles
- `src/css/tokens.css` — design tokens
- `src/` — page templates (index, provider-detail, providers, explore, stats, api-explorer)
- `dev/registry/` — data extraction scripts
- `dev/registry/registry_contract_models.py` — Pydantic contracts for all JSON payloads
- `dev/registry/export_registry_schemas.py` — generates OpenAPI spec from contracts
