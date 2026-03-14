# Common AI Provider — Development Guide

Located at `providers/common/ai/`. This provider wraps
[pydantic-ai](https://ai.pydantic.dev/) to connect Airflow pipelines to LLMs.
The hook is a thin bridge between Airflow connections and pydantic-ai's model/provider
abstractions.

## Design Principles

- **Delegate to pydantic-ai.** pydantic-ai supports 20+ providers (OpenAI, Anthropic, Google,
  Azure, Bedrock, Ollama, etc.) via `infer_model()` and provider classes. Do not re-implement
  provider-specific logic that pydantic-ai handles.
  Before writing new code, check: https://ai.pydantic.dev/models/
- **Keep the hook thin.** `PydanticAIHook.get_conn()` maps Airflow connection fields to
  pydantic-ai constructors. That is the hook's entire job. No extra abstraction layers.
- **No premature abstraction.** No Protocols, builder patterns, or plugin systems for a single
  code path. Wait until 3+ concrete use cases exist.
- **Operators stay focused.** Each operator does one thing: `LLMOperator` (prompt -> output),
  `LLMBranchOperator` (prompt -> branch decision), `LLMSQLOperator` (prompt -> validated SQL).
- **One backend per toolset.** A toolset wraps a single execution backend. If a new backend needs
  the same interface, create a new toolset class — don't branch in every method. Users compose
  via `AgentOperator(toolsets=[...])`.

## Adding Support for a New LLM Provider

If pydantic-ai already supports the provider:

1. **Do nothing in this package.** Users set the `provider:model` string in their connection
   (e.g. `azure:gpt-4o`, `bedrock:anthropic.claude-sonnet-4-20250514`) and the hook resolves it
   via `infer_model()`.
2. If the provider needs credentials beyond `api_key` and `base_url`, add a branch in
   `get_conn()` using pydantic-ai's own provider class.
3. Update the connection form docs if new fields are needed.

If pydantic-ai does *not* support the provider, contribute upstream to pydantic-ai.

## Adding a New Toolset

1. For SQL-like access, follow the four-tool pattern from `SQLToolset`: `list_tables`,
   `get_schema`, `query`, `check_query`.
2. Extract shared logic to helpers in the toolsets package rather than inheritance.
3. Keep constructors focused — only accept params that apply to the backend.
4. Mark tools `sequential=True` when the backend uses synchronous I/O.

## Security

- **No dynamic imports from connection extras.** Never use `importlib.import_module()` on
  user-provided strings. Connection extras must not become a code execution vector.
- **SQL validation is on by default.** `LLMSQLOperator` validates via AST parsing. Don't disable.

## Pitfalls

- Do not construct raw provider SDK clients — use pydantic-ai's provider classes.
- Do not add provider-specific connection types. The single `pydantic_ai` connection type works
  for all providers via the `provider:model` format.
- Use `from airflow.providers.common.compat.sdk import ...` for SDK imports, never
  `from airflow.sdk import ...` directly.

## Key Paths

- Hook: `src/airflow/providers/common/ai/hooks/pydantic_ai.py`
- Operators: `src/airflow/providers/common/ai/operators/`
- Decorators: `src/airflow/providers/common/ai/decorators/`
- Toolsets: `src/airflow/providers/common/ai/toolsets/`
- Tests: `tests/unit/common/ai/`
- Docs: `docs/`
