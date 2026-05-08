# VS Code 1.119 workflow

This workspace includes VS Code settings and tasks for the 1.119 agent workflow.

## Browser sharing for frontend review

1. Run the `frontend: dev operator dashboard` task.
2. Open `http://127.0.0.1:5174` in the VS Code integrated browser.
3. Attach that browser tab to chat when asking an agent to inspect or change the dashboard.
4. Stop sharing from the browser sharing control when the review is done.

This keeps the operator dashboard available for visual checks while allowing the agent to reload and inspect the live page in context.

## Agent observability

OpenTelemetry is configured but disabled by default because it needs a local or remote OTLP collector.

To enable it, update `.vscode/settings.json`:

```json
{
  "github.copilot.chat.otel.enabled": true,
  "github.copilot.chat.otel.otlpEndpoint": "http://127.0.0.1:4318/v1/traces"
}
```

Use this for long agent sessions where token usage, latency, tool calls, and model behavior need to be inspected.

## Agent execution defaults

The workspace opts into:

- background todo management for lower token overhead in long agent sessions;
- model detail badges for Copilot CLI and Claude agent responses;
- `allowNetwork` sandbox mode where VS Code and organization policy permit it;
- default outside-workspace write detection, which keeps non-temp external writes guarded.

## Common validation tasks

Use `Terminal: Run Task` for:

- `frontend: e2e`
- `python: tests`
- `rust: tests`
- `repo: check all`

The full check mirrors `scripts/check_all.sh` and covers Rust, Python, generated OpenAPI types, frontend typechecking, Playwright, and build.

## Markdown review

For files in `docs/`, use `Markdown: Switch to Preview View` to replace the current editor with preview, then `Switch to Editor View` to return to source.
