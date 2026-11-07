# Openly backend

The backend is the local orchestrator and the only service that owns persistent data. It stores workflows and versions, runs, recordings, event logs, placeholders, schedules, and the local workspace in PostgreSQL. It starts and stops workflow execution through the agents service; it does not operate the VM itself.

For full setup and the other repositories, see [computer-use](https://github.com/openlybuilt/computer-use). For a coding agent's workflow-building and debugging guide, see [Working with workflows as an agent](https://github.com/openlybuilt/computer-use/blob/main/docs/agent-workflows.md).

## Requirements and configuration

Use Node.js 24, pnpm 11.25.0, PostgreSQL, and a configured agents service. The backend listens on `127.0.0.1`, port `3000` by default.

| Variable                     | Purpose                                                                                                   |
| ---------------------------- | --------------------------------------------------------------------------------------------------------- |
| `PORT`                       | Backend port; default `2120`.                                                                             |
| `DATABASE_URL`               | PostgreSQL connection string for the local workspace.                                                     |
| `AGENTS_URL`                 | Agents loopback origin; default `http://127.0.0.1:2130`.                                                  |
| `AGENTS_API_TOKEN`           | Shared bearer token used to call agents.                                                                  |
| `AGENTS_CALLBACK_TOKEN`      | Shared bearer token used to authenticate agent callbacks.                                                 |
| `PLACEHOLDER_ENCRYPTION_KEY` | Base64-encoded 32-byte key for saved placeholder values. Keep it stable so saved values can be decrypted. |

Both service tokens must match the agents configuration and be 32–256 URL-safe characters. `AGENTS_URL` must be an HTTP origin on `127.0.0.1`. Keep credentials and encryption keys outside the repository.

## Run locally

Create a PostgreSQL database and configure the environment, then:

```sh
pnpm install --frozen-lockfile
pnpm build
pnpm start
```

The backend applies its SQL migrations at startup. Use a dedicated local database; the migration from the former signed-in workspace schema removes legacy signed-in workspace data. `GET /health` checks the process, and `GET /ready` reports database readiness. `GET /app-availability` checks the services needed by the UI.

## Run and diagnostics

The platform calls the backend to create workflows, save versions, start and cancel runs, and retrieve run history. The backend discovers configured providers from agents and sends the selected workflow to agents. Recordings are stored in PostgreSQL, and events are best-effort, sanitized metadata; they do not contain full prompts, page contents, screenshots, or typed values.

The prerecorded guide example is marked separately in history and does not invoke a provider. Real workflows can use provider and model resources and incur charges.

## Checks

```sh
pnpm typecheck
pnpm build
pnpm format:check
```

## License

[MIT](LICENSE).
