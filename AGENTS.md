# Repository instructions

- This service owns PostgreSQL, workflow versions, scheduling, run orchestration, recordings, and persisted events. Agents executes VM sessions; the platform talks to the backend.
- Preserve local-workspace behavior, service-to-service token boundaries, version conflict handling, cancellation, recovery, and history retention when changing APIs or migrations.
- Never log secrets or resolved placeholder values. Treat recordings and live-view URLs as sensitive.
- Use the existing `pnpm typecheck`, `pnpm build`, and `pnpm format:check` checks as appropriate. Follow the user's instructions before incurring provider usage or modifying tests.
- Do not create or modify README.md or other documentation files unless the user explicitly requests it.
