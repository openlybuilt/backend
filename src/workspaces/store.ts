import type pg from "pg";

const LOCAL_WORKSPACE_ID = "00000000-0000-4000-8000-000000000001";

export class WorkspaceStore {
  constructor(private readonly pool: pg.Pool) {}

  async getOrCreate(): Promise<string> {
    const result = await this.pool.query<{ id: string }>(
      "INSERT INTO workspaces (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET id=EXCLUDED.id RETURNING id",
      [LOCAL_WORKSPACE_ID],
    );
    return result.rows[0]!.id;
  }
}
