import { readFile } from "node:fs/promises";
import pg from "pg";
import type { RunRecord, RunRequest } from "./contracts.js";

export type BackendRun = RunRecord & { cancelRequested: boolean };
export class RunStore {
  readonly pool: pg.Pool;
  private readonly lock: pg.Client;
  constructor(url: string) {
    this.pool = new pg.Pool({ connectionString: url });
    this.lock = new pg.Client({ connectionString: url });
    this.lock.on("error", () => {
      console.error("Database ownership connection lost");
      process.exit(1);
    });
  }
  async initialize(): Promise<void> {
    await this.lock.connect();
    const lock = await this.lock.query(
      "SELECT pg_try_advisory_lock(78422117) AS acquired",
    );
    if (!lock.rows[0].acquired)
      throw new Error("Another backend service owns this database");
    const sql = await readFile(
      new URL("../../migrations/001_backend_runs.sql", import.meta.url),
      "utf8",
    );
    await this.pool.query(sql);
    await this.pool.query(
      "UPDATE backend_runs SET status = 'dispatch_unknown', updated_at = now() WHERE status = 'dispatch_pending'",
    );
  }
  async create(request: RunRequest): Promise<BackendRun> {
    await this.pool.query(
      "INSERT INTO backend_runs (id, instruction, max_steps, timeout_ms, status) VALUES ($1,$2,$3,$4,'dispatch_pending') ON CONFLICT (id) DO NOTHING",
      [request.id, request.instruction, request.maxSteps, request.timeoutMs],
    );
    const record = await this.get(request.id);
    if (!record) throw new Error("Run insert failed");
    if (
      record.instruction !== request.instruction ||
      record.maxSteps !== request.maxSteps ||
      record.timeoutMs !== request.timeoutMs
    )
      throw new Error("Run ID already has different inputs");
    return record;
  }
  async get(id: string): Promise<BackendRun | undefined> {
    const result = await this.pool.query(
      'SELECT id, instruction, max_steps AS "maxSteps", timeout_ms AS "timeoutMs", status, cancel_requested AS "cancelRequested", result, created_at AS "createdAt", updated_at AS "updatedAt" FROM backend_runs WHERE id=$1',
      [id],
    );
    return result.rows[0] as BackendRun | undefined;
  }
  async requestCancel(id: string): Promise<void> {
    await this.pool.query(
      "UPDATE backend_runs SET cancel_requested=true, updated_at=now() WHERE id=$1",
      [id],
    );
  }
  async update(id: string, status: string, result: unknown): Promise<void> {
    await this.pool.query(
      "UPDATE backend_runs SET status=$2, result=COALESCE($3::jsonb,result), updated_at=now() WHERE id=$1 AND status NOT IN ('completed','failed','cancelled','interrupted','outcome_unknown') AND (CASE $2 WHEN 'dispatch_pending' THEN 0 WHEN 'dispatch_unknown' THEN 0 WHEN 'provisioning' THEN 1 WHEN 'running' THEN 2 WHEN 'cancelling' THEN 3 WHEN 'cleaning_up' THEN 3 ELSE 4 END) >= (CASE status WHEN 'dispatch_pending' THEN 0 WHEN 'dispatch_unknown' THEN 0 WHEN 'provisioning' THEN 1 WHEN 'running' THEN 2 WHEN 'cancelling' THEN 3 WHEN 'cleaning_up' THEN 3 ELSE 4 END)",
      [
        id,
        status,
        result === undefined || result === null ? null : JSON.stringify(result),
      ],
    );
  }
  async close(): Promise<void> {
    await this.pool.end();
    await this.lock.end();
  }
}
