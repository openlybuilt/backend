import { createHash } from "node:crypto";
import type pg from "pg";
import type { VerifiedIdentity } from "../workspaces/store.js";
import { AgentsClient } from "../runs/agents-client.js";
import { IdentityClient } from "../identity/client.js";

export function identityHash(identity: VerifiedIdentity): string {
  return createHash("sha256")
    .update(JSON.stringify([identity.issuer, identity.subject]))
    .digest("hex");
}

type Entry = { blocked: boolean; active: number; drained?: () => void };
export class AccountGate {
  private readonly entries = new Map<string, Entry>();
  enter(identity: VerifiedIdentity): (() => void) | undefined {
    const key = identityHash(identity);
    const entry = this.entries.get(key) ?? { blocked: false, active: 0 };
    this.entries.set(key, entry);
    if (entry.blocked) return;
    entry.active++;
    return () => {
      entry.active--;
      if (entry.active === 0) {
        entry.drained?.();
        if (!entry.blocked) this.entries.delete(key);
      }
    };
  }
  async block(identity: VerifiedIdentity): Promise<void> {
    const key = identityHash(identity);
    const entry = this.entries.get(key) ?? { blocked: false, active: 0 };
    this.entries.set(key, entry);
    if (entry.blocked) throw new Error("Account deletion already requested");
    entry.blocked = true;
    if (entry.active)
      await new Promise<void>((resolve) => {
        entry.drained = resolve;
      });
  }
  unblock(identity: VerifiedIdentity): void {
    this.entries.delete(identityHash(identity));
  }
}

type Job = {
  identity_hash: string;
  issuer: string;
  subject: string;
  workspace_id: string | null;
  state: string;
};
export class AccountDeletionService {
  private timer?: ReturnType<typeof setInterval>;
  private sweep?: Promise<void>;
  constructor(
    private readonly pool: pg.Pool,
    private readonly agents: AgentsClient,
    private readonly auth: IdentityClient,
  ) {}
  async queue(identity: VerifiedIdentity, workspaceId: string): Promise<void> {
    await this.pool.query(
      "INSERT INTO account_deletions(identity_hash,issuer,subject,workspace_id,state) VALUES($1,$2,$3,$4,'pending') ON CONFLICT (identity_hash) DO NOTHING",
      [identityHash(identity), identity.issuer, identity.subject, workspaceId],
    );
    this.kick();
  }
  async status(identity: VerifiedIdentity): Promise<string | undefined> {
    const result = await this.pool.query<{ state: string }>(
      "SELECT state FROM account_deletions WHERE identity_hash=$1",
      [identityHash(identity)],
    );
    return result.rows[0]?.state;
  }
  start(): void {
    this.timer = setInterval(() => this.kick(), 5000);
    this.timer.unref();
    this.kick();
  }
  private kick(): void {
    if (this.sweep) return;
    this.sweep = this.process()
      .catch(() => console.error("Account deletion processing failed"))
      .finally(() => {
        this.sweep = undefined;
      });
  }
  async stop(): Promise<void> {
    clearInterval(this.timer);
    await this.sweep;
  }
  private async process(): Promise<void> {
    const jobs = await this.pool.query<Job>(
      "SELECT identity_hash,issuer,subject,workspace_id,state FROM account_deletions WHERE state IN ('pending','data_deleted') ORDER BY updated_at LIMIT 100",
    );
    for (const job of jobs.rows) {
      try {
        await this.processOne(job);
      } catch {
        console.error("Account deletion will be retried");
        await this.pool.query(
          "UPDATE account_deletions SET updated_at=now() WHERE identity_hash=$1",
          [job.identity_hash],
        );
      }
    }
  }
  private async processOne(job: Job): Promise<void> {
    if (job.state === "pending") {
      const ids = await this.pool.query<{
        id: string;
        status: string;
        result: unknown;
        contains_secrets: boolean;
      }>(
        "SELECT id,status,result,contains_secrets FROM backend_runs WHERE workspace_id=$1",
        [job.workspace_id],
      );
      let active = false;
      for (const { id, status, result, contains_secrets } of ids.rows) {
        const remote = await this.agents.request(`/runs/${id}`, "GET");
        if (remote) {
          if (remote.id !== id) throw new Error("Invalid agents response");
          await this.agents.request(`/runs/${id}/cancel`, "POST");
          active = true;
        } else if (
          !["completed", "failed", "cancelled"].includes(status) ||
          !result ||
          (status === "failed" && contains_secrets) ||
          (typeof result === "object" && "cleanupError" in result)
        ) {
          // A missing in-memory run is not evidence its browser was cleaned up.
          active = true;
        }
      }
      if (active) {
        await this.pool.query(
          "UPDATE account_deletions SET updated_at=now() WHERE identity_hash=$1",
          [job.identity_hash],
        );
        return;
      }
      const client = await this.pool.connect();
      try {
        await client.query("BEGIN");
        // Provider cleanup is not implemented yet; retain references and do not
        // claim account deletion completed for runs that could have replays.
        const recordingColumn = await client.query(
          "SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name='backend_runs' AND column_name='recording_enabled'",
        );
        const providerCleanup = await client.query(
          `SELECT 1 FROM backend_runs WHERE workspace_id=$1 AND workflow_id IS NOT NULL ${recordingColumn.rowCount ? "AND recording_enabled" : ""} LIMIT 1`,
          [job.workspace_id],
        );
        await client.query(
          "INSERT INTO account_deletion_replays(identity_hash,run_id,browser_id,replay_id) SELECT $1,r.run_id,r.browser_id,r.replay_id FROM run_recordings r JOIN backend_runs b ON b.id=r.run_id WHERE b.workspace_id=$2 ON CONFLICT DO NOTHING",
          [job.identity_hash, job.workspace_id],
        );
        await client.query("DELETE FROM backend_runs WHERE workspace_id=$1", [
          job.workspace_id,
        ]);
        await client.query(
          "DELETE FROM workflow_versions WHERE workflow_id IN (SELECT id FROM workflows WHERE workspace_id=$1)",
          [job.workspace_id],
        );
        await client.query("DELETE FROM workflows WHERE workspace_id=$1", [
          job.workspace_id,
        ]);
        await client.query("DELETE FROM placeholders WHERE workspace_id=$1", [
          job.workspace_id,
        ]);
        await client.query("DELETE FROM workspaces WHERE id=$1", [
          job.workspace_id,
        ]);
        await client.query(
          "DELETE FROM identities WHERE issuer=$1 AND subject=$2",
          [job.issuer, job.subject],
        );
        await client.query(
          "UPDATE account_deletions SET state=$2,workspace_id=NULL,updated_at=now() WHERE identity_hash=$1",
          [
            job.identity_hash,
            providerCleanup.rowCount
              ? "provider_cleanup_pending"
              : "data_deleted",
          ],
        );
        await client.query("COMMIT");
        if (providerCleanup.rowCount) return;
      } catch (error) {
        await client.query("ROLLBACK").catch(() => undefined);
        throw error;
      } finally {
        client.release();
      }
    }
    await this.auth.deleteUser(job.subject);
    await this.pool.query(
      "UPDATE account_deletions SET state='deleted',issuer=NULL,subject=NULL,workspace_id=NULL,updated_at=now() WHERE identity_hash=$1",
      [job.identity_hash],
    );
  }
}
