import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";

export class DatabaseUnavailableError extends Error {
  constructor() {
    super("Database is currently unavailable");
  }
}

export function isDatabaseFailure(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const { code, message } = error as { code?: string; message?: string };
  return (
    (typeof code === "string" &&
      (/^08/.test(code) ||
        [
          "57P01",
          "57P02",
          "57P03",
          "ECONNREFUSED",
          "ECONNRESET",
          "ETIMEDOUT",
          "EPIPE",
          "ENOTFOUND",
          "EHOSTUNREACH",
        ].includes(code))) ||
    (typeof message === "string" &&
      /Query read timeout|Connection terminated|connection error and is not queryable|timeout exceeded when trying to connect/.test(
        message,
      ))
  );
}

export class Database {
  readonly pool: pg.Pool;
  private owner?: pg.Client;
  private controller = new AbortController();
  private readonly lifetime = new AbortController();
  private task?: Promise<void>;
  private closing = false;
  private available = false;
  private recoveryNeeded = false;
  private onUnavailable?: () => Promise<void>;
  private draining = Promise.resolve();

  constructor(private readonly url: string) {
    this.pool = new pg.Pool({
      connectionString: url,
      connectionTimeoutMillis: 2000,
      query_timeout: 5000,
      statement_timeout: 5000,
    });
    this.pool.on("error", () => this.unavailable());
  }

  get ready(): boolean {
    return this.available;
  }

  get signal(): AbortSignal {
    return this.controller.signal;
  }

  assertReady(signal = this.signal): void {
    if (!this.ready || signal.aborted || signal !== this.signal)
      throw new DatabaseUnavailableError();
  }

  unavailable(): void {
    const firstLoss = !this.signal.aborted;
    this.available = false;
    this.controller.abort();
    if (!this.closing) this.recoveryNeeded = true;
    if (firstLoss && !this.closing && this.onUnavailable)
      this.draining = Promise.resolve()
        .then(this.onUnavailable)
        .catch(() => undefined);
  }

  start(
    prepare: (signal: AbortSignal, recover: boolean) => Promise<void>,
    onUnavailable: () => Promise<void>,
  ): void {
    if (this.task) return;
    this.onUnavailable = onUnavailable;
    this.task = this.connect(prepare).catch(() => this.unavailable());
  }

  private async connect(
    prepare: (signal: AbortSignal, recover: boolean) => Promise<void>,
  ): Promise<void> {
    while (!this.closing) {
      const owner = new pg.Client({
        connectionString: this.url,
        connectionTimeoutMillis: 2000,
        query_timeout: 2000,
        statement_timeout: 2000,
      });
      this.owner = owner;
      this.controller = new AbortController();
      const signal = this.signal;
      owner.on("error", () => {
        if (this.owner === owner) this.unavailable();
      });
      try {
        await owner.connect();
        const lock = await owner.query(
          "SELECT pg_try_advisory_lock(78422117) AS acquired",
        );
        if (!lock.rows[0].acquired) throw new DatabaseUnavailableError();
        await prepare(signal, this.recoveryNeeded);
        signal.throwIfAborted();
        this.available = true;
        this.recoveryNeeded = false;
        while (!this.closing) {
          await delay(1000, undefined, { signal });
          await owner.query("SELECT 1");
          signal.throwIfAborted();
        }
      } catch {
        this.unavailable();
      } finally {
        await this.draining;
        await owner.end().catch(() => undefined);
        if (this.owner === owner) this.owner = undefined;
      }
      await delay(1000, undefined, { signal: this.lifetime.signal });
    }
  }

  async stop(): Promise<void> {
    this.closing = true;
    this.unavailable();
    this.lifetime.abort();
    await this.owner?.end().catch(() => undefined);
    await this.task;
    await this.pool.end();
  }
}
