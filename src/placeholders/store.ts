import {
  createCipheriv,
  createDecipheriv,
  createHmac,
  randomBytes,
} from "node:crypto";
import type pg from "pg";

const namePattern = /^[A-Za-z][A-Za-z0-9_]{0,49}$/;
type Row = {
  name: string;
  secret: boolean;
  ciphertext: Buffer;
  nonce: Buffer;
  tag: Buffer;
};
export type Placeholder = { name: string; secret: boolean; value?: string };

export class PlaceholderStore {
  private readonly key: Buffer;
  constructor(
    private readonly pool: pg.Pool,
    encodedKey: string,
  ) {
    if (!/^[A-Za-z0-9+/]{43}=$/.test(encodedKey))
      throw new Error(
        "PLACEHOLDER_ENCRYPTION_KEY must be a base64-encoded 32-byte key",
      );
    this.key = Buffer.from(encodedKey, "base64");
    if (this.key.length !== 32 || this.key.toString("base64") !== encodedKey)
      throw new Error("Invalid placeholder encryption key");
  }
  private aad(workspace: string, name: string, secret: boolean): Buffer {
    return Buffer.from(JSON.stringify([workspace, name, secret]));
  }
  fingerprint(value: unknown): string {
    return createHmac("sha256", this.key)
      .update("virela-run-fingerprint-v1\0")
      .update(JSON.stringify(value))
      .digest("hex");
  }
  private decrypt(workspace: string, row: Row): string {
    const cipher = createDecipheriv("aes-256-gcm", this.key, row.nonce);
    cipher.setAAD(this.aad(workspace, row.name, row.secret));
    cipher.setAuthTag(row.tag);
    return Buffer.concat([
      cipher.update(row.ciphertext),
      cipher.final(),
    ]).toString("utf8");
  }
  async list(workspace: string): Promise<Placeholder[]> {
    const rows = await this.pool.query<Row>(
      "SELECT name,secret,ciphertext,nonce,tag FROM placeholders WHERE workspace_id=$1 ORDER BY name",
      [workspace],
    );
    return rows.rows.map((row) => ({
      name: row.name,
      secret: row.secret,
      ...(!row.secret ? { value: this.decrypt(workspace, row) } : {}),
    }));
  }
  async save(workspace: string, input: unknown): Promise<Placeholder> {
    if (!input || typeof input !== "object" || Array.isArray(input))
      throw new Error("Invalid placeholder");
    const obj = input as Record<string, unknown>;
    if (
      Object.keys(obj).some((k) => !["name", "secret", "value"].includes(k)) ||
      typeof obj.name !== "string" ||
      !namePattern.test(obj.name) ||
      typeof obj.secret !== "boolean" ||
      typeof obj.value !== "string" ||
      !obj.value.length ||
      Buffer.byteLength(obj.value) > 10000
    )
      throw new Error("Invalid placeholder");
    const { name, secret, value } = obj as {
      name: string;
      secret: boolean;
      value: string;
    };
    const nonce = randomBytes(12),
      cipher = createCipheriv("aes-256-gcm", this.key, nonce);
    cipher.setAAD(this.aad(workspace, name, secret));
    const ciphertext = Buffer.concat([
      cipher.update(value, "utf8"),
      cipher.final(),
    ]);
    await this.pool.query(
      "INSERT INTO placeholders (workspace_id,name,secret,ciphertext,nonce,tag) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (workspace_id,name) DO UPDATE SET secret=EXCLUDED.secret,ciphertext=EXCLUDED.ciphertext,nonce=EXCLUDED.nonce,tag=EXCLUDED.tag,updated_at=now()",
      [workspace, name, secret, ciphertext, nonce, cipher.getAuthTag()],
    );
    return { name, secret, ...(!secret ? { value } : {}) };
  }
  async delete(workspace: string, name: string): Promise<boolean> {
    if (!namePattern.test(name)) return false;
    const result = await this.pool.query(
      "DELETE FROM placeholders WHERE workspace_id=$1 AND name=$2",
      [workspace, name],
    );
    return (result.rowCount ?? 0) > 0;
  }
  async resolve(
    workspace: string,
    names: string[],
  ): Promise<{
    values: Record<string, string>;
    secretNames: string[];
    missing: string[];
  }> {
    const rows = await this.pool.query<Row>(
      "SELECT name,secret,ciphertext,nonce,tag FROM placeholders WHERE workspace_id=$1 AND name=ANY($2::text[])",
      [workspace, names],
    );
    const values: Record<string, string> = Object.create(null),
      secretNames: string[] = [];
    for (const row of rows.rows) {
      values[row.name] = this.decrypt(workspace, row);
      if (row.secret) secretNames.push(row.name);
    }
    return {
      values,
      secretNames,
      missing: names.filter((name) => !Object.hasOwn(values, name)),
    };
  }
}
