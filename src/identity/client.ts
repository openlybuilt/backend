import type { VerifiedIdentity } from "../workspaces/store.js";

export class IdentityClient {
  private readonly endpoint: URL;

  constructor(
    base: string,
    private readonly serviceToken: string,
  ) {
    const url = new URL(base);
    if (
      url.username ||
      url.password ||
      url.pathname !== "/" ||
      url.search ||
      url.hash ||
      (url.protocol !== "https:" &&
        !(url.protocol === "http:" && url.hostname === "127.0.0.1"))
    )
      throw new Error(
        "AUTH_VERIFY_URL must be an HTTPS origin or loopback HTTP origin",
      );
    if (!/^[A-Za-z0-9_-]{32,256}$/.test(serviceToken))
      throw new Error(
        "AUTH_SERVICE_TOKEN must be 32 to 256 URL-safe characters",
      );
    this.endpoint = new URL("/verify", url);
  }

  async deleteUser(subject: string): Promise<void> {
    const response = await fetch(
      new URL(`/internal/users/${encodeURIComponent(subject)}`, this.endpoint),
      {
        method: "DELETE",
        headers: { authorization: `Bearer ${this.serviceToken}` },
        signal: AbortSignal.timeout(10000),
        redirect: "error",
      },
    );
    await response.body?.cancel();
    if (!response.ok) throw new Error("Identity deletion unavailable");
  }

  async verifyDeletion(token: string): Promise<VerifiedIdentity | undefined> {
    return this.verifyRequest(token, "/verify-deletion", true);
  }

  async verify(token: string): Promise<VerifiedIdentity | undefined> {
    return this.verifyRequest(token, "/verify", false);
  }

  private async verifyRequest(
    token: string,
    path: string,
    deletion: boolean,
  ): Promise<VerifiedIdentity | undefined> {
    const response = await fetch(new URL(path, this.endpoint), {
      method: "POST",
      headers: {
        authorization: `Bearer ${this.serviceToken}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ token }),
      signal: AbortSignal.timeout(5000),
      redirect: "error",
    });
    if (response.status === 401 || (deletion && response.status === 403)) {
      await response.body?.cancel();
      return undefined;
    }
    if (!response.ok) {
      await response.body?.cancel();
      throw new Error("Identity verification unavailable");
    }
    const value: unknown = await response.json();
    if (!value || typeof value !== "object" || Array.isArray(value))
      throw new Error("Invalid identity verification response");
    const identity = value as Record<string, unknown>;
    if (
      Object.keys(identity).length !== 2 ||
      typeof identity.issuer !== "string" ||
      !identity.issuer ||
      identity.issuer.length > 2048 ||
      typeof identity.subject !== "string" ||
      !identity.subject ||
      identity.subject.length > 255
    )
      throw new Error("Invalid identity verification response");
    return { issuer: identity.issuer, subject: identity.subject };
  }
}
