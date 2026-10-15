/** Minimal Node test host for importing Shiitake's Cloudflare-only module. */
export class DurableObject<Env> {
  protected readonly ctx: DurableObjectState;
  protected readonly env: Env;

  public constructor(ctx: DurableObjectState, env: Env) {
    this.ctx = ctx;
    this.env = env;
  }
}
