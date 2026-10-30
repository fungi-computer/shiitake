import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { fauxModel } from "@fungi.computer/mule/testing";
import { expect, it } from "vitest";

import { appendSessionProgress, type SessionProgress } from "../src/client.js";
import { memoryHost } from "../src/testing.js";

function deferred() {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

for (const mode of ["slow watch", "subscribe(after) mid-reply"] as const) {
  it(`${mode} repairs a lost retained prefix and continues appending the reply`, async () => {
    const text = "abcdefghijklmnopqrstuvwxyz".repeat(6_000);
    const paused = deferred();
    const resume = deferred();
    const model = fauxModel([fauxAssistantMessage(text)], {
      tokenSize: { min: 1024, max: 1024 },
    });
    const host = await memoryHost({
      model: {
        acquire: () =>
          Promise.resolve({
            model: model.model,
            retryable: model.retryable,
            release: () => Promise.resolve(),
            stream: (...args) => {
              const source = model.stream(...args);
              return {
                result: () => source.result(),
                async *[Symbol.asyncIterator]() {
                  let length = 0;
                  let held = false;
                  for await (const event of source) {
                    yield event;
                    if (event.type !== "text_delta") continue;
                    length += event.delta.length;
                    if (!held && length >= 100_000) {
                      held = true;
                      paused.resolve();
                      await resume.promise;
                    }
                    // Give the real subscription a chance to read each append.
                    await new Promise<void>((resolve) => setImmediate(resolve));
                  }
                },
              };
            },
          }),
        describe: () => Promise.resolve({ contextWindow: 200_000 }),
      },
    });
    const sessionId = "continuity";
    const session = host.agent.session(sessionId);
    const watch = mode === "slow watch" ? await session.watch() : undefined;
    let settling: ReturnType<typeof host.settle> | undefined;
    try {
      await session.send("stream the reply");
      settling = host.settle(sessionId);
      await paused.promise;
      const midway = await session.read();
      expect(midway.progress?.textTruncated).toBe(true);
      expect(midway.progress?.textOffset).toBeGreaterThan(0);
      if (midway.cursor === null) throw new Error("Missing mid-reply cursor");
      const changes =
        watch?.changes ?? session.subscribe({ after: midway.cursor });
      let progress: SessionProgress | null = watch?.snapshot.progress ?? null;
      let resets = 0;
      let appends = 0;
      let lastEnd = 0;
      for await (const frame of changes) {
        if (frame.kind === "reset") {
          progress = frame.baseline.progress;
          resets += 1;
          expect(progress?.textTruncated).toBe(true);
          resume.resolve();
        } else if (frame.kind === "progress") {
          progress = appendSessionProgress(progress, frame.progress);
          expect(progress?.revision).toBe(frame.progress.revision);
          appends += 1;
        } else if (frame.kind === "update" && frame.terminals.length > 0) {
          break;
        }
        if (progress !== null) {
          const end = progress.textOffset + progress.text.length;
          expect(end).toBeGreaterThanOrEqual(lastEnd);
          expect(progress.text).toBe(text.slice(progress.textOffset, end));
          lastEnd = end;
        }
      }
      await settling;
      expect(resets).toBeGreaterThan(0);
      expect(appends).toBeGreaterThan(1);
      expect(lastEnd).toBeGreaterThan(150_000);
      expect((await session.read()).messages.at(-1)?.value).toMatchObject({
        role: "assistant",
        content: [{ type: "text", text }],
      });
    } finally {
      resume.resolve();
      await settling;
      await watch?.close();
      await host.close();
    }
  });
}
