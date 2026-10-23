import { expect, it } from "vitest";
import { createAgentDirectory, type AgentGrant } from "../src/fleet.js";
import {
  machineAgentCodecs,
  machineAgentReservations,
} from "../src/delegation.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("lends the machine link Shiitake's own Session-creation and receipt grammar", () => {
  expect(machineAgentCodecs.sessionCreation({ requestId: "create-1" })).toEqual(
    { requestId: "create-1" },
  );
  expect(
    machineAgentCodecs.sessionCreation({ requestId: "create-1", extra: true }),
  ).toBeNull();
  expect(machineAgentCodecs.sessionCreation({ requestId: "" })).toBeNull();
  expect(() => {
    machineAgentCodecs.accepted({ status: "accepted", commandId: "work-1" });
  }).not.toThrow();
  expect(() => {
    machineAgentCodecs.accepted({ status: "accepted", requestId: "caller-1" });
  }).toThrow();
  expect(() =>
    machineAgentCodecs.settlement('{"status":"accepted"}'),
  ).toThrow();
});

it("reserves one directory identity per enrollment receipt, created by the enrolling member", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    const reserve = machineAgentReservations(fixture.owner.sql);
    const reservation = {
      memberId: "member-1",
      requestId: "device-1:0",
      name: "Laptop Agent",
    };
    const first = reserve(reservation);
    expect(reserve(reservation)).toEqual(first);
    expect(() => reserve({ ...reservation, name: "Other" })).toThrow(
      "request_conflict",
    );
    const reader: AgentGrant = {
      agents: "all",
      actions: ["read"],
      expiresAt: Date.now() + 60_000,
      provenance: { actor: "auditor" },
    };
    const directory = createAgentDirectory(fixture.owner.sql);
    expect(directory.list(reader).map(({ agentId }) => agentId)).toEqual([
      first.agentId,
    ]);
    expect(
      directory.readCreator(
        reader,
        directory.list(reader)[0]?.agentId ?? first.agentId,
      ),
    ).toBe("member-1");
  } finally {
    fixture.close();
  }
});
