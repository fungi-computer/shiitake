import type { WatchdogSqliteOwner } from "@fungi.computer/watchdog";
import { z } from "zod";

/** Fleet identity and receipt storage; the caller owns the enclosing transaction. */
export function createFleetDirectoryStore(sql: WatchdogSqliteOwner["sql"]) {
  const input = { sql };
  input.sql.exec(`CREATE TABLE IF NOT EXISTS hub_team_agents (
        agent_id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        actor_user_id TEXT NOT NULL,
        created_by TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      ) STRICT`);
  // Existing pre-release Agents have no recorded maker. Never infer one from
  // correspondence or actor_user_id (the human credential owner).
  const columns = input.sql
    .exec("PRAGMA table_info(hub_team_agents)")
    .toArray();
  if (!columns.some((column) => column.name === "created_by"))
    input.sql.exec("ALTER TABLE hub_team_agents ADD COLUMN created_by TEXT");
  input.sql.exec(`CREATE TABLE IF NOT EXISTS hub_team_agent_create_receipts (
        request_id TEXT PRIMARY KEY,
        actor_user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        computer_mode TEXT NOT NULL,
        FOREIGN KEY (agent_id) REFERENCES hub_team_agents (agent_id)
      ) STRICT`);
  // Team erasure must retain the DO address after a directory removal.
  input.sql.exec(`CREATE TABLE IF NOT EXISTS hub_team_agent_erasure_ids (
        agent_id TEXT PRIMARY KEY
      ) STRICT`);
  input.sql.exec(`CREATE TABLE IF NOT EXISTS hub_team_agent_erasure_scope (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        complete INTEGER NOT NULL CHECK(complete IN (0,1))
      ) STRICT`);
  input.sql.exec(
    "INSERT OR IGNORE INTO hub_team_agent_erasure_scope(singleton,complete) VALUES (1,1)",
  );

  return {
    read: (agentId: string) =>
      input.sql
        .exec(
          "SELECT agent_id, name, actor_user_id, created_by, created_at, updated_at FROM hub_team_agents WHERE agent_id = ?",
          agentId,
        )
        .toArray()[0],
    readReceipt: (requestId: string) =>
      input.sql
        .exec(
          "SELECT request_id, actor_user_id, name, agent_id, computer_mode FROM hub_team_agent_create_receipts WHERE request_id = ?",
          requestId,
        )
        .toArray()[0],
    findByName: (name: string) =>
      input.sql
        .exec(
          "SELECT agent_id, name, actor_user_id, created_by, created_at, updated_at FROM hub_team_agents WHERE name = ?",
          name,
        )
        .toArray()[0],
    reserve(
      agentId: string,
      name: string,
      actorUserId: string,
      now: number,
      requestId: string,
      configuration: string,
      createdBy: string | null,
    ) {
      input.sql.exec(
        "INSERT INTO hub_team_agents (agent_id, name, actor_user_id, created_by, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        agentId,
        name,
        actorUserId,
        createdBy,
        now,
        now,
      );
      input.sql.exec(
        "INSERT INTO hub_team_agent_erasure_ids(agent_id) VALUES (?)",
        agentId,
      );
      input.sql.exec(
        "INSERT INTO hub_team_agent_create_receipts (request_id, actor_user_id, name, agent_id, computer_mode) VALUES (?, ?, ?, ?, ?)",
        requestId,
        actorUserId,
        name,
        agentId,
        configuration,
      );
    },
    list: () =>
      input.sql
        .exec(
          "SELECT agent_id, name, actor_user_id, created_by, created_at, updated_at FROM hub_team_agents ORDER BY created_at DESC, agent_id DESC",
        )
        .toArray(),
    rename(agentId: string, name: string, now: number) {
      input.sql.exec(
        "UPDATE hub_team_agents SET name = ?, updated_at = ? WHERE agent_id = ?",
        name,
        now,
        agentId,
      );
    },
    listErasureIds,
    erasureRosterComplete,
    readCreator: (agentId: string) =>
      input.sql
        .exec(
          "SELECT actor_user_id FROM hub_team_agents WHERE agent_id = ?",
          agentId,
        )
        .toArray()[0],
    remove(agentId: string) {
      input.sql.exec(
        "DELETE FROM hub_team_agent_create_receipts WHERE agent_id = ?",
        agentId,
      );
      input.sql.exec("DELETE FROM hub_team_agents WHERE agent_id = ?", agentId);
    },
  };
  function listErasureIds(): readonly string[] {
    return input.sql
      .exec("SELECT agent_id FROM hub_team_agent_erasure_ids ORDER BY agent_id")
      .toArray()
      .map(({ agent_id }) => z.string().parse(agent_id));
  }

  function erasureRosterComplete(): boolean {
    const row = input.sql
      .exec(
        "SELECT complete FROM hub_team_agent_erasure_scope WHERE singleton=1",
      )
      .toArray()[0];
    return row?.complete === 1;
  }
}

/** Durable correspondence is frozen before delivery; retries reuse the same row. */
export function createFleetDelegationStore(storage: WatchdogSqliteOwner) {
  storage.sql.exec(`CREATE TABLE IF NOT EXISTS shiitake_fleet_delegations (
    request_id TEXT PRIMARY KEY, input TEXT NOT NULL, mail TEXT NOT NULL, receipt TEXT
  ) STRICT`);
  return {
    read: (requestId: string) =>
      storage.sql
        .exec(
          "SELECT input, mail, receipt FROM shiitake_fleet_delegations WHERE request_id = ?",
          requestId,
        )
        .toArray()[0],
    /** Every delegation this owner has sent and had accepted, newest first. */
    sent: () =>
      storage.sql
        .exec(
          "SELECT input, mail, receipt FROM shiitake_fleet_delegations WHERE receipt IS NOT NULL ORDER BY rowid DESC",
        )
        .toArray(),
    reserve(requestId: string, input: string, mail: string) {
      storage.transactionSync(() => {
        storage.sql.exec(
          "INSERT OR IGNORE INTO shiitake_fleet_delegations(request_id, input, mail) VALUES (?, ?, ?)",
          requestId,
          input,
          mail,
        );
      });
    },
    recordReceipt(requestId: string, receipt: string) {
      storage.transactionSync(() => {
        storage.sql.exec(
          "UPDATE shiitake_fleet_delegations SET receipt = ? WHERE request_id = ?",
          receipt,
          requestId,
        );
      });
    },
  };
}
