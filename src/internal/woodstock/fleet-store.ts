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
    request_id TEXT PRIMARY KEY, input TEXT NOT NULL, mail TEXT NOT NULL,
    receipt TEXT
  ) STRICT`);
  storage.sql.exec(`CREATE TABLE IF NOT EXISTS shiitake_fleet_held_delegations (
    request_id TEXT PRIMARY KEY
  ) STRICT`);
  storage.sql
    .exec(`CREATE TABLE IF NOT EXISTS shiitake_fleet_undeliverable_delegations (
    request_id TEXT PRIMARY KEY
  ) STRICT`);
  // A remote settlement confirmed by this owner. Without this local fact an
  // accepted sender receipt remains indistinguishable from outstanding work.
  storage.sql
    .exec(`CREATE TABLE IF NOT EXISTS shiitake_fleet_settled_delegations (
    request_id TEXT PRIMARY KEY
  ) STRICT`);
  return {
    read: (requestId: string) =>
      storage.sql
        .exec(
          `SELECT delegation.input, delegation.mail,
            CASE
              WHEN terminal.request_id IS NOT NULL THEN 'undeliverable'
              WHEN held.request_id IS NOT NULL THEN 'held'
              WHEN delegation.receipt IS NOT NULL THEN 'accepted'
              ELSE 'pending'
            END AS state,
            delegation.receipt
          FROM shiitake_fleet_delegations AS delegation
          LEFT JOIN shiitake_fleet_held_delegations AS held
            ON held.request_id = delegation.request_id
          LEFT JOIN shiitake_fleet_undeliverable_delegations AS terminal
            ON terminal.request_id = delegation.request_id
          WHERE delegation.request_id = ?`,
          requestId,
        )
        .toArray()[0],
    /** Accepted sends not yet observed settled, for owner-local archive admission. */
    unsettled: () =>
      storage.sql
        .exec(
          `SELECT input, mail, 'accepted' AS state, receipt
          FROM shiitake_fleet_delegations AS delegation
          WHERE receipt IS NOT NULL
            AND NOT EXISTS (
              SELECT 1 FROM shiitake_fleet_settled_delegations AS settled
              WHERE settled.request_id = delegation.request_id
            )
          ORDER BY delegation.rowid DESC`,
        )
        .toArray(),
    recordSettled(requestId: string) {
      storage.transactionSync(() => {
        const accepted = storage.sql
          .exec(
            "SELECT 1 FROM shiitake_fleet_delegations WHERE request_id = ? AND receipt IS NOT NULL",
            requestId,
          )
          .toArray();
        if (accepted.length !== 1)
          throw new Error("Cannot record settlement without acceptance");
        storage.sql.exec(
          "INSERT OR IGNORE INTO shiitake_fleet_settled_delegations(request_id) VALUES(?)",
          requestId,
        );
      });
    },
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
    recordReceipt(
      requestId: string,
      receipt: string,
      expectedMail: string,
      acceptedMail: string,
    ) {
      storage.transactionSync(() => {
        const row = storage.sql
          .exec(
            "SELECT mail, receipt FROM shiitake_fleet_delegations WHERE request_id = ?",
            requestId,
          )
          .toArray()[0];
        if (row === undefined)
          throw new Error(
            "Cannot record acceptance without frozen delegation mail",
          );
        if (row.receipt !== null) {
          if (row.receipt === receipt && row.mail === acceptedMail) return;
          throw new Error(
            "Delegation acceptance conflicts with its recorded receipt",
          );
        }
        if (row.mail !== expectedMail)
          throw new Error(
            "Delegation mail changed before acceptance was recorded",
          );
        storage.sql.exec(
          "UPDATE shiitake_fleet_delegations SET mail = ?, receipt = ? WHERE request_id = ? AND receipt IS NULL",
          acceptedMail,
          receipt,
          requestId,
        );
        storage.sql.exec(
          "DELETE FROM shiitake_fleet_held_delegations WHERE request_id = ?",
          requestId,
        );
      });
    },
    recordHeld(requestId: string) {
      storage.transactionSync(() => {
        storage.sql.exec(
          `INSERT OR IGNORE INTO shiitake_fleet_held_delegations (request_id)
          SELECT request_id FROM shiitake_fleet_delegations
          WHERE request_id = ? AND receipt IS NULL
            AND NOT EXISTS (
              SELECT 1 FROM shiitake_fleet_undeliverable_delegations
              WHERE request_id = ?
            )`,
          requestId,
          requestId,
        );
      });
    },
    recordUndeliverable(requestId: string): boolean {
      return storage.transactionSync(() => {
        const held = storage.sql
          .exec(
            `SELECT delegation.request_id
            FROM shiitake_fleet_delegations AS delegation
            JOIN shiitake_fleet_held_delegations AS held
              ON held.request_id = delegation.request_id
            WHERE delegation.request_id = ? AND delegation.receipt IS NULL`,
            requestId,
          )
          .toArray()[0];
        if (held === undefined) return false;
        storage.sql.exec(
          "INSERT OR IGNORE INTO shiitake_fleet_undeliverable_delegations(request_id) VALUES (?)",
          requestId,
        );
        storage.sql.exec(
          "DELETE FROM shiitake_fleet_held_delegations WHERE request_id = ?",
          requestId,
        );
        return true;
      });
    },
  };
}
