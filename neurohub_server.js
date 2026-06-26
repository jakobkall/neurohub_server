/**
 * neurohub_server.js — v5.0
 *
 * Nyt i v5.0:
 *  - Hub-system: nh_hubs tabel, privat hub med access_code
 *  - Default-indhold: nh_hub_defaults — pixels + zoner indlæses når hub initialiseres
 *  - Admin-endpoint: opret/slet hub via HTTP API
 *  - Zone-block: server sender 'zone_flash' event tilbage til klienten
 *  - Wave-fix: locked pixels sendes individuelt — ikke zone-rektanglet
 */
"use strict";

const http = require("http");
const crypto = require("crypto");
const { Server } = require("socket.io");
const mysql = require("mysql2/promise");
require("dotenv").config();

const PORT = process.env.PORT || 3001;
const ALLOWED_ORIGINS = (
  process.env.ALLOWED_ORIGINS ||
  "https://jakobkall.com,https://hub.jakobkall.com"
)
  .split(",")
  .map((s) => s.trim());

// ── DB configs ────────────────────────────────────────────────────────────────
const NEUROHUB_DB = {
  host: process.env.DB_HOST || "mysql48.unoeuro.com",
  port: parseInt(process.env.DB_PORT || "3306"),
  user: process.env.DB_USER || "jakobkall_com",
  password: process.env.DB_PASS || "cfDEmaw5n96t",
  database: process.env.DB_NAME || "jakobkall_com_db_neurohub",
  waitForConnections: true,
  connectionLimit: 10,
  charset: "utf8mb4",
};
const USERS_DB = {
  host: process.env.DB_HOST || "mysql48.unoeuro.com",
  port: parseInt(process.env.DB_PORT || "3306"),
  user: process.env.DB_USER || "jakobkall_com",
  password: process.env.DB_PASS || "cfDEmaw5n96t",
  database: process.env.USERS_DB_NAME || "jakobkall_com_db",
  waitForConnections: true,
  connectionLimit: 5,
  charset: "utf8mb4",
};

// ── RAM caches ────────────────────────────────────────────────────────────────
const pixelBuffer = new Map(); // hubId → Map("cx,cy" → {x,y,color,user_id,username,locked})
const erasedKeys = new Map(); // hubId → Set("cx,cy")
const bubbleCache = new Map(); // hubId → Map(id → bubble)
const branchCache = new Map(); // hubId → Set("pid-cid")
const chatCache = new Map(); // hubId → [{username,message,ts}]
const presence = new Map(); // hubId → Map(socketId → {user_id,username,x,y})
const zoneCache = new Map(); // hubId → Map(id → {id,x,y,w,h,label})
const adminCache = new Map(); // userId → bool (only cache hits)
const hubCache = new Map(); // hubId → {id,label,is_private,access_code,owner_id}
const initializedHubs = new Set(); // hubIds that have had defaults applied

const FLUSH_INTERVAL = 20_000;
const FLUSH_BATCH = 200;
const WORLD_W = 4000,
  WORLD_H = 2800;

let pool, usersPool;

// ── Bootstrap ─────────────────────────────────────────────────────────────────
async function bootstrap() {
  pool = await mysql.createPool(NEUROHUB_DB);
  usersPool = await mysql.createPool(USERS_DB);
  console.log("[neurohub] Both DB pools connected");

  try {
    const [rows] = await usersPool.execute("SELECT COUNT(*) AS cnt FROM users");
    console.log(`[neurohub] users table: ${rows[0].cnt} users found`);
  } catch (e) {
    console.error("[neurohub] WARNING: Cannot read users table:", e.message);
  }

  // Ensure schema ──────────────────────────────────────────────────────────────
  await pool
    .execute(
      "ALTER TABLE nh_pixels ADD COLUMN IF NOT EXISTS locked TINYINT(1) NOT NULL DEFAULT 0",
    )
    .catch(() => {});

  try {
    await pool.execute(
      "ALTER TABLE nh_pixels ADD UNIQUE INDEX ux_hub_xy (hub_id,x,y)",
    );
  } catch (e) {}

  await pool
    .execute(
      `
    CREATE TABLE IF NOT EXISTS nh_admin_zones (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
      hub_id VARCHAR(64) NOT NULL DEFAULT 'public',
      x INT NOT NULL, y INT NOT NULL, w INT NOT NULL, h INT NOT NULL,
      label VARCHAR(64) DEFAULT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_hub (hub_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `,
    )
    .catch(() => {});

  await pool
    .execute(
      `
    CREATE TABLE IF NOT EXISTS nh_hubs (
      id          VARCHAR(64) COLLATE utf8mb4_unicode_ci NOT NULL,
      label       VARCHAR(128) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
      is_private  TINYINT(1) NOT NULL DEFAULT 0,
      access_code VARCHAR(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
      owner_id    BIGINT DEFAULT NULL,
      created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `,
    )
    .catch(() => {});

  await pool
    .execute(
      "INSERT IGNORE INTO nh_hubs (id, label, is_private) VALUES ('public','Public Hub',0)",
    )
    .catch(() => {});

  await pool
    .execute(
      `
    CREATE TABLE IF NOT EXISTS nh_hub_defaults (
      id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      hub_id  VARCHAR(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'public',
      type    ENUM('pixel','zone') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'pixel',
      x       SMALLINT UNSIGNED NOT NULL DEFAULT 0,
      y       SMALLINT UNSIGNED NOT NULL DEFAULT 0,
      color   CHAR(7) COLLATE utf8mb4_unicode_ci DEFAULT '#ffffff',
      locked  TINYINT(1) NOT NULL DEFAULT 0,
      w       SMALLINT UNSIGNED DEFAULT NULL,
      h       SMALLINT UNSIGNED DEFAULT NULL,
      label   VARCHAR(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      INDEX idx_hub_type (hub_id, type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `,
    )
    .catch(() => {});

  await warmUp();
  setInterval(flushAllPixels, FLUSH_INTERVAL);
  startServer();
}

// ── Warm-up ───────────────────────────────────────────────────────────────────
async function warmUp() {
  // Hubs
  const [hubs] = await pool.query(
    "SELECT id,label,is_private,access_code,owner_id FROM nh_hubs",
  );
  hubs.forEach((h) => hubCache.set(h.id, h));

  // Pixels
  const [pixels] = await pool.query(
    "SELECT hub_id,x,y,color,user_id,username,IFNULL(locked,0) AS locked FROM nh_pixels LIMIT 500000",
  );
  pixels.forEach((p) => {
    ensureHubBuffers(p.hub_id);
    pixelBuffer.get(p.hub_id).set(`${p.x},${p.y}`, {
      x: p.x,
      y: p.y,
      color: p.color,
      user_id: String(p.user_id),
      username: p.username,
      locked: p.locked ? 1 : 0,
    });
  });
  console.log(`[neurohub] Warmed ${pixels.length} pixels`);

  // Bubbles
  const [bubbles] = await pool.query(
    "SELECT id,hub_id,type,x,y,color,content,emotion,emotion_val,user_id,username FROM nh_bubbles",
  );
  bubbles.forEach((b) => {
    ensureHubBuffers(b.hub_id);
    bubbleCache.get(b.hub_id).set(b.id, { ...b });
  });

  // Branches
  const [branches] = await pool.query(
    "SELECT hub_id,parent_id,child_id FROM nh_branches",
  );
  branches.forEach((br) => {
    ensureHubBuffers(br.hub_id);
    branchCache.get(br.hub_id).add(`${br.parent_id}-${br.child_id}`);
  });

  // Chat
  const [chats] = await pool.query(
    "SELECT hub_id,username,message,UNIX_TIMESTAMP(created_at) AS ts FROM nh_chat ORDER BY id DESC LIMIT 500",
  );
  chats.reverse().forEach((c) => {
    ensureHubBuffers(c.hub_id);
    const cc = chatCache.get(c.hub_id);
    cc.push({ username: c.username, message: c.message, ts: c.ts });
    if (cc.length > 50) cc.shift();
  });

  // Zones
  const [zones] = await pool.query(
    "SELECT id,hub_id,x,y,w,h,label FROM nh_admin_zones",
  );
  zones.forEach((z) => {
    ensureHubBuffers(z.hub_id);
    zoneCache.get(z.hub_id).set(z.id, {
      id: z.id,
      x: z.x,
      y: z.y,
      w: z.w,
      h: z.h,
      label: z.label || "",
    });
  });

  console.log(
    `[neurohub] Warmed ${bubbles.length} bubbles, ${zones.length} admin zones`,
  );
}

function ensureHubBuffers(hubId) {
  if (!pixelBuffer.has(hubId)) pixelBuffer.set(hubId, new Map());
  if (!erasedKeys.has(hubId)) erasedKeys.set(hubId, new Set());
  if (!bubbleCache.has(hubId)) bubbleCache.set(hubId, new Map());
  if (!branchCache.has(hubId)) branchCache.set(hubId, new Set());
  if (!chatCache.has(hubId)) chatCache.set(hubId, []);
  if (!presence.has(hubId)) presence.set(hubId, new Map());
  if (!zoneCache.has(hubId)) zoneCache.set(hubId, new Map());
}

// ── Apply defaults to a fresh hub ─────────────────────────────────────────────
async function applyHubDefaults(hubId) {
  if (initializedHubs.has(hubId)) return;
  initializedHubs.add(hubId);

  // Check if hub already has pixels — if so, skip defaults
  const buf = pixelBuffer.get(hubId);
  if (buf && buf.size > 0) return;

  const [rows] = await pool
    .query("SELECT * FROM nh_hub_defaults WHERE hub_id = ?", [hubId])
    .catch(() => [[]]);

  if (!rows.length) return;

  for (const row of rows) {
    if (row.type === "pixel") {
      const key = `${row.x},${row.y}`;
      buf.set(key, {
        x: row.x,
        y: row.y,
        color: row.color,
        user_id: "0",
        username: "system",
        locked: row.locked ? 1 : 0,
      });
      // Also persist to DB
      await pool
        .execute(
          `INSERT INTO nh_pixels (hub_id,x,y,color,user_id,username,locked)
         VALUES (?,?,?,?,0,'system',?)
         ON DUPLICATE KEY UPDATE color=VALUES(color), locked=VALUES(locked)`,
          [hubId, row.x, row.y, row.color, row.locked ? 1 : 0],
        )
        .catch(() => {});
    } else if (row.type === "zone") {
      const [res] = await pool
        .execute(
          "INSERT INTO nh_admin_zones (hub_id,x,y,w,h,label) VALUES (?,?,?,?,?,?)",
          [hubId, row.x, row.y, row.w || 1, row.h || 1, row.label || ""],
        )
        .catch(() => [{ insertId: null }]);
      if (res.insertId) {
        zoneCache.get(hubId).set(res.insertId, {
          id: res.insertId,
          x: row.x,
          y: row.y,
          w: row.w || 1,
          h: row.h || 1,
          label: row.label || "",
        });
      }
    }
  }
  console.log(`[neurohub] Applied ${rows.length} defaults to hub: ${hubId}`);
}

// ── Admin resolution ──────────────────────────────────────────────────────────
function parseIsAdmin(str) {
  if (!str) return false;
  return str
    .split(",")
    .map((t) => t.trim().toLowerCase())
    .includes("admin");
}

async function resolveAdmin(userId) {
  const key = String(userId);
  if (adminCache.has(key)) return adminCache.get(key);

  try {
    const [rows] = await usersPool.execute(
      "SELECT user_type FROM users WHERE id=? OR user_id=? LIMIT 1",
      [userId, userId],
    );
    if (rows.length > 0) {
      const isAdmin = parseIsAdmin(rows[0].user_type);
      adminCache.set(key, isAdmin);
      return isAdmin;
    }
    adminCache.set(key, false);
    setTimeout(() => adminCache.delete(key), 5 * 60 * 1000);
    return false;
  } catch (e) {
    console.error(`[neurohub] usersPool admin lookup failed:`, e.message);
  }

  try {
    const [rows] = await pool.execute(
      `SELECT user_type FROM \`${USERS_DB.database}\`.users WHERE id=? OR user_id=? LIMIT 1`,
      [userId, userId],
    );
    if (rows.length > 0) {
      const isAdmin = parseIsAdmin(rows[0].user_type);
      adminCache.set(key, isAdmin);
      return isAdmin;
    }
  } catch (e) {}

  return false;
}

// ── Zone helpers ──────────────────────────────────────────────────────────────
function isInLockedZone(hubId, cx, cy) {
  const zones = zoneCache.get(hubId);
  if (!zones) return false;
  for (const z of zones.values()) {
    if (cx >= z.x && cx < z.x + z.w && cy >= z.y && cy < z.y + z.h) return true;
  }
  return false;
}
function getZoneAt(hubId, cx, cy) {
  const zones = zoneCache.get(hubId);
  if (!zones) return null;
  for (const z of zones.values()) {
    if (cx >= z.x && cx < z.x + z.w && cy >= z.y && cy < z.y + z.h) return z;
  }
  return null;
}

// ── Pixel flush ───────────────────────────────────────────────────────────────
async function flushPixels(hubId) {
  const buf = pixelBuffer.get(hubId);
  const erased = erasedKeys.get(hubId);

  if (erased && erased.size > 0) {
    for (const key of erased) {
      const [x, y] = key.split(",").map(Number);
      await pool
        .execute("DELETE FROM nh_pixels WHERE hub_id=? AND x=? AND y=?", [
          hubId,
          x,
          y,
        ])
        .catch((e) => console.error("[neurohub] pixel delete:", e.message));
    }
    erased.clear();
  }

  if (buf && buf.size > 0) {
    const rows = [...buf.values()];
    for (let i = 0; i < rows.length; i += FLUSH_BATCH) {
      const chunk = rows.slice(i, i + FLUSH_BATCH);
      const ph = chunk.map(() => "(?,?,?,?,?,?,?)").join(",");
      const vals = [];
      chunk.forEach((p) =>
        vals.push(
          hubId,
          p.x,
          p.y,
          p.color,
          p.user_id,
          p.username,
          p.locked ? 1 : 0,
        ),
      );
      await pool
        .execute(
          `INSERT INTO nh_pixels (hub_id,x,y,color,user_id,username,locked) VALUES ${ph}
         ON DUPLICATE KEY UPDATE color=VALUES(color),user_id=VALUES(user_id),username=VALUES(username),locked=VALUES(locked)`,
          vals,
        )
        .catch(async (e) => {
          console.error("[neurohub] pixel upsert:", e.message);
          for (const p of chunk) {
            await pool
              .execute(
                `INSERT INTO nh_pixels (hub_id,x,y,color,user_id,username,locked) VALUES (?,?,?,?,?,?,?)
             ON DUPLICATE KEY UPDATE color=VALUES(color),user_id=VALUES(user_id),username=VALUES(username),locked=VALUES(locked)`,
                [
                  hubId,
                  p.x,
                  p.y,
                  p.color,
                  p.user_id,
                  p.username,
                  p.locked ? 1 : 0,
                ],
              )
              .catch((e2) =>
                console.error("[neurohub] pixel individual:", e2.message),
              );
          }
        });
    }
  }
}

async function flushAllPixels() {
  for (const hubId of pixelBuffer.keys())
    await flushPixels(hubId).catch((e) =>
      console.error(`[neurohub] flush(${hubId}):`, e.message),
    );
}

// ── State builder ─────────────────────────────────────────────────────────────
function buildState(hubId) {
  const pixels = [];
  (pixelBuffer.get(hubId) || new Map()).forEach((p) =>
    pixels.push([p.x, p.y, p.color, p.locked ? 1 : 0]),
  );
  const bubbles = [...(bubbleCache.get(hubId) || new Map()).values()];
  const branches = [];
  (branchCache.get(hubId) || new Set()).forEach((k) => {
    const [p, c] = k.split("-").map(Number);
    branches.push([p, c]);
  });
  const chat = (chatCache.get(hubId) || []).slice(-50);
  const zones = [...(zoneCache.get(hubId) || new Map()).values()];
  return { pixels, bubbles, branches, chat, zones };
}

function getPresence(hubId) {
  return presence.get(hubId) || new Map();
}
function presenceList(hubId) {
  return [...getPresence(hubId).values()];
}

// ── HTTP server ───────────────────────────────────────────────────────────────
function startServer() {
  const httpServer = http.createServer(async (req, res) => {
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type,X-Admin-Key");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    // ── Health ────────────────────────────────────────────────────────────────
    if (req.url === "/health" && req.method === "GET") {
      res.writeHead(200);
      res.end(
        JSON.stringify({
          ok: true,
          pixels: [...pixelBuffer.values()].reduce((s, m) => s + m.size, 0),
          hubs: hubCache.size,
        }),
      );
      return;
    }

    // ── List hubs (public info only) ──────────────────────────────────────────
    if (req.url === "/hubs" && req.method === "GET") {
      const list = [...hubCache.values()].map((h) => ({
        id: h.id,
        label: h.label,
        is_private: h.is_private,
      }));
      res.writeHead(200);
      res.end(JSON.stringify(list));
      return;
    }

    // ── Create hub ────────────────────────────────────────────────────────────
    if (req.url === "/hubs" && req.method === "POST") {
      const body = await readBody(req);
      let data;
      try {
        data = JSON.parse(body);
      } catch {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, reason: "bad_json" }));
        return;
      }

      const { id, label, is_private, access_code, owner_id } = data;
      if (!id || !/^[a-z0-9_-]{1,64}$/.test(id)) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, reason: "invalid_id" }));
        return;
      }
      if (hubCache.has(id)) {
        res.writeHead(409);
        res.end(JSON.stringify({ ok: false, reason: "exists" }));
        return;
      }

      const priv = is_private ? 1 : 0;
      const code = priv && access_code ? access_code : null;

      await pool.execute(
        "INSERT INTO nh_hubs (id,label,is_private,access_code,owner_id) VALUES (?,?,?,?,?)",
        [id, label || id, priv, code, owner_id || null],
      );
      const hub = {
        id,
        label: label || id,
        is_private: priv,
        access_code: code,
        owner_id: owner_id || null,
      };
      hubCache.set(id, hub);
      ensureHubBuffers(id);
      res.writeHead(201);
      res.end(
        JSON.stringify({
          ok: true,
          hub: { id, label: hub.label, is_private: priv },
        }),
      );
      return;
    }

    // ── Delete hub ────────────────────────────────────────────────────────────
    if (req.url.startsWith("/hubs/") && req.method === "DELETE") {
      const hubId = req.url.replace("/hubs/", "");
      if (hubId === "public") {
        res.writeHead(403);
        res.end(JSON.stringify({ ok: false, reason: "cannot_delete_public" }));
        return;
      }
      await pool
        .execute("DELETE FROM nh_hubs WHERE id=?", [hubId])
        .catch(() => {});
      hubCache.delete(hubId);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true }));
      return;
    }

    // ── Hub defaults: list ────────────────────────────────────────────────────
    if (req.url.startsWith("/hub-defaults/") && req.method === "GET") {
      const hubId = req.url.replace("/hub-defaults/", "");
      const [rows] = await pool
        .query("SELECT * FROM nh_hub_defaults WHERE hub_id=?", [hubId])
        .catch(() => [[]]);
      res.writeHead(200);
      res.end(JSON.stringify(rows));
      return;
    }

    // ── Hub defaults: save pixels from editor ─────────────────────────────────
    if (req.url.startsWith("/hub-defaults/") && req.method === "POST") {
      const hubId = req.url.replace("/hub-defaults/", "");
      const body = await readBody(req);
      let data;
      try {
        data = JSON.parse(body);
      } catch {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false }));
        return;
      }

      // data.pixels = [{x,y,color,locked}]
      // data.zones  = [{x,y,w,h,label}]
      // data.replace = true → clear existing defaults first

      if (data.replace) {
        await pool
          .execute("DELETE FROM nh_hub_defaults WHERE hub_id=?", [hubId])
          .catch(() => {});
      }

      let inserted = 0;
      if (Array.isArray(data.pixels)) {
        for (const p of data.pixels) {
          await pool
            .execute(
              "INSERT INTO nh_hub_defaults (hub_id,type,x,y,color,locked) VALUES (?,?,?,?,?,?)",
              [
                hubId,
                "pixel",
                p.x,
                p.y,
                p.color || "#ffffff",
                p.locked ? 1 : 0,
              ],
            )
            .catch(() => {});
          inserted++;
        }
      }
      if (Array.isArray(data.zones)) {
        for (const z of data.zones) {
          await pool
            .execute(
              "INSERT INTO nh_hub_defaults (hub_id,type,x,y,w,h,label) VALUES (?,?,?,?,?,?,?)",
              [hubId, "zone", z.x, z.y, z.w, z.h, z.label || ""],
            )
            .catch(() => {});
          inserted++;
        }
      }

      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, inserted }));
      return;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ ok: false, reason: "not_found" }));
  });

  // ── Socket.IO ─────────────────────────────────────────────────────────────
  const io = new Server(httpServer, {
    cors: {
      origin: ALLOWED_ORIGINS,
      methods: ["GET", "POST"],
      credentials: true,
    },
    pingInterval: 10_000,
    pingTimeout: 25_000,
  });

  io.use(async (socket, next) => {
    const { user_id, username, hub_id, access_code } =
      socket.handshake.auth || {};
    if (!user_id || !username) return next(new Error("auth_required"));

    const hid = String(hub_id || "public").substring(0, 64);

    // Hub access check
    let hub = hubCache.get(hid);
    if (!hub) {
      // Hub doesn't exist yet — create it on first join (like "public")
      hub = {
        id: hid,
        label: hid,
        is_private: 0,
        access_code: null,
        owner_id: null,
      };
      hubCache.set(hid, hub);
      ensureHubBuffers(hid);
    }

    if (hub.is_private && hub.access_code) {
      const isAdmin = await resolveAdmin(user_id);
      if (!isAdmin) {
        if (!access_code || access_code !== hub.access_code) {
          return next(new Error("access_denied"));
        }
      }
    }

    socket.userId = String(user_id);
    socket.username = String(username).substring(0, 64);
    socket.hubId = hid;
    socket.isAdmin = await resolveAdmin(user_id);
    next();
  });

  io.on("connection", async (socket) => {
    const { userId, username, hubId, isAdmin } = socket;
    console.log(`[+] ${username} (${userId}) admin=${isAdmin} → hub:${hubId}`);

    socket.join(hubId);
    ensureHubBuffers(hubId);

    // Apply defaults for fresh hub
    await applyHubDefaults(hubId);

    socket.emit("state", buildState(hubId));
    socket.emit("auth_info", { is_admin: isAdmin });

    getPresence(hubId).set(socket.id, {
      user_id: userId,
      username,
      x: WORLD_W / 2,
      y: WORLD_H / 2,
    });
    io.to(hubId).emit("presence_list", presenceList(hubId));

    // ── PIXEL ──────────────────────────────────────────────────────────────
    socket.on("pixel", (data) => {
      const x = parseInt(data.x),
        y = parseInt(data.y);
      if (isNaN(x) || isNaN(y) || x < 0 || y < 0) return;
      const erase = !!data.erase;
      const locked = !!data.locked && isAdmin;
      const color = erase
        ? null
        : String(data.color || "").match(/^#[0-9a-fA-F]{6}$/)
          ? data.color
          : "#ffffff";

      const buf = pixelBuffer.get(hubId);
      const erased = erasedKeys.get(hubId);
      const key = `${x},${y}`;
      const existing = buf.get(key);

      // Block non-admins from locked pixels
      if (!isAdmin && existing?.locked) {
        // Flash the individual pixel back to client
        socket.emit("zone_flash", { pixels: [{ x, y }] });
        return;
      }

      // Block non-admins from locked zones
      if (!isAdmin && isInLockedZone(hubId, x, y)) {
        const zone = getZoneAt(hubId, x, y);
        if (zone) socket.emit("zone_flash", { zone });
        return;
      }

      if (erase) {
        buf.delete(key);
        erased.add(key);
      } else {
        erased.delete(key);
        buf.set(key, { x, y, color, user_id: userId, username, locked });
      }

      io.to(hubId).emit("pixel", {
        x,
        y,
        color: erase ? null : color,
        erase,
        locked,
      });
      if (buf.size >= FLUSH_BATCH) flushPixels(hubId).catch(console.error);
    });

    // ── CHAT ──────────────────────────────────────────────────────────────
    socket.on("chat", async (data) => {
      const message = String(data.message || "")
        .trim()
        .substring(0, 500);
      if (!message) return;
      const msg = { username, message, ts: Math.floor(Date.now() / 1000) };
      const cc = chatCache.get(hubId);
      cc.push(msg);
      if (cc.length > 50) cc.shift();
      io.to(hubId).emit("chat", msg);
      await pool
        .execute(
          "INSERT INTO nh_chat (hub_id,user_id,username,message) VALUES (?,?,?,?)",
          [hubId, userId, username, message],
        )
        .catch((e) => console.error("[neurohub] chat:", e.message));
    });

    // ── MOVE ──────────────────────────────────────────────────────────────
    socket.on("move", (data) => {
      const x = Math.max(0, Math.min(WORLD_W, parseInt(data.x) || WORLD_W / 2));
      const y = Math.max(0, Math.min(WORLD_H, parseInt(data.y) || WORLD_H / 2));
      const p = getPresence(hubId).get(socket.id);
      if (p) {
        p.x = x;
        p.y = y;
      }
      socket.to(hubId).emit("move", { user_id: userId, username, x, y });
    });

    // ── BUBBLE SAVE ───────────────────────────────────────────────────────
    socket.on("bubble_save", async (data, ack) => {
      const type = data.type === "emotion" ? "emotion" : "brain";
      const x = Math.max(0, parseInt(data.x) || 100);
      const y = Math.max(0, parseInt(data.y) || 100);
      const color = String(data.color || "#6ee7f7").match(/^#[0-9a-fA-F]{6}$/)
        ? data.color
        : "#6ee7f7";
      const content =
        type === "brain" ? String(data.content || "").substring(0, 500) : null;
      const emotion =
        type === "emotion" ? String(data.emotion || "").substring(0, 64) : null;
      const emotionVal =
        type === "emotion"
          ? Math.min(10, Math.max(1, parseInt(data.emotion_val) || 5))
          : null;
      try {
        const [res] = await pool.execute(
          "INSERT INTO nh_bubbles (hub_id,type,x,y,color,content,emotion,emotion_val,user_id,username) VALUES (?,?,?,?,?,?,?,?,?,?)",
          [
            hubId,
            type,
            x,
            y,
            color,
            content,
            emotion,
            emotionVal,
            userId,
            username,
          ],
        );
        const bubbleId = res.insertId;
        if (data.parent_id) {
          const parentId = parseInt(data.parent_id);
          await pool.execute(
            "INSERT IGNORE INTO nh_branches (hub_id,parent_id,child_id) VALUES (?,?,?)",
            [hubId, parentId, bubbleId],
          );
          branchCache.get(hubId).add(`${parentId}-${bubbleId}`);
          io.to(hubId).emit("branch_add", {
            parent_id: parentId,
            child_id: bubbleId,
          });
        }
        const bubble = {
          id: bubbleId,
          type,
          x,
          y,
          color,
          content,
          emotion,
          emotion_val: emotionVal,
          username,
          user_id: userId,
        };
        bubbleCache.get(hubId).set(bubbleId, bubble);
        io.to(hubId).emit("bubble_update", bubble);
        if (typeof ack === "function") ack({ ok: true, id: bubbleId });
      } catch (e) {
        console.error("[neurohub] bubble_save:", e.message);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

    // ── BUBBLE MOVE ───────────────────────────────────────────────────────
    socket.on("bubble_move", async (data) => {
      const id = parseInt(data.id),
        x = parseInt(data.x),
        y = parseInt(data.y);
      if (isNaN(id) || isNaN(x) || isNaN(y)) return;
      const b = bubbleCache.get(hubId)?.get(id);
      if (b) {
        b.x = x;
        b.y = y;
      }
      socket.to(hubId).emit("bubble_moved", { id, x, y });
      await pool
        .execute("UPDATE nh_bubbles SET x=?,y=? WHERE id=?", [x, y, id])
        .catch((e) => console.error(e.message));
    });

    // ── BUBBLE EDIT ───────────────────────────────────────────────────────
    socket.on("bubble_edit", async (data) => {
      const id = parseInt(data.id);
      const b = bubbleCache.get(hubId)?.get(id);
      if (!b) return;
      if (b.type === "emotion") {
        if (data.emotion !== undefined)
          b.emotion = String(data.emotion).substring(0, 64);
        if (data.emotion_val !== undefined)
          b.emotion_val = Math.min(
            10,
            Math.max(1, parseInt(data.emotion_val) || 5),
          );
        await pool
          .execute("UPDATE nh_bubbles SET emotion=?,emotion_val=? WHERE id=?", [
            b.emotion,
            b.emotion_val,
            id,
          ])
          .catch((e) => console.error(e.message));
      } else {
        if (data.content !== undefined)
          b.content = String(data.content).substring(0, 500);
        await pool
          .execute("UPDATE nh_bubbles SET content=? WHERE id=?", [
            b.content,
            id,
          ])
          .catch((e) => console.error(e.message));
      }
      io.to(hubId).emit("bubble_update", b);
    });

    // ── BUBBLE DELETE ─────────────────────────────────────────────────────
    socket.on("bubble_delete", async (data) => {
      const id = parseInt(data.id);
      if (isNaN(id)) return;
      bubbleCache.get(hubId)?.delete(id);
      const bc = branchCache.get(hubId);
      if (bc)
        for (const k of [...bc]) {
          const [p, c] = k.split("-").map(Number);
          if (p === id || c === id) bc.delete(k);
        }
      io.to(hubId).emit("bubble_delete", { id });
      await pool
        .execute("DELETE FROM nh_bubbles WHERE id=?", [id])
        .catch((e) => console.error(e.message));
      await pool
        .execute("DELETE FROM nh_branches WHERE parent_id=? OR child_id=?", [
          id,
          id,
        ])
        .catch((e) => console.error(e.message));
    });

    // ── ADMIN ZONE ADD ────────────────────────────────────────────────────
    socket.on("zone_add", async (data, ack) => {
      if (!isAdmin) {
        if (typeof ack === "function") ack({ ok: false, reason: "not_admin" });
        return;
      }
      const x = parseInt(data.x),
        y = parseInt(data.y),
        w = parseInt(data.w),
        h = parseInt(data.h);
      const label = String(data.label || "").substring(0, 64);
      if (isNaN(x) || isNaN(y) || w < 1 || h < 1) {
        if (typeof ack === "function") ack({ ok: false });
        return;
      }
      try {
        const [res] = await pool.execute(
          "INSERT INTO nh_admin_zones (hub_id,x,y,w,h,label) VALUES (?,?,?,?,?,?)",
          [hubId, x, y, w, h, label],
        );
        const zone = { id: res.insertId, x, y, w, h, label };
        zoneCache.get(hubId).set(zone.id, zone);
        io.to(hubId).emit("zone_add", zone);
        if (typeof ack === "function") ack({ ok: true, id: zone.id });
      } catch (e) {
        console.error("[neurohub] zone_add:", e.message);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

    // ── ADMIN ZONE DELETE ─────────────────────────────────────────────────
    socket.on("zone_delete", async (data, ack) => {
      if (!isAdmin) {
        if (typeof ack === "function") ack({ ok: false, reason: "not_admin" });
        return;
      }
      const id = parseInt(data.id);
      if (isNaN(id)) return;
      zoneCache.get(hubId)?.delete(id);
      io.to(hubId).emit("zone_delete", { id });
      await pool
        .execute("DELETE FROM nh_admin_zones WHERE id=?", [id])
        .catch((e) => console.error("[neurohub] zone_delete:", e.message));
      if (typeof ack === "function") ack({ ok: true });
    });

    // ── DISCONNECT ────────────────────────────────────────────────────────
    socket.on("disconnect", () => {
      console.log(`[-] ${username} ← hub:${hubId}`);
      getPresence(hubId).delete(socket.id);
      io.to(hubId).emit("presence_list", presenceList(hubId));
    });
  });

  httpServer.listen(PORT, () => {
    console.log(`[neurohub] v5.0 listening on :${PORT}`);
    console.log(`[neurohub] CORS: ${ALLOWED_ORIGINS.join(", ")}`);
  });
}

// ── Util ──────────────────────────────────────────────────────────────────────
function readBody(req) {
  return new Promise((res, rej) => {
    let body = "";
    req.on("data", (d) => (body += d));
    req.on("end", () => res(body));
    req.on("error", rej);
  });
}

async function shutdown() {
  console.log("[neurohub] Shutting down…");
  await flushAllPixels();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

bootstrap().catch((err) => {
  console.error("[neurohub] Fatal:", err);
  process.exit(1);
});
