/**
 * neurohub_server.js — v4.4
 *
 * KEY FIX: users table lives in a DIFFERENT database (jakobkall_com_db) than
 * the neurohub tables (jakobkall_com_db_neurohub). We use a second DB pool
 * for user lookups, with cross-db fallback using fully-qualified table name.
 *
 * Also fixed:
 *  - adminCache no longer caches failures permanently (only caches hits)
 *  - user_type parsing handles "admin, therapist, user" and all variants
 *  - session user_id matched against BOTH users.id and users.user_id columns
 *  - nh_admin_zones table creation skipped if already exists (DB has it)
 *  - pixel zone check uses cell coords correctly
 */
"use strict";

const http = require("http");
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

// ── Two DB configs: neurohub data + user auth ─────────────────────────────────
// neurohub tables (nh_pixels, nh_bubbles, nh_admin_zones, etc.)
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

// users table lives here
const USERS_DB = {
  host: process.env.DB_HOST || "mysql48.unoeuro.com",
  port: parseInt(process.env.DB_PORT || "3306"),
  user: process.env.DB_USER || "jakobkall_com",
  password: process.env.DB_PASS || "cfDEmaw5n96t",
  database: process.env.USERS_DB_NAME || "jakobkall_com_db", // ← separate DB
  waitForConnections: true,
  connectionLimit: 5,
  charset: "utf8mb4",
};

// ── RAM caches ────────────────────────────────────────────────────────────────
const pixelBuffer = new Map();
const erasedKeys = new Map();
const bubbleCache = new Map();
const branchCache = new Map();
const chatCache = new Map();
const presence = new Map();
const zoneCache = new Map();
// adminCache: key → true/false — only cache confirmed results, never failures
const adminCache = new Map();

const FLUSH_INTERVAL = 20_000;
const FLUSH_BATCH = 200;
const WORLD_W = 4000,
  WORLD_H = 2800;

let pool; // neurohub DB
let usersPool; // users DB

// ── Bootstrap ─────────────────────────────────────────────────────────────────
async function bootstrap() {
  pool = await mysql.createPool(NEUROHUB_DB);
  usersPool = await mysql.createPool(USERS_DB);
  console.log("[neurohub] Both DB pools connected");
  console.log(`[neurohub] neurohub DB: ${NEUROHUB_DB.database}`);
  console.log(`[neurohub] users DB:    ${USERS_DB.database}`);

  // Test users DB connectivity immediately
  try {
    const [rows] = await usersPool.execute("SELECT COUNT(*) AS cnt FROM users");
    console.log(`[neurohub] users table: ${rows[0].cnt} users found`);
  } catch (e) {
    console.error("[neurohub] WARNING: Cannot read users table:", e.message);
    console.error("[neurohub] Admin login will not work until this is fixed");
  }

  // Ensure locked column
  try {
    await pool.execute(
      "ALTER TABLE nh_pixels ADD COLUMN IF NOT EXISTS locked TINYINT(1) NOT NULL DEFAULT 0",
    );
  } catch (e) {}

  // Ensure unique index on pixels
  try {
    await pool.execute(
      "ALTER TABLE nh_pixels ADD UNIQUE INDEX IF NOT EXISTS ux_hub_xy (hub_id,x,y)",
    );
  } catch (e) {
    try {
      await pool.execute(
        "ALTER TABLE nh_pixels ADD UNIQUE INDEX ux_hub_xy (hub_id,x,y)",
      );
    } catch (e2) {}
  }

  // nh_admin_zones already exists per DB schema — just ensure it in case
  try {
    await pool.execute(`
      CREATE TABLE IF NOT EXISTS nh_admin_zones (
        id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        hub_id     VARCHAR(64) NOT NULL DEFAULT 'public',
        x          INT NOT NULL,
        y          INT NOT NULL,
        w          INT NOT NULL,
        h          INT NOT NULL,
        label      VARCHAR(64) DEFAULT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_hub (hub_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
    console.log("[neurohub] nh_admin_zones table ready");
  } catch (e) {
    console.log("[neurohub] nh_admin_zones already exists");
  }

  await warmUp();
  setInterval(flushAllPixels, FLUSH_INTERVAL);
  startServer();
}

// ── Warm-up ───────────────────────────────────────────────────────────────────
async function warmUp() {
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

  const [bubbles] = await pool.query(
    "SELECT id,hub_id,type,x,y,color,content,emotion,emotion_val,user_id,username FROM nh_bubbles",
  );
  bubbles.forEach((b) => {
    ensureHubBuffers(b.hub_id);
    bubbleCache.get(b.hub_id).set(b.id, { ...b });
  });

  const [branches] = await pool.query(
    "SELECT hub_id,parent_id,child_id FROM nh_branches",
  );
  branches.forEach((br) => {
    ensureHubBuffers(br.hub_id);
    branchCache.get(br.hub_id).add(`${br.parent_id}-${br.child_id}`);
  });

  const [chats] = await pool.query(
    "SELECT hub_id,username,message,UNIX_TIMESTAMP(created_at) AS ts FROM nh_chat ORDER BY id DESC LIMIT 500",
  );
  chats.reverse().forEach((c) => {
    ensureHubBuffers(c.hub_id);
    const cc = chatCache.get(c.hub_id);
    cc.push({ username: c.username, message: c.message, ts: c.ts });
    if (cc.length > 50) cc.shift();
  });

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

// ── Admin resolution ──────────────────────────────────────────────────────────
// user_type examples: "admin", "user", "admin, user", "admin, therapist, user"
function parseIsAdmin(userTypeStr) {
  if (!userTypeStr) return false;
  return userTypeStr
    .split(",")
    .map((t) => t.trim().toLowerCase())
    .includes("admin");
}

async function resolveAdmin(userId) {
  const cacheKey = String(userId);

  // Return cached result if we have one
  if (adminCache.has(cacheKey)) {
    const cached = adminCache.get(cacheKey);
    console.log(`[neurohub] admin cache hit for ${userId}: ${cached}`);
    return cached;
  }

  // Try usersPool first (correct DB)
  try {
    // Match on both users.id (int PK) and users.user_id (bigint, set by Discord auth)
    const [rows] = await usersPool.execute(
      "SELECT user_type FROM users WHERE id=? OR user_id=? LIMIT 1",
      [userId, userId],
    );
    if (rows.length > 0) {
      const isAdmin = parseIsAdmin(rows[0].user_type);
      adminCache.set(cacheKey, isAdmin);
      console.log(
        `[neurohub] Admin resolved for userId=${userId} user_type="${rows[0].user_type}" → isAdmin=${isAdmin}`,
      );
      return isAdmin;
    } else {
      console.log(
        `[neurohub] No user found for userId=${userId} in users table`,
      );
      // Cache as false but only briefly (5min) so it retries
      adminCache.set(cacheKey, false);
      setTimeout(() => adminCache.delete(cacheKey), 5 * 60 * 1000);
      return false;
    }
  } catch (e) {
    console.error(
      `[neurohub] usersPool admin lookup failed for ${userId}:`,
      e.message,
    );
  }

  // Fallback: try cross-database query via neurohub pool
  // This works if the DB user has SELECT on both databases
  try {
    const usersDbName = USERS_DB.database;
    const [rows] = await pool.execute(
      `SELECT user_type FROM \`${usersDbName}\`.users WHERE id=? OR user_id=? LIMIT 1`,
      [userId, userId],
    );
    if (rows.length > 0) {
      const isAdmin = parseIsAdmin(rows[0].user_type);
      adminCache.set(cacheKey, isAdmin);
      console.log(
        `[neurohub] Admin resolved (cross-db) for userId=${userId}: isAdmin=${isAdmin}`,
      );
      return isAdmin;
    }
  } catch (e) {
    console.error(`[neurohub] Cross-db admin lookup also failed:`, e.message);
  }

  // Both attempts failed — don't cache, let next connection retry
  console.error(
    `[neurohub] Could not resolve admin for userId=${userId} — defaulting to false`,
  );
  return false;
}

// ── Zone check ────────────────────────────────────────────────────────────────
function isInLockedZone(hubId, cx, cy) {
  const zones = zoneCache.get(hubId);
  if (!zones) return false;
  for (const z of zones.values()) {
    if (cx >= z.x && cx < z.x + z.w && cy >= z.y && cy < z.y + z.h) return true;
  }
  return false;
}

// ── Pixel flush ───────────────────────────────────────────────────────────────
async function flushPixels(hubId) {
  const buf = pixelBuffer.get(hubId);
  const erased = erasedKeys.get(hubId);

  if (erased && erased.size > 0) {
    for (const key of erased) {
      const [x, y] = key.split(",").map(Number);
      try {
        await pool.execute(
          "DELETE FROM nh_pixels WHERE hub_id=? AND x=? AND y=?",
          [hubId, x, y],
        );
      } catch (err) {
        console.error("[neurohub] pixel delete:", err.message);
      }
    }
    erased.clear();
  }

  if (buf && buf.size > 0) {
    const rows = [...buf.values()];
    for (let i = 0; i < rows.length; i += 200) {
      const chunk = rows.slice(i, i + 200);
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
      try {
        await pool.execute(
          `INSERT INTO nh_pixels (hub_id,x,y,color,user_id,username,locked) VALUES ${ph}
           ON DUPLICATE KEY UPDATE color=VALUES(color),user_id=VALUES(user_id),username=VALUES(username),locked=VALUES(locked)`,
          vals,
        );
      } catch (err) {
        console.error("[neurohub] pixel upsert:", err.message);
        for (const p of chunk) {
          try {
            await pool.execute(
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
            );
          } catch (e2) {
            console.error("[neurohub] pixel individual:", e2.message);
          }
        }
      }
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

// ── Socket server ─────────────────────────────────────────────────────────────
function startServer() {
  const httpServer = http.createServer((req, res) => {
    if (req.url === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          ok: true,
          pixels: [...pixelBuffer.values()].reduce((s, m) => s + m.size, 0),
          adminCacheSize: adminCache.size,
        }),
      );
      return;
    }
    // Debug endpoint to check a specific user's admin status
    if (req.url.startsWith("/debug/admin/")) {
      const uid = req.url.replace("/debug/admin/", "");
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          userId: uid,
          cached: adminCache.has(uid) ? adminCache.get(uid) : "not-cached",
        }),
      );
      return;
    }
    res.writeHead(404);
    res.end();
  });

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
    const { user_id, username, hub_id } = socket.handshake.auth || {};
    if (!user_id || !username) return next(new Error("auth_required"));
    socket.userId = String(user_id);
    socket.username = String(username).substring(0, 64);
    socket.hubId = String(hub_id || "public").substring(0, 64);
    socket.isAdmin = await resolveAdmin(user_id);
    console.log(
      `[neurohub] Socket auth: userId=${socket.userId} username=${socket.username} isAdmin=${socket.isAdmin}`,
    );
    next();
  });

  io.on("connection", (socket) => {
    const { userId, username, hubId, isAdmin } = socket;
    console.log(`[+] ${username} (${userId}) admin=${isAdmin} → hub:${hubId}`);

    socket.join(hubId);
    ensureHubBuffers(hubId);

    // Send state
    socket.emit("state", buildState(hubId));
    // Send auth info — client uses this to show/hide admin controls
    socket.emit("auth_info", { is_admin: isAdmin });

    getPresence(hubId).set(socket.id, {
      user_id: userId,
      username,
      x: WORLD_W / 2,
      y: WORLD_H / 2,
    });
    io.to(hubId).emit("presence_list", presenceList(hubId));

    // ── PIXEL ────────────────────────────────────────────────────────────────
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

      // Block non-admins in locked zones (x,y are cell coords from client)
      if (!isAdmin && isInLockedZone(hubId, x, y)) return;

      const buf = pixelBuffer.get(hubId);
      const erased = erasedKeys.get(hubId);
      const key = `${x},${y}`;
      const existing = buf.get(key);

      if (erase && existing?.locked && !isAdmin) return;

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

    // ── CHAT ─────────────────────────────────────────────────────────────────
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
      try {
        await pool.execute(
          "INSERT INTO nh_chat (hub_id,user_id,username,message) VALUES (?,?,?,?)",
          [hubId, userId, username, message],
        );
      } catch (e) {
        console.error("[neurohub] chat:", e.message);
      }
    });

    // ── MOVE ─────────────────────────────────────────────────────────────────
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

    // ── BUBBLE SAVE ───────────────────────────────────────────────────────────
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

    // ── BUBBLE MOVE ───────────────────────────────────────────────────────────
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
      try {
        await pool.execute("UPDATE nh_bubbles SET x=?,y=? WHERE id=?", [
          x,
          y,
          id,
        ]);
      } catch (e) {
        console.error(e.message);
      }
    });

    // ── BUBBLE EDIT ───────────────────────────────────────────────────────────
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
        try {
          await pool.execute(
            "UPDATE nh_bubbles SET emotion=?,emotion_val=? WHERE id=?",
            [b.emotion, b.emotion_val, id],
          );
        } catch (e) {
          console.error(e.message);
        }
      } else {
        if (data.content !== undefined)
          b.content = String(data.content).substring(0, 500);
        try {
          await pool.execute("UPDATE nh_bubbles SET content=? WHERE id=?", [
            b.content,
            id,
          ]);
        } catch (e) {
          console.error(e.message);
        }
      }
      io.to(hubId).emit("bubble_update", b);
    });

    // ── BUBBLE DELETE ─────────────────────────────────────────────────────────
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
      try {
        await pool.execute("DELETE FROM nh_bubbles WHERE id=?", [id]);
        await pool.execute(
          "DELETE FROM nh_branches WHERE parent_id=? OR child_id=?",
          [id, id],
        );
      } catch (e) {
        console.error(e.message);
      }
    });

    // ── ADMIN ZONE ADD ────────────────────────────────────────────────────────
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
        console.log(`[neurohub] Zone added id=${zone.id} by ${username}`);
      } catch (e) {
        console.error("[neurohub] zone_add:", e.message);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

    // ── ADMIN ZONE DELETE ─────────────────────────────────────────────────────
    socket.on("zone_delete", async (data, ack) => {
      if (!isAdmin) {
        if (typeof ack === "function") ack({ ok: false, reason: "not_admin" });
        return;
      }
      const id = parseInt(data.id);
      if (isNaN(id)) return;
      zoneCache.get(hubId)?.delete(id);
      io.to(hubId).emit("zone_delete", { id });
      try {
        await pool.execute("DELETE FROM nh_admin_zones WHERE id=?", [id]);
        if (typeof ack === "function") ack({ ok: true });
      } catch (e) {
        console.error("[neurohub] zone_delete:", e.message);
      }
    });

    // ── DISCONNECT ────────────────────────────────────────────────────────────
    socket.on("disconnect", () => {
      console.log(`[-] ${username} ← hub:${hubId}`);
      getPresence(hubId).delete(socket.id);
      io.to(hubId).emit("presence_list", presenceList(hubId));
    });
  });

  httpServer.listen(PORT, () => {
    console.log(`[neurohub] v4.4 listening on :${PORT}`);
    console.log(`[neurohub] CORS: ${ALLOWED_ORIGINS.join(", ")}`);
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
