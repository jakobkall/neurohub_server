/**
 * neurohub_server.js — v6.7
 *
 * CHANGES vs v6.6:
 * - NY: nh_bubbles.size_scale (FLOAT, default 1) — bobler kan nu have en
 *   individuel størrelse (0.5–3.0). Tilføjes via migration, gemmes i
 *   bubble_save, kan ændres via bubble_edit og sendes med i state/bubble_update.
 * - NY: bubble_edit understøtter nu `color` (#rrggbb) og `size_scale` for
 *   BÅDE thought- og emotion-bobler. UPDATE bygges dynamisk, så kun de felter
 *   der faktisk blev sendt, bliver skrevet.
 * - NY: "branch_delete" socket-handler — sletter en enkelt forbindelse mellem
 *   to bobler (begge retninger), opdaterer RAM-cache + DB og broadcaster
 *   "branch_delete" til hub-rummet.
 * - DB-adgangskoden står IKKE længere hardcoded i filen. Sæt DB_PASS (og
 *   evt. DB_HOST/DB_USER/DB_NAME/USERS_DB_NAME) i din .env.
 * - warmUp() henter bobler med SELECT * så serveren ikke crasher hvis
 *   migrationen af en ny kolonne endnu ikke er kørt.
 * - bubble_move / bubble_delete / bubble_edit begrænser nu også på hub_id i SQL.
 *
 * Hvis din MySQL-version ikke understøtter "ADD COLUMN IF NOT EXISTS", så kør
 * manuelt:
 *   ALTER TABLE nh_bubbles ADD COLUMN size_scale FLOAT NOT NULL DEFAULT 1;
 *
 * (Ældre changelog: se v6.6 — duplikeret const [bubbles] fix, emotion_icon_id,
 *  branch_add, default-kopiering fra nh_hub_defaults.)
 */
"use strict";

const http = require("http");
const { Server } = require("socket.io");
const mysql = require("mysql2/promise");
require("dotenv").config();

const PORT = process.env.PORT || 3001;
const ALLOWED_ORIGINS = (
  process.env.ALLOWED_ORIGINS ||
  "https://jakobkall.com,https://hub.jakobkall.com,http://localhost"
)
  .split(",")
  .map((s) => s.trim());

// ── DB configs ────────────────────────────────────────────────────────────────
const NEUROHUB_DB = {
  host: process.env.DB_HOST || "mysql48.unoeuro.com",
  port: parseInt(process.env.DB_PORT || "3306"),
  user: process.env.DB_USER || "jakobkall_com",
  password: process.env.DB_PASS,
  database: process.env.DB_NAME || "jakobkall_com_db_neurohub",
  waitForConnections: true,
  connectionLimit: 10,
  charset: "utf8mb4",
};

const USERS_DB = {
  host: process.env.DB_HOST || "mysql48.unoeuro.com",
  port: parseInt(process.env.DB_PORT || "3306"),
  user: process.env.DB_USER || "jakobkall_com",
  password: process.env.DB_PASS,
  database: process.env.USERS_DB_NAME || "jakobkall_com_db",
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
const adminCache = new Map();

const loadedHubs = new Set();
const initPromises = new Map();

const FLUSH_INTERVAL = 20_000;
const FLUSH_BATCH = 200;
const WORLD_W = 4000,
  WORLD_H = 2800;

const MIN_SCALE = 0.5;
const MAX_SCALE = 3.0;
const HEX_RE = /^#[0-9a-fA-F]{6}$/;

function clampScale(v, fallback = 1) {
  const n = parseFloat(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.round(Math.min(MAX_SCALE, Math.max(MIN_SCALE, n)) * 100) / 100;
}

let pool;
let usersPool;
let io;

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

  // Schema extensions — runs ONCE, fully, before warmUp() reads nh_bubbles.
  for (const sql of [
    "ALTER TABLE nh_pixels ADD COLUMN IF NOT EXISTS locked TINYINT(1) NOT NULL DEFAULT 0",
    "ALTER TABLE nh_pixels ADD COLUMN IF NOT EXISTS locked_by_zone TINYINT(1) NOT NULL DEFAULT 0",
    "ALTER TABLE nh_bubbles ADD COLUMN IF NOT EXISTS emotion_icon_id INT NULL DEFAULT NULL",
    "ALTER TABLE nh_bubbles ADD COLUMN IF NOT EXISTS size_scale FLOAT NOT NULL DEFAULT 1",
  ]) {
    try {
      await pool.execute(sql);
    } catch (_) {}
  }
  try {
    await pool.execute(
      "ALTER TABLE nh_pixels ADD UNIQUE INDEX IF NOT EXISTS ux_hub_xy (hub_id,x,y)",
    );
  } catch (_) {
    try {
      await pool.execute(
        "ALTER TABLE nh_pixels ADD UNIQUE INDEX ux_hub_xy (hub_id,x,y)",
      );
    } catch (_2) {}
  }

  await warmUp();
  setInterval(flushAllPixels, FLUSH_INTERVAL);
  startServer();
}

// ── Warm-up ───────────────────────────────────────────────────────────────────
async function warmUp() {
  const [pixels] = await pool.query(
    "SELECT hub_id,x,y,color,user_id,username,IFNULL(locked,0) AS locked,IFNULL(locked_by_zone,0) AS locked_by_zone FROM nh_pixels LIMIT 500000",
  );
  pixels.forEach((p) => {
    loadedHubs.add(p.hub_id);
    ensureHubBuffers(p.hub_id);
    pixelBuffer.get(p.hub_id).set(`${p.x},${p.y}`, {
      x: p.x,
      y: p.y,
      color: p.color,
      user_id: String(p.user_id),
      username: p.username,
      locked: p.locked ? 1 : 0,
      locked_by_zone: p.locked_by_zone ? 1 : 0,
    });
  });

  // SELECT * så size_scale / emotion_icon_id automatisk følger med
  const [bubbles] = await pool.query("SELECT * FROM nh_bubbles");
  bubbles.forEach((b) => {
    loadedHubs.add(b.hub_id);
    ensureHubBuffers(b.hub_id);
    bubbleCache.get(b.hub_id).set(b.id, normalizeBubbleRow(b));
  });

  const [branches] = await pool.query(
    "SELECT hub_id,parent_id,child_id FROM nh_branches",
  );
  branches.forEach((br) => {
    loadedHubs.add(br.hub_id);
    ensureHubBuffers(br.hub_id);
    branchCache.get(br.hub_id).add(`${br.parent_id}-${br.child_id}`);
  });

  const [chats] = await pool.query(
    "SELECT hub_id,username,message,UNIX_TIMESTAMP(created_at) AS ts FROM nh_chat ORDER BY id DESC LIMIT 500",
  );
  chats.reverse().forEach((c) => {
    loadedHubs.add(c.hub_id);
    ensureHubBuffers(c.hub_id);
    const cc = chatCache.get(c.hub_id);
    cc.push({ username: c.username, message: c.message, ts: c.ts });
    if (cc.length > 50) cc.shift();
  });

  const [zones] = await pool.query(
    "SELECT id,hub_id,x,y,w,h,label FROM nh_admin_zones",
  );
  zones.forEach((z) => {
    loadedHubs.add(z.hub_id);
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

  loadedHubs.add("public");
  ensureHubBuffers("public");
  console.log("[neurohub] Warm-up complete");
}

// Sørger for at size_scale altid er et tal (default 1), uanset DB-version
function normalizeBubbleRow(b) {
  return { ...b, size_scale: clampScale(b.size_scale, 1) };
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

// ── RAM reset for a hub ───────────────────────────────────────────────────────
async function resetHubRAM(hubId) {
  console.log(`[neurohub] resetHubRAM: clearing RAM for hub "${hubId}"`);
  erasedKeys.get(hubId)?.clear();
  pixelBuffer.set(hubId, new Map());
  erasedKeys.set(hubId, new Set());
  bubbleCache.set(hubId, new Map());
  branchCache.set(hubId, new Set());
  chatCache.set(hubId, []);
  zoneCache.set(hubId, new Map());
  loadedHubs.delete(hubId);
  await initHubIfNeeded(hubId);
  if (io) {
    const state = buildState(hubId);
    io.to(hubId).emit("state", state);
    console.log(
      `[neurohub] resetHubRAM: broadcast fresh state to hub "${hubId}" (${state.pixels.length} pixels, ${state.zones.length} zones)`,
    );
  }
  return buildState(hubId);
}

// ── Lazy Hub Loader ───────────────────────────────────────────────────────────
async function initHubIfNeeded(hubId) {
  if (loadedHubs.has(hubId)) return true;
  if (!initPromises.has(hubId)) {
    initPromises.set(
      hubId,
      _doInitHub(hubId).finally(() => initPromises.delete(hubId)),
    );
  }
  return await initPromises.get(hubId);
}

async function _doInitHub(hubId) {
  if (loadedHubs.has(hubId)) return true;
  ensureHubBuffers(hubId);

  try {
    const [hubRows] = await pool.execute(
      "SELECT id FROM nh_hubs WHERE id = ?",
      [hubId],
    );

    if (hubRows.length === 0) {
      console.log(`[neurohub] Hub "${hubId}" not found in DB — rejecting.`);
      return false;
    }

    // ── KOPIERING AF DEFAULT ELEMENTER HVIS HUBBEN ER HELT TOM ──
    const [pCheck] = await pool.query(
      "SELECT 1 FROM nh_pixels WHERE hub_id = ? LIMIT 1",
      [hubId],
    );
    const [zCheck] = await pool.query(
      "SELECT 1 FROM nh_admin_zones WHERE hub_id = ? LIMIT 1",
      [hubId],
    );

    if (pCheck.length === 0 && zCheck.length === 0) {
      console.log(
        `[neurohub] Hub "${hubId}" is empty. Loading defaults from nh_hub_defaults...`,
      );
      try {
        await pool.execute(
          `
          INSERT IGNORE INTO nh_pixels (hub_id, x, y, color, locked, locked_by_zone, user_id, username)
          SELECT ?, x, y, color, locked, 0, '999999', 'System'
          FROM nh_hub_defaults WHERE type = 'pixel'
        `,
          [hubId],
        );

        await pool.execute(
          `
          INSERT IGNORE INTO nh_admin_zones (hub_id, x, y, w, h, label)
          SELECT ?, x, y, w, h, label
          FROM nh_hub_defaults WHERE type = 'zone'
        `,
          [hubId],
        );
        console.log(`[neurohub] Defaults loaded successfully for "${hubId}".`);
      } catch (err) {
        console.error(
          `[neurohub] Failed to load defaults for "${hubId}":`,
          err.message,
        );
      }
    }

    // Load pixels
    const [pixels] = await pool.query(
      "SELECT * FROM nh_pixels WHERE hub_id = ?",
      [hubId],
    );
    pixels.forEach((p) => {
      pixelBuffer.get(hubId).set(`${p.x},${p.y}`, {
        x: p.x,
        y: p.y,
        color: p.color,
        user_id: String(p.user_id),
        username: p.username,
        locked: p.locked ? 1 : 0,
        locked_by_zone: p.locked_by_zone ? 1 : 0,
      });
    });

    // Load zones
    const [zones] = await pool.query(
      "SELECT * FROM nh_admin_zones WHERE hub_id = ?",
      [hubId],
    );
    zones.forEach((z) => {
      zoneCache.get(hubId).set(z.id, {
        id: z.id,
        x: z.x,
        y: z.y,
        w: z.w,
        h: z.h,
        label: z.label || "",
      });
    });

    // Load bubbles (SELECT * picks up emotion_icon_id + size_scale)
    const [bubbles] = await pool.query(
      "SELECT * FROM nh_bubbles WHERE hub_id = ?",
      [hubId],
    );
    bubbles.forEach((b) =>
      bubbleCache.get(hubId).set(b.id, normalizeBubbleRow(b)),
    );

    // Load branches
    const [branches] = await pool.query(
      "SELECT * FROM nh_branches WHERE hub_id = ?",
      [hubId],
    );
    branches.forEach((br) =>
      branchCache.get(hubId).add(`${br.parent_id}-${br.child_id}`),
    );

    // Load chat
    const [chats] = await pool.query(
      "SELECT * FROM nh_chat WHERE hub_id = ? ORDER BY id DESC LIMIT 50",
      [hubId],
    );
    const cc = chatCache.get(hubId);
    chats.reverse().forEach((c) =>
      cc.push({
        username: c.username,
        message: c.message,
        ts: Math.floor(new Date(c.created_at).getTime() / 1000),
      }),
    );

    loadedHubs.add(hubId);
    console.log(
      `[neurohub] Loaded hub "${hubId}": ${pixels.length}px, ${zones.length} zones, ${bubbles.length} bubbles`,
    );
    return true;
  } catch (err) {
    console.error(`[neurohub] Error initializing hub ${hubId}:`, err.message);
    return false;
  }
}

// ── Admin helpers ─────────────────────────────────────────────────────────────
function parseIsAdmin(userTypeStr) {
  if (!userTypeStr) return false;
  return userTypeStr
    .split(",")
    .map((t) => t.trim().toLowerCase())
    .includes("admin");
}

async function resolveAdmin(userId) {
  const cacheKey = String(userId);
  if (adminCache.has(cacheKey)) return adminCache.get(cacheKey);
  try {
    const [rows] = await usersPool.execute(
      "SELECT user_type FROM users WHERE id=? OR user_id=? LIMIT 1",
      [userId, userId],
    );
    if (rows.length > 0) {
      const isAdmin = parseIsAdmin(rows[0].user_type);
      adminCache.set(cacheKey, isAdmin);
      return isAdmin;
    }
  } catch (e) {}
  adminCache.set(cacheKey, false);
  setTimeout(() => adminCache.delete(cacheKey), 5 * 60 * 1000);
  return false;
}

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

// ── DB flushing ───────────────────────────────────────────────────────────────
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
      } catch (_) {}
    }
    erased.clear();
  }

  if (buf && buf.size > 0) {
    const rows = [...buf.values()];
    for (let i = 0; i < rows.length; i += FLUSH_BATCH) {
      const chunk = rows.slice(i, i + FLUSH_BATCH);
      const ph = chunk.map(() => "(?,?,?,?,?,?,?,?)").join(",");
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
          p.locked_by_zone ? 1 : 0,
        ),
      );
      try {
        await pool.execute(
          `INSERT INTO nh_pixels (hub_id,x,y,color,user_id,username,locked,locked_by_zone) VALUES ${ph}
           ON DUPLICATE KEY UPDATE color=VALUES(color),user_id=VALUES(user_id),username=VALUES(username),
           locked=VALUES(locked),locked_by_zone=VALUES(locked_by_zone)`,
          vals,
        );
      } catch (err) {
        console.error("[neurohub] pixel upsert error:", err.message);
      }
    }
  }
}

async function flushAllPixels() {
  for (const hubId of pixelBuffer.keys()) {
    await flushPixels(hubId).catch((e) =>
      console.error(`[neurohub] flush error:`, e.message),
    );
  }
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

// ── HTTP + Socket server ───────────────────────────────────────────────────────
function startServer() {
  const httpServer = http.createServer(async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    if (req.method === "OPTIONS") {
      res.writeHead(200);
      return res.end();
    }

    if (req.url === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(
        JSON.stringify({
          ok: true,
          loadedHubs: [...loadedHubs],
          pixelCounts: Object.fromEntries(
            [...pixelBuffer.entries()].map(([k, v]) => [k, v.size]),
          ),
          adminCacheSize: adminCache.size,
        }),
      );
    }

    if (req.url === "/hubs") {
      try {
        const [hubs] = await pool.query(
          "SELECT id, label, is_private FROM nh_hubs ORDER BY created_at DESC LIMIT 50",
        );
        res.writeHead(200, { "Content-Type": "application/json" });
        return res.end(JSON.stringify(hubs));
      } catch (e) {
        res.writeHead(500);
        return res.end("[]");
      }
    }

    const resetMatch = req.url.match(/^\/reset-hub\/([^/?]+)/);
    if (resetMatch) {
      const hubId = decodeURIComponent(resetMatch[1]);
      console.log(`[neurohub] HTTP /reset-hub/${hubId} called`);
      try {
        const state = await resetHubRAM(hubId);
        res.writeHead(200, { "Content-Type": "application/json" });
        return res.end(
          JSON.stringify({
            ok: true,
            hubId,
            pixels: state.pixels.length,
            bubbles: state.bubbles.length,
            zones: state.zones.length,
            message: `RAM reset for hub "${hubId}". ${state.pixels.length} pixels loaded from DB.`,
          }),
        );
      } catch (e) {
        console.error(`[neurohub] reset-hub error:`, e.message);
        res.writeHead(500, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: e.message }));
      }
    }

    res.writeHead(404);
    res.end();
  });

  io = new Server(httpServer, {
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

    const hubExists = await initHubIfNeeded(socket.hubId);
    if (!hubExists) {
      return next(new Error("hub_not_found"));
    }

    next();
  });

  io.on("connection", (socket) => {
    const { userId, username, hubId, isAdmin } = socket;
    socket.join(hubId);

    socket.emit("state", buildState(hubId));
    socket.emit("auth_info", { is_admin: isAdmin });

    getPresence(hubId).set(socket.id, {
      user_id: userId,
      username,
      x: WORLD_W / 2,
      y: WORLD_H / 2,
    });
    io.to(hubId).emit("presence_list", presenceList(hubId));

    socket.on("pixel", (data) => {
      const x = parseInt(data.x),
        y = parseInt(data.y);
      if (isNaN(x) || isNaN(y) || x < 0 || y < 0) return;
      const erase = !!data.erase;
      const key = `${x},${y}`;
      const buf = pixelBuffer.get(hubId);
      const erased = erasedKeys.get(hubId);
      const existing = buf.get(key);
      const inLockedZone = isInLockedZone(hubId, x, y);
      const isLockedPixel = existing && existing.locked;

      if (!isAdmin && (inLockedZone || isLockedPixel)) {
        const zone = getZoneAt(hubId, x, y);
        socket.emit("zone_flash", {
          zone: zone || null,
          pixels: zone ? null : [{ x, y }],
        });
        return;
      }

      const color = erase
        ? null
        : String(data.color || "").match(HEX_RE)
          ? data.color
          : "#ffffff";
      const locked = isAdmin && !erase && (!!data.locked || inLockedZone);
      const locked_by_zone = !erase && inLockedZone && !data.locked ? 1 : 0;

      if (erase) {
        buf.delete(key);
        erased.add(key);
      } else {
        erased.delete(key);
        buf.set(key, {
          x,
          y,
          color,
          user_id: userId,
          username,
          locked: locked ? 1 : 0,
          locked_by_zone,
        });
      }

      io.to(hubId).emit("pixel", {
        x,
        y,
        color: erase ? null : color,
        erase,
        locked: locked ? 1 : 0,
      });
      if (buf.size >= FLUSH_BATCH) flushPixels(hubId).catch(console.error);
    });

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
      } catch (_) {}
    });

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

    socket.on("bubble_save", async (data, ack) => {
      const type = data.type === "emotion" ? "emotion" : "brain";
      const x = Math.max(0, parseInt(data.x) || 100);
      const y = Math.max(0, parseInt(data.y) || 100);
      const color = String(data.color || "#6ee7f7").match(HEX_RE)
        ? data.color
        : "#6ee7f7";
      const content =
        type === "brain" ? String(data.content || "").substring(0, 500) : null;
      const emotion =
        type === "emotion" ? String(data.emotion || "").substring(0, 64) : null;
      const sizeScale = clampScale(data.size_scale, 1);

      // 0 skal kunne gemmes — Number.isFinite i stedet for `|| 5`
      const rawVal = parseInt(data.emotion_val, 10);
      const emotionVal =
        type === "emotion"
          ? Number.isFinite(rawVal)
            ? Math.min(10, Math.max(0, rawVal))
            : 5
          : null;

      const rawIconId = parseInt(data.emotion_icon_id, 10);
      const emotionIconId =
        type === "emotion" && Number.isFinite(rawIconId) ? rawIconId : null;

      try {
        const [res] = await pool.execute(
          "INSERT INTO nh_bubbles (hub_id,type,x,y,color,content,emotion,emotion_val,emotion_icon_id,size_scale,user_id,username) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
          [
            hubId,
            type,
            x,
            y,
            color,
            content,
            emotion,
            emotionVal,
            emotionIconId,
            sizeScale,
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
          emotion_icon_id: emotionIconId,
          size_scale: sizeScale,
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

    socket.on("bubble_move", async (data) => {
      const id = parseInt(data.id),
        x = parseInt(data.x),
        y = parseInt(data.y);
      if (isNaN(id) || isNaN(x) || isNaN(y)) return;
      const b = bubbleCache.get(hubId)?.get(id);
      if (!b) return;
      b.x = x;
      b.y = y;
      socket.to(hubId).emit("bubble_moved", { id, x, y });
      try {
        await pool.execute(
          "UPDATE nh_bubbles SET x=?,y=? WHERE id=? AND hub_id=?",
          [x, y, id, hubId],
        );
      } catch (_) {}
    });

    // bubble_edit — opdaterer kun de felter der faktisk er sendt:
    //   color, size_scale  (alle bobler)
    //   emotion, emotion_val, emotion_icon_id  (emotion-bobler)
    //   content  (thought-bobler)
    socket.on("bubble_edit", async (data) => {
      const id = parseInt(data.id);
      if (isNaN(id)) return;
      const b = bubbleCache.get(hubId)?.get(id);
      if (!b) return;

      const sets = [];
      const vals = [];

      if (data.color !== undefined && HEX_RE.test(String(data.color))) {
        b.color = String(data.color);
        sets.push("color=?");
        vals.push(b.color);
      }

      if (data.size_scale !== undefined) {
        b.size_scale = clampScale(data.size_scale, b.size_scale ?? 1);
        sets.push("size_scale=?");
        vals.push(b.size_scale);
      }

      if (b.type === "emotion") {
        if (data.emotion !== undefined) {
          b.emotion = String(data.emotion).substring(0, 64);
          sets.push("emotion=?");
          vals.push(b.emotion);
        }
        // 0 skal kunne gemmes — samme fix som i bubble_save
        if (data.emotion_val !== undefined) {
          const parsed = parseInt(data.emotion_val, 10);
          b.emotion_val = Number.isFinite(parsed)
            ? Math.min(10, Math.max(0, parsed))
            : 5;
          sets.push("emotion_val=?");
          vals.push(b.emotion_val);
        }
        if (data.emotion_icon_id !== undefined) {
          const parsedIcon = parseInt(data.emotion_icon_id, 10);
          b.emotion_icon_id = Number.isFinite(parsedIcon) ? parsedIcon : null;
          sets.push("emotion_icon_id=?");
          vals.push(b.emotion_icon_id);
        }
      } else {
        if (data.content !== undefined) {
          b.content = String(data.content).substring(0, 500);
          sets.push("content=?");
          vals.push(b.content);
        }
      }

      if (sets.length) {
        try {
          await pool.execute(
            `UPDATE nh_bubbles SET ${sets.join(",")} WHERE id=? AND hub_id=?`,
            [...vals, id, hubId],
          );
        } catch (e) {
          console.error("[neurohub] bubble_edit:", e.message);
        }
      }
      io.to(hubId).emit("bubble_update", b);
    });

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
        await pool.execute("DELETE FROM nh_bubbles WHERE id=? AND hub_id=?", [
          id,
          hubId,
        ]);
        await pool.execute(
          "DELETE FROM nh_branches WHERE hub_id=? AND (parent_id=? OR child_id=?)",
          [hubId, id, id],
        );
      } catch (_) {}
    });

    // Forbinder to bobler der ALLEREDE findes.
    socket.on("branch_add", async (data, ack) => {
      const parentId = parseInt(data.parent_id);
      const childId = parseInt(data.child_id);
      if (isNaN(parentId) || isNaN(childId) || parentId === childId) {
        if (typeof ack === "function")
          ack({ ok: false, reason: "invalid_ids" });
        return;
      }

      const bubbles = bubbleCache.get(hubId);
      if (!bubbles || !bubbles.has(parentId) || !bubbles.has(childId)) {
        if (typeof ack === "function")
          ack({ ok: false, reason: "bubble_not_found" });
        return;
      }

      const bc = branchCache.get(hubId);
      const key1 = `${parentId}-${childId}`;
      const key2 = `${childId}-${parentId}`;
      if (bc && (bc.has(key1) || bc.has(key2))) {
        if (typeof ack === "function") ack({ ok: true, already: true });
        return;
      }

      try {
        await pool.execute(
          "INSERT IGNORE INTO nh_branches (hub_id,parent_id,child_id) VALUES (?,?,?)",
          [hubId, parentId, childId],
        );
        bc.add(key1);
        io.to(hubId).emit("branch_add", {
          parent_id: parentId,
          child_id: childId,
        });
        if (typeof ack === "function") ack({ ok: true });
      } catch (e) {
        console.error("[neurohub] branch_add:", e.message);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

    // Sletter ÉN specifik forbindelse mellem to bobler (uanset retning).
    // Boblerne selv røres ikke.
    socket.on("branch_delete", async (data, ack) => {
      const parentId = parseInt(data?.parent_id);
      const childId = parseInt(data?.child_id);
      if (isNaN(parentId) || isNaN(childId)) {
        if (typeof ack === "function")
          ack({ ok: false, reason: "invalid_ids" });
        return;
      }

      const bc = branchCache.get(hubId);
      const key1 = `${parentId}-${childId}`;
      const key2 = `${childId}-${parentId}`;
      if (bc) {
        bc.delete(key1);
        bc.delete(key2);
      }

      io.to(hubId).emit("branch_delete", {
        parent_id: parentId,
        child_id: childId,
      });

      try {
        await pool.execute(
          "DELETE FROM nh_branches WHERE hub_id=? AND ((parent_id=? AND child_id=?) OR (parent_id=? AND child_id=?))",
          [hubId, parentId, childId, childId, parentId],
        );
        if (typeof ack === "function") ack({ ok: true });
      } catch (e) {
        console.error("[neurohub] branch_delete:", e.message);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

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
        const buf = pixelBuffer.get(hubId);
        const lockedPixels = [];
        for (let px = x; px < x + w; px++) {
          for (let py = y; py < y + h; py++) {
            const key = `${px},${py}`;
            const existing = buf.get(key);
            if (existing) {
              existing.locked = 1;
              existing.locked_by_zone = 1;
              lockedPixels.push({
                x: px,
                y: py,
                color: existing.color,
                locked: 1,
              });
            }
          }
        }
        io.to(hubId).emit("zone_add", zone);
        if (lockedPixels.length > 0)
          io.to(hubId).emit("pixels_locked", lockedPixels);
        if (typeof ack === "function") ack({ ok: true, id: zone.id });
      } catch (e) {
        console.error("[neurohub] zone_add:", e.message);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

    socket.on("zone_delete", async (data, ack) => {
      if (!isAdmin) {
        if (typeof ack === "function") ack({ ok: false, reason: "not_admin" });
        return;
      }
      const id = parseInt(data.id);
      if (isNaN(id)) return;
      const zone = zoneCache.get(hubId)?.get(id);
      if (!zone) {
        if (typeof ack === "function") ack({ ok: false });
        return;
      }
      const buf = pixelBuffer.get(hubId);
      const unlockedPixels = [];
      for (let px = zone.x; px < zone.x + zone.w; px++) {
        for (let py = zone.y; py < zone.y + zone.h; py++) {
          const key = `${px},${py}`;
          const existing = buf.get(key);
          if (existing && existing.locked && existing.locked_by_zone) {
            existing.locked = 0;
            existing.locked_by_zone = 0;
            unlockedPixels.push({
              x: px,
              y: py,
              color: existing.color,
              locked: 0,
            });
          }
        }
      }
      zoneCache.get(hubId)?.delete(id);
      io.to(hubId).emit("zone_delete", { id });
      if (unlockedPixels.length > 0)
        io.to(hubId).emit("pixels_locked", unlockedPixels);
      try {
        await pool.execute("DELETE FROM nh_admin_zones WHERE id=?", [id]);
        await pool.execute(
          "UPDATE nh_pixels SET locked=0, locked_by_zone=0 WHERE hub_id=? AND x>=? AND x<? AND y>=? AND y<? AND locked_by_zone=1",
          [hubId, zone.x, zone.x + zone.w, zone.y, zone.y + zone.h],
        );
        if (typeof ack === "function") ack({ ok: true });
      } catch (e) {
        console.error("[neurohub] zone_delete:", e.message);
      }
    });

    socket.on("disconnect", () => {
      getPresence(hubId).delete(socket.id);
      io.to(hubId).emit("presence_list", presenceList(hubId));
    });
  });

  httpServer.listen(PORT, () => {
    console.log(`[neurohub] v6.7 listening on :${PORT}`);
    console.log(`[neurohub] CORS: ${ALLOWED_ORIGINS.join(", ")}`);
  });
}

async function shutdown() {
  console.log("[neurohub] Shutting down, flushing pixels…");
  await flushAllPixels();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

bootstrap().catch((err) => {
  console.error("[neurohub] Fatal:", err);
  process.exit(1);
});
