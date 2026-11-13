/**
 * neurohub_server.js — v6.12  (boble-tekst krypteret, adgangskode i klartekst)
 *
 * v6.12:
 *   · Boble-tekst (content + emotion) er krypteret i databasen (AES-256-GCM,
 *     ren base64 uden prefix). Klienterne får klartekst.
 *   · nh_hubs.access_code er KLARTEKST. Var den krypteret af en tidligere
 *     version, dekrypteres den automatisk tilbage ved opstart.
 *   · Login KRÆVER et signeret token fra index.php. Admin afgøres kun af
 *     serveren via databasen.
 *   · Private hubs tjekkes på serveren (kode + brute-force-spærring).
 *   · /reset-hub/... kræver header "x-admin-key" (env ADMIN_HTTP_KEY).
 *
 * Miljøvariabler (DigitalOcean): ENCRYPTION_KEY (SAMME som i PHP), DB_PASS,
 * ADMIN_HTTP_KEY (valgfri, men nødvendig for /reset-hub).
 */
"use strict";

const http = require("http");
const crypto = require("crypto");
const { Server } = require("socket.io");
const mysql = require("mysql2/promise");
require("dotenv").config();
const {
  decryptData,
  encryptStored,
  decryptStored,
  inspectStored,
  assertKeyConfigured,
} = require("./neurohub_crypto");

const PORT = process.env.PORT || 3001;
const ALLOWED_ORIGINS = (
  process.env.ALLOWED_ORIGINS ||
  "https://jakobkall.com,https://hub.jakobkall.com,http://localhost"
)
  .split(",")
  .map((s) => s.trim());
const ADMIN_HTTP_KEY = process.env.ADMIN_HTTP_KEY || "";

// ── Krypterings-kontekster (bruges som AAD) ───────────────────────────────────
const CTX_BUBBLE_CONTENT = "nh_bubble_content";
const CTX_BUBBLE_EMOTION = "nh_bubble_emotion";
const CTX_HUB_ACCESS = "nh_hub_access"; // kun brugt til at dekryptere gamle koder tilbage
const CTX_AUTH_TOKEN = "nh_auth_token";

const encField = (v, ctx) =>
  v === null || v === undefined ? null : encryptStored(String(v), ctx);
const decField = (v, ctx) =>
  v === null || v === undefined ? null : decryptStored(v, ctx);

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
const bubbleCache = new Map(); // indeholder KLARTEKST (kun i RAM)
const branchCache = new Map();
const chatCache = new Map();
const presence = new Map();
const zoneCache = new Map();
const adminCache = new Map(); // userId -> { v:boolean, until:ms }
const dirtyKeys = new Map();
const flushing = new Set();

const loadedHubs = new Set();
const initPromises = new Map();

const FLUSH_INTERVAL = 20_000;
const FLUSH_BATCH = 200;
const WORLD_W = 4000,
  WORLD_H = 2800;
const PIXEL_CELL = 12;
const MAX_CX = Math.floor(WORLD_W / PIXEL_CELL);
const MAX_CY = Math.floor(WORLD_H / PIXEL_CELL);
const MAX_PIXEL_OPS_PER_BATCH = 1000;
const ADMIN_TTL = 5 * 60 * 1000;

const MIN_BUBBLE_W = 60;
const MAX_BUBBLE_W = 1000;
const MIN_BUBBLE_H = 40;
const MAX_BUBBLE_H = 800;
const HEX_RE = /^#[0-9a-fA-F]{6}$/;
const HUB_ID_RE = /^[a-zA-Z0-9_-]{1,64}$/;

function parseDim(v, min, max) {
  if (v === null) return null;
  const n = parseInt(v, 10);
  if (!Number.isFinite(n)) return undefined;
  return Math.min(max, Math.max(min, n));
}

// Ensartet form på bobler fra DB (+ dekryptering af tekst)
function normalizeBubbleRow(b) {
  const { size_scale, ...rest } = b;
  return {
    ...rest,
    content: decField(rest.content, CTX_BUBBLE_CONTENT),
    emotion: decField(rest.emotion, CTX_BUBBLE_EMOTION),
    size_w: rest.size_w == null ? null : Number(rest.size_w),
    size_h: rest.size_h == null ? null : Number(rest.size_h),
  };
}

let pool;
let usersPool;
let io;

// ── Schema-hjælpere (MySQL 8 + MariaDB kompatible) ─────────────────────────────
const schema = { emotion_icon_id: false, size_w: false, size_h: false };

async function columnExists(table, column) {
  const [rows] = await pool.query(
    "SELECT COUNT(*) AS c FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?",
    [table, column],
  );
  return Number(rows[0].c) > 0;
}

async function ensureColumn(table, column, ddl) {
  try {
    if (await columnExists(table, column)) return true;
    await pool.query(`ALTER TABLE ${table} ADD COLUMN ${column} ${ddl}`);
    console.log(`[neurohub] added column ${table}.${column}`);
    return true;
  } catch (e) {
    console.error(
      `[neurohub] ❌ Kunne ikke oprette ${table}.${column}: ${e.message}\n` +
        `            Kør manuelt: ALTER TABLE ${table} ADD COLUMN ${column} ${ddl};`,
    );
    return false;
  }
}

async function ensureUniqueIndex(table, name, cols) {
  try {
    const [rows] = await pool.query(
      "SELECT COUNT(*) AS c FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
      [table, name],
    );
    if (Number(rows[0].c) > 0) return;
    await pool.query(`ALTER TABLE ${table} ADD UNIQUE INDEX ${name} ${cols}`);
    console.log(`[neurohub] added unique index ${table}.${name}`);
  } catch (e) {
    console.warn(
      `[neurohub] unique index ${table}.${name} skipped: ${e.message}`,
    );
  }
}

// ── Migrationer ───────────────────────────────────────────────────────────────

// Adgangskoder skal være KLARTEKST. Er en kode krypteret (fra en tidligere
// version), dekrypteres den og skrives tilbage som klartekst.
async function restoreHubCodesToPlain() {
  try {
    const [hubs] = await pool.query(
      "SELECT id, access_code FROM nh_hubs WHERE access_code IS NOT NULL AND access_code <> ''",
    );
    let n = 0;
    for (const h of hubs) {
      const r = inspectStored(h.access_code, CTX_HUB_ACCESS);
      if (r.state === "ok") {
        await pool.execute("UPDATE nh_hubs SET access_code=? WHERE id=?", [
          r.text,
          h.id,
        ]);
        n++;
      } else if (r.state === "bad") {
        console.warn(
          `[neurohub] hub "${h.id}": adgangskoden ligner krypteret men kan ikke dekrypteres (forkert ENCRYPTION_KEY?) — rører den ikke`,
        );
      }
    }
    if (n)
      console.log(`[neurohub] restored ${n} hub access code(s) to plaintext`);
  } catch (e) {
    console.error("[neurohub] restoreHubCodesToPlain:", e.message);
  }
}

// Boble-tekst skal være krypteret uden prefix. Klartekst krypteres, "v2:"-
// prefix fjernes. Værdier der ikke kan dekrypteres, røres ikke.
function migratedValue(value, ctx) {
  const r = inspectStored(value, ctx);
  if (r.state === "plain") return encryptStored(r.text, ctx);
  if (r.state === "ok" && r.hasPrefix) return encryptStored(r.text, ctx);
  if (r.state === "bad")
    console.warn(
      "[neurohub] migration: en værdi kan ikke dekrypteres (forkert ENCRYPTION_KEY?) — rører den ikke",
    );
  return null;
}

async function migrateBubbleText() {
  try {
    const [rows] = await pool.query(
      `SELECT id, content, emotion FROM nh_bubbles
       WHERE (content IS NOT NULL AND content <> '')
          OR (emotion IS NOT NULL AND emotion <> '')`,
    );
    let n = 0;
    for (const r of rows) {
      const c = migratedValue(r.content, CTX_BUBBLE_CONTENT);
      const e = migratedValue(r.emotion, CTX_BUBBLE_EMOTION);
      if (c === null && e === null) continue;
      await pool.execute(
        "UPDATE nh_bubbles SET content=?, emotion=? WHERE id=?",
        [c !== null ? c : r.content, e !== null ? e : r.emotion, r.id],
      );
      n++;
    }
    if (n) console.log(`[neurohub] migrated text of ${n} bubble(s)`);
  } catch (e) {
    console.error("[neurohub] migrateBubbleText:", e.message);
  }
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────
async function bootstrap() {
  assertKeyConfigured(); // stopper opstart hvis ENCRYPTION_KEY mangler

  pool = await mysql.createPool(NEUROHUB_DB);
  usersPool = await mysql.createPool(USERS_DB);
  console.log("[neurohub] Both DB pools connected");

  try {
    const [rows] = await usersPool.execute("SELECT COUNT(*) AS cnt FROM users");
    console.log(`[neurohub] users table: ${rows[0].cnt} users found`);
  } catch (e) {
    console.error("[neurohub] WARNING: Cannot read users table:", e.message);
  }

  await ensureColumn("nh_pixels", "locked", "TINYINT(1) NOT NULL DEFAULT 0");
  await ensureColumn(
    "nh_pixels",
    "locked_by_zone",
    "TINYINT(1) NOT NULL DEFAULT 0",
  );
  schema.emotion_icon_id = await ensureColumn(
    "nh_bubbles",
    "emotion_icon_id",
    "INT NULL DEFAULT NULL",
  );
  schema.size_w = await ensureColumn(
    "nh_bubbles",
    "size_w",
    "INT NULL DEFAULT NULL",
  );
  schema.size_h = await ensureColumn(
    "nh_bubbles",
    "size_h",
    "INT NULL DEFAULT NULL",
  );
  await ensureUniqueIndex("nh_pixels", "ux_hub_xy", "(hub_id,x,y)");
  console.log("[neurohub] schema:", JSON.stringify(schema));

  // FØR warm-up
  await restoreHubCodesToPlain();
  await migrateBubbleText();

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

function ensureHubBuffers(hubId) {
  if (!pixelBuffer.has(hubId)) pixelBuffer.set(hubId, new Map());
  if (!erasedKeys.has(hubId)) erasedKeys.set(hubId, new Set());
  if (!dirtyKeys.has(hubId)) dirtyKeys.set(hubId, new Set());
  if (!bubbleCache.has(hubId)) bubbleCache.set(hubId, new Map());
  if (!branchCache.has(hubId)) branchCache.set(hubId, new Set());
  if (!chatCache.has(hubId)) chatCache.set(hubId, []);
  if (!presence.has(hubId)) presence.set(hubId, new Map());
  if (!zoneCache.has(hubId)) zoneCache.set(hubId, new Map());
}

// ── RAM reset for a hub ───────────────────────────────────────────────────────
async function resetHubRAM(hubId) {
  console.log(`[neurohub] resetHubRAM: clearing RAM for hub "${hubId}"`);
  await flushPixels(hubId).catch((e) =>
    console.error("[neurohub] flush before reset:", e.message),
  );
  pixelBuffer.set(hubId, new Map());
  erasedKeys.set(hubId, new Set());
  dirtyKeys.set(hubId, new Set());
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

    const [bubbles] = await pool.query(
      "SELECT * FROM nh_bubbles WHERE hub_id = ?",
      [hubId],
    );
    bubbles.forEach((b) =>
      bubbleCache.get(hubId).set(b.id, normalizeBubbleRow(b)),
    );

    const [branches] = await pool.query(
      "SELECT * FROM nh_branches WHERE hub_id = ?",
      [hubId],
    );
    branches.forEach((br) =>
      branchCache.get(hubId).add(`${br.parent_id}-${br.child_id}`),
    );

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

// ── Login: token, admin, hub-adgang ───────────────────────────────────────────

function verifyAuthToken(token) {
  if (typeof token !== "string" || token === "") {
    console.warn(
      "[neurohub] token mangler (er index.php opdateret og uploadet?)",
    );
    return null;
  }
  if (token.length < 20 || token.length > 4096 || !token.startsWith("v2:")) {
    console.warn("[neurohub] token har forkert format");
    return null;
  }
  const plain = decryptData(token, CTX_AUTH_TOKEN);
  if (!plain) {
    console.warn(
      "[neurohub] token kunne ikke dekrypteres — ENCRYPTION_KEY på serveren er IKKE den samme som i PHP",
    );
    return null;
  }
  let p;
  try {
    p = JSON.parse(plain);
  } catch (_) {
    console.warn("[neurohub] token indeholder ugyldig JSON");
    return null;
  }
  if (!p || p.t !== "nh_auth" || !p.uid || !p.un) {
    console.warn("[neurohub] token mangler felter");
    return null;
  }
  if (typeof p.exp !== "number" || p.exp < Math.floor(Date.now() / 1000)) {
    console.warn("[neurohub] token er udløbet");
    return null;
  }
  return { userId: String(p.uid), username: String(p.un) };
}

function parseIsAdmin(userTypeStr) {
  if (!userTypeStr) return false;
  return userTypeStr
    .split(",")
    .map((t) => t.trim().toLowerCase())
    .includes("admin");
}

// Admin afgøres KUN her, ud fra det verificerede user_id og databasen.
async function resolveAdmin(userId) {
  const k = String(userId);
  const c = adminCache.get(k);
  if (c && c.until > Date.now()) return c.v;
  let isAdmin = false;
  try {
    const [rows] = await usersPool.execute(
      "SELECT user_type FROM users WHERE user_id=? LIMIT 1",
      [k],
    );
    if (rows.length > 0) isAdmin = parseIsAdmin(rows[0].user_type);
  } catch (e) {
    console.error("[neurohub] resolveAdmin:", e.message);
    return false; // fejl caches ikke
  }
  adminCache.set(k, { v: isAdmin, until: Date.now() + ADMIN_TTL });
  return isAdmin;
}

const sha = (s) =>
  crypto.createHash("sha256").update(String(s).trim().toLowerCase()).digest();
const safeEqual = (a, b) => crypto.timingSafeEqual(sha(a), sha(b));

// Brute-force-spærring for adgangskoder: 10 fejl pr. 10 min pr. bruger/IP
const codeFails = new Map();
const MAX_FAILS = 10;
const FAIL_WINDOW = 10 * 60 * 1000;
function isLockedOut(keys) {
  const now = Date.now();
  return keys.some((k) => {
    const f = codeFails.get(k);
    return f && f.until > now && f.n >= MAX_FAILS;
  });
}
function registerFail(keys) {
  const now = Date.now();
  keys.forEach((k) => {
    let f = codeFails.get(k);
    if (!f || f.until <= now) f = { n: 0, until: now + FAIL_WINDOW };
    f.n++;
    codeFails.set(k, f);
  });
}
function clearFails(keys) {
  keys.forEach((k) => codeFails.delete(k));
}
setInterval(() => {
  const now = Date.now();
  codeFails.forEach((f, k) => {
    if (f.until <= now) codeFails.delete(k);
  });
}, FAIL_WINDOW);

// Returnerer "ok" | "not_found" | "access_denied" | "too_many_attempts"
async function checkHubAccess(socket, hubId, code) {
  if (hubId === "public") return "ok";
  const [rows] = await pool.execute(
    "SELECT is_private, access_code FROM nh_hubs WHERE id=? LIMIT 1",
    [hubId],
  );
  if (!rows.length) return "not_found";
  if (!rows[0].is_private) return "ok";

  const ip =
    String(socket.handshake.headers["x-forwarded-for"] || "")
      .split(",")[0]
      .trim() || socket.handshake.address;
  const keys = [`u:${socket.userId}`, `ip:${ip}`];

  if (isLockedOut(keys)) return "too_many_attempts";
  if (!code) return "access_denied";

  // Adgangskoden står i klartekst i databasen
  const stored = String(rows[0].access_code || "");
  if (stored && safeEqual(stored, String(code).substring(0, 64))) {
    clearFails(keys);
    return "ok";
  }
  registerFail(keys);
  return "access_denied";
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

// ── DB flushing ───────────────────────────────────────────────────────────────
async function flushPixels(hubId) {
  if (flushing.has(hubId)) return;
  flushing.add(hubId);
  try {
    const buf = pixelBuffer.get(hubId);
    const erased = erasedKeys.get(hubId);
    const dirty = dirtyKeys.get(hubId);

    if (erased && erased.size > 0) {
      const keys = [...erased];
      erased.clear();
      for (let i = 0; i < keys.length; i += FLUSH_BATCH) {
        const chunk = keys.slice(i, i + FLUSH_BATCH);
        const ph = chunk.map(() => "(?,?)").join(",");
        const vals = [hubId];
        chunk.forEach((k) => {
          const [x, y] = k.split(",").map(Number);
          vals.push(x, y);
        });
        try {
          await pool.query(
            `DELETE FROM nh_pixels WHERE hub_id=? AND (x,y) IN (${ph})`,
            vals,
          );
        } catch (err) {
          console.error("[neurohub] pixel delete error:", err.message);
          chunk.forEach((k) => erased.add(k));
        }
      }
    }

    if (buf && dirty && dirty.size > 0) {
      const keys = [...dirty];
      dirty.clear();
      const rows = keys.map((k) => buf.get(k)).filter(Boolean);
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
          chunk.forEach((p) => dirty.add(`${p.x},${p.y}`));
        }
      }
    }
  } finally {
    flushing.delete(hubId);
  }
}

function scheduleFlushIfBig(hubId) {
  const dirty = dirtyKeys.get(hubId);
  if (dirty && dirty.size >= FLUSH_BATCH && !flushing.has(hubId))
    flushPixels(hubId).catch((e) =>
      console.error("[neurohub] flush error:", e.message),
    );
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
function httpKeyOk(req) {
  if (!ADMIN_HTTP_KEY) return false; // ikke sat = endpoint er slået fra
  const given = String(req.headers["x-admin-key"] || "");
  return given !== "" && safeEqual(given, ADMIN_HTTP_KEY);
}

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
          version: "6.12",
          schema,
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
      if (!httpKeyOk(req)) {
        res.writeHead(403, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "forbidden" }));
      }
      const hubId = decodeURIComponent(resetMatch[1]);
      if (!HUB_ID_RE.test(hubId)) {
        res.writeHead(400);
        return res.end();
      }
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
    try {
      const auth = socket.handshake.auth || {};

      // Identitet kommer KUN fra det signerede token — aldrig fra user_id/username
      const who = verifyAuthToken(auth.token);
      if (!who) return next(new Error("invalid_token"));

      const hubId = String(auth.hub_id || "public");
      if (!HUB_ID_RE.test(hubId)) return next(new Error("hub_not_found"));

      socket.userId = who.userId;
      socket.username = who.username.substring(0, 64);
      socket.hubId = hubId;

      const access = await checkHubAccess(socket, hubId, auth.access_code);
      if (access !== "ok") return next(new Error(access));

      const hubExists = await initHubIfNeeded(hubId);
      if (!hubExists) return next(new Error("hub_not_found"));

      socket.isAdmin = await resolveAdmin(socket.userId);
      next();
    } catch (e) {
      console.error("[neurohub] auth middleware:", e.message);
      next(new Error("auth_failed"));
    }
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

    function handlePixelOp(data, out) {
      const x = parseInt(data?.x),
        y = parseInt(data?.y);
      if (isNaN(x) || isNaN(y) || x < 0 || y < 0 || x >= MAX_CX || y >= MAX_CY)
        return;
      const erase = !!data.erase;
      const key = `${x},${y}`;
      const buf = pixelBuffer.get(hubId);
      const erased = erasedKeys.get(hubId);
      const dirty = dirtyKeys.get(hubId);
      const existing = buf.get(key);
      const inLockedZone = isInLockedZone(hubId, x, y);
      const isLockedPixel = existing && existing.locked;

      if (!isAdmin && (inLockedZone || isLockedPixel)) {
        const zone = getZoneAt(hubId, x, y);
        if (zone) out.zones.set(zone.id, zone);
        else out.pixels.push({ x, y });
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
        dirty.delete(key);
        erased.add(key);
      } else {
        erased.delete(key);
        dirty.add(key);
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
      out.accepted.push({
        x,
        y,
        color: erase ? null : color,
        erase,
        locked: locked ? 1 : 0,
      });
    }

    function processPixelOps(list) {
      const out = { accepted: [], zones: new Map(), pixels: [] };
      for (const op of list) handlePixelOp(op, out);
      if (out.accepted.length)
        socket.to(hubId).emit("pixels", { list: out.accepted });
      out.zones.forEach((zone) =>
        socket.emit("zone_flash", { zone, pixels: null }),
      );
      if (out.pixels.length)
        socket.emit("zone_flash", { zone: null, pixels: out.pixels });
      scheduleFlushIfBig(hubId);
    }

    socket.on("pixel", (data) => processPixelOps([data]));
    socket.on("pixels", (data) => {
      const list = Array.isArray(data?.list)
        ? data.list.slice(0, MAX_PIXEL_OPS_PER_BATCH)
        : [];
      if (list.length) processPixelOps(list);
    });

    socket.on("chat", async (data) => {
      const message = String(data?.message || "")
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
      const x = Math.max(
        0,
        Math.min(WORLD_W, parseInt(data?.x) || WORLD_W / 2),
      );
      const y = Math.max(
        0,
        Math.min(WORLD_H, parseInt(data?.y) || WORLD_H / 2),
      );
      const p = getPresence(hubId).get(socket.id);
      if (p) {
        p.x = x;
        p.y = y;
      }
      socket.to(hubId).emit("move", { user_id: userId, username, x, y });
    });

    socket.on("bubble_save", async (data, ack) => {
      data = data || {};
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

      const sizeW =
        parseDim(data.size_w ?? null, MIN_BUBBLE_W, MAX_BUBBLE_W) ?? null;
      const sizeH =
        parseDim(data.size_h ?? null, MIN_BUBBLE_H, MAX_BUBBLE_H) ?? null;

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

      let parentId = null;
      if (data.parent_id !== undefined && data.parent_id !== null) {
        const pid = parseInt(data.parent_id, 10);
        if (Number.isFinite(pid) && bubbleCache.get(hubId)?.has(pid))
          parentId = pid;
      }

      try {
        const cols = [
          "hub_id",
          "type",
          "x",
          "y",
          "color",
          "content",
          "emotion",
          "emotion_val",
        ];
        // Teksten krypteres FØR den skrives til databasen
        const vals = [
          hubId,
          type,
          x,
          y,
          color,
          encField(content, CTX_BUBBLE_CONTENT),
          encField(emotion, CTX_BUBBLE_EMOTION),
          emotionVal,
        ];
        if (schema.emotion_icon_id) {
          cols.push("emotion_icon_id");
          vals.push(emotionIconId);
        }
        if (schema.size_w) {
          cols.push("size_w");
          vals.push(sizeW);
        }
        if (schema.size_h) {
          cols.push("size_h");
          vals.push(sizeH);
        }
        cols.push("user_id", "username");
        vals.push(userId, username);
        const [res] = await pool.execute(
          `INSERT INTO nh_bubbles (${cols.join(",")}) VALUES (${cols.map(() => "?").join(",")})`,
          vals,
        );
        const bubbleId = res.insertId;
        // RAM/klienter arbejder med klartekst
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
          size_w: sizeW,
          size_h: sizeH,
          username,
          user_id: userId,
        };
        bubbleCache.get(hubId).set(bubbleId, bubble);
        if (parentId !== null)
          branchCache.get(hubId).add(`${parentId}-${bubbleId}`);

        if (typeof ack === "function") ack({ ok: true, id: bubbleId, bubble });

        socket.to(hubId).emit("bubble_update", bubble);
        if (parentId !== null)
          socket.to(hubId).emit("branch_add", {
            parent_id: parentId,
            child_id: bubbleId,
          });

        if (parentId !== null) {
          try {
            await pool.execute(
              "INSERT IGNORE INTO nh_branches (hub_id,parent_id,child_id) VALUES (?,?,?)",
              [hubId, parentId, bubbleId],
            );
          } catch (e) {
            console.error("[neurohub] bubble_save branch insert:", e.message);
          }
        }
      } catch (e) {
        console.error(
          "[neurohub] bubble_save FAILED:",
          e.code || "",
          e.message,
        );
        if (typeof ack === "function")
          ack({ ok: false, error: String(e.code || e.message || "db_error") });
      }
    });

    socket.on("bubble_move", async (data) => {
      const id = parseInt(data?.id),
        x = parseInt(data?.x),
        y = parseInt(data?.y);
      if (isNaN(id) || isNaN(x) || isNaN(y)) return;
      const b = bubbleCache.get(hubId)?.get(id);
      if (!b) return;
      b.x = x;
      b.y = y;
      socket.to(hubId).emit("bubble_moved", { id, x, y });
      if (data.final === false) return;
      try {
        await pool.execute(
          "UPDATE nh_bubbles SET x=?,y=? WHERE id=? AND hub_id=?",
          [x, y, id, hubId],
        );
      } catch (_) {}
    });

    socket.on("bubble_edit", async (data) => {
      data = data || {};
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

      if (data.size_w !== undefined) {
        const w = parseDim(data.size_w, MIN_BUBBLE_W, MAX_BUBBLE_W);
        if (w !== undefined) {
          b.size_w = w;
          if (schema.size_w) {
            sets.push("size_w=?");
            vals.push(w);
          }
        }
      }
      if (data.size_h !== undefined) {
        const h = parseDim(data.size_h, MIN_BUBBLE_H, MAX_BUBBLE_H);
        if (h !== undefined) {
          b.size_h = h;
          if (schema.size_h) {
            sets.push("size_h=?");
            vals.push(h);
          }
        }
      }

      if (b.type === "emotion") {
        if (data.emotion !== undefined) {
          b.emotion = String(data.emotion).substring(0, 64);
          sets.push("emotion=?");
          vals.push(encField(b.emotion, CTX_BUBBLE_EMOTION)); // krypteret i DB
        }
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
          if (schema.emotion_icon_id) {
            sets.push("emotion_icon_id=?");
            vals.push(b.emotion_icon_id);
          }
        }
      } else {
        if (data.content !== undefined) {
          b.content = String(data.content).substring(0, 500);
          sets.push("content=?");
          vals.push(encField(b.content, CTX_BUBBLE_CONTENT)); // krypteret i DB
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
      socket.to(hubId).emit("bubble_update", b);
    });

    socket.on("bubble_delete", async (data) => {
      const id = parseInt(data?.id);
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

    socket.on("branch_add", async (data, ack) => {
      const parentId = parseInt(data?.parent_id);
      const childId = parseInt(data?.child_id);
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
        socket.to(hubId).emit("branch_add", {
          parent_id: parentId,
          child_id: childId,
        });
        if (typeof ack === "function") ack({ ok: true });
      } catch (e) {
        console.error("[neurohub] branch_add:", e.message);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

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

      socket.to(hubId).emit("branch_delete", {
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

    // ── Admin-zoner: kun for brugere som SERVEREN har verificeret som admin ──
    socket.on("zone_add", async (data, ack) => {
      if (!isAdmin) {
        if (typeof ack === "function") ack({ ok: false, reason: "not_admin" });
        return;
      }
      data = data || {};
      const x = parseInt(data.x),
        y = parseInt(data.y),
        w = parseInt(data.w),
        h = parseInt(data.h);
      const label = String(data.label || "").substring(0, 64);
      if (isNaN(x) || isNaN(y) || isNaN(w) || isNaN(h) || w < 1 || h < 1) {
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
      const id = parseInt(data?.id);
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
        await pool.execute(
          "DELETE FROM nh_admin_zones WHERE id=? AND hub_id=?",
          [id, hubId],
        );
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
    console.log(`[neurohub] v6.12 listening on :${PORT}`);
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
