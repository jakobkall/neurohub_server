/**
 * neurohub_server.js — v6.11  (adgangskontrol + kryptering af boble-tekst)
 *
 * v6.11: SERVEREN TJEKKER NU access_code for private hubs ved socket-forbindelse
 *   (før hubben indlæses). Koden i DB må være klartekst eller krypteret
 *   (kontekst "nh_hubs.access_code"). Rate limit: 5 forkerte koder pr. IP+hub
 *   og 60 pr. hub pr. 10 min. /hubs viser ikke længere private hubs.
 *   HUB_ACCESS_MODE=log giver en "tør-kørsel" uden at nægte nogen.
 *
 * v6.10:
 *   · KRYPTERING: al tekst skrevet i bobler (thought: content, emotion: emotion)
 *     krypteres med AES-256-GCM via ./neurohub_crypto.js (port af den centrale
 *     public_html/includes/crypto.inc.php — samme "v2:"-format og nøgle).
 *     - DB indeholder kun krypteret tekst ("v2:...").
 *     - RAM-cache og socket-trafik er klartekst (brug https/wss i produktion).
 *     - Ved opstart: kolonnerne udvides til TEXT, og eksisterende klartekst-
 *       bobler krypteres automatisk (engangs-migration).
 *     - Kontekst (AAD): "nh_bubbles.content" / "nh_bubbles.emotion"
 *       → i PHP:  decryptData($v, 'nh_bubbles.content')
 *   · BOBLE-STØRRELSER (skal matche klienten v9.13):
 *       bredde: thought 140–600 px, emotion 210–600 px
 *       højde : thought 56–500 px,  emotion 130–500 px
 *     Højden er en MINIMUM-højde; boblen vokser selv hvis teksten kræver det,
 *     så tekst aldrig skæres af og der aldrig kommer scrollbar.
 *
 * v6.9.1 — FIX: "ny boble vises kort og forsvinder igen" (migrationer uden
 *   ADD COLUMN IF NOT EXISTS; INSERT bygges ud fra eksisterende kolonner).
 *
 * v6.9 — performance: dirty-pixel flush, batch-"pixels", ack-first bubble_save,
 *   broadcast kun til de andre, live bubble_move uden DB-skrivning.
 */
"use strict";

const http = require("http");
const crypto = require("crypto");
const { Server } = require("socket.io");
const mysql = require("mysql2/promise");
require("dotenv").config();
const {
  encryptData,
  decryptData,
  assertKeyConfigured,
} = require("./neurohub_crypto");

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
const bubbleCache = new Map(); // klartekst i RAM
const branchCache = new Map();
const chatCache = new Map();
const presence = new Map();
const zoneCache = new Map();
const adminCache = new Map();
const dirtyKeys = new Map(); // hubId -> Set("x,y") af pixels der skal skrives til DB
const flushing = new Set(); // hubIds hvor en flush kører lige nu

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

// ── Boble-grænser (SKAL matche klienten) ──────────────────────────────────────
const MIN_BUBBLE_W_THOUGHT = 140;
const MIN_BUBBLE_W_EMOTION = 210;
const MAX_BUBBLE_W = 600;
const MIN_BUBBLE_H_THOUGHT = 56;
const MIN_BUBBLE_H_EMOTION = 130;
const MAX_BUBBLE_H = 500;
const MAX_THOUGHT_CHARS = 500;
const MAX_EMOTION_CHARS = 64;
const HEX_RE = /^#[0-9a-fA-F]{6}$/;

const minW = (type) =>
  type === "emotion" ? MIN_BUBBLE_W_EMOTION : MIN_BUBBLE_W_THOUGHT;
const minH = (type) =>
  type === "emotion" ? MIN_BUBBLE_H_EMOTION : MIN_BUBBLE_H_THOUGHT;

// ── Kryptering af boble-tekst ─────────────────────────────────────────────────
const CTX_CONTENT = "nh_bubbles.content";
const CTX_EMOTION = "nh_bubbles.emotion";

// null → null (kolonnen er NULL for den anden boble-type), ellers krypteret
function encField(v, ctx) {
  if (v === null || v === undefined) return null;
  return encryptData(String(v), ctx);
}
function decField(v, ctx) {
  if (v === null || v === undefined) return v;
  return decryptData(v, ctx);
}

// null  → nulstil til auto
// tal   → clampet heltal
// ugyldigt → undefined (ignoreres)
function parseDim(v, min, max) {
  if (v === null) return null;
  const n = parseInt(v, 10);
  if (!Number.isFinite(n)) return undefined;
  return Math.min(max, Math.max(min, n));
}

// Ensartet form på bobler fra DB: tekst dekrypteret, size_w/size_h er tal
// (clampet til de gældende grænser) eller null, og den gamle size_scale-kolonne
// følger ikke med ud til klienterne.
function normalizeBubbleRow(b) {
  const { size_scale, ...rest } = b;
  const w = rest.size_w == null ? null : Number(rest.size_w);
  const h = rest.size_h == null ? null : Number(rest.size_h);
  return {
    ...rest,
    content: decField(rest.content, CTX_CONTENT),
    emotion: decField(rest.emotion, CTX_EMOTION),
    size_w: Number.isFinite(w)
      ? Math.min(MAX_BUBBLE_W, Math.max(minW(rest.type), Math.round(w)))
      : null,
    size_h: Number.isFinite(h)
      ? Math.min(MAX_BUBBLE_H, Math.max(minH(rest.type), Math.round(h)))
      : null,
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

// Returnerer true hvis kolonnen findes (eller blev oprettet)
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

// Sikrer at en kolonne er en TEXT-type (krypteret tekst er længere end klartekst).
// Returnerer true hvis kolonnen kan rumme krypteret tekst.
async function ensureTextColumn(table, column) {
  try {
    const [rows] = await pool.query(
      "SELECT DATA_TYPE AS t FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?",
      [table, column],
    );
    if (!rows.length) return false;
    const t = String(rows[0].t).toLowerCase();
    if (["text", "mediumtext", "longtext"].includes(t)) return true;
    await pool.query(`ALTER TABLE ${table} MODIFY COLUMN ${column} TEXT NULL`);
    console.log(`[neurohub] widened ${table}.${column} (${t} → TEXT)`);
    return true;
  } catch (e) {
    console.error(
      `[neurohub] ❌ Kunne ikke udvide ${table}.${column} til TEXT: ${e.message}\n` +
        `            Kør manuelt: ALTER TABLE ${table} MODIFY COLUMN ${column} TEXT NULL;`,
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

// Engangs-migration: krypterer eksisterende klartekst-bobler.
// Rækker der allerede starter med "v2:" røres ikke → sikker at køre ved hver start.
async function migrateEncryptBubbles() {
  const [rows] = await pool.query(
    `SELECT id, content, emotion FROM nh_bubbles
      WHERE (content IS NOT NULL AND content <> '' AND content NOT LIKE 'v2:%')
         OR (emotion IS NOT NULL AND emotion <> '' AND emotion NOT LIKE 'v2:%')`,
  );
  if (!rows.length) return;
  console.log(`[neurohub] krypterer ${rows.length} eksisterende boble(r)…`);
  let done = 0;
  for (const r of rows) {
    const sets = [];
    const vals = [];
    if (r.content && !String(r.content).startsWith("v2:")) {
      sets.push("content=?");
      vals.push(encryptData(r.content, CTX_CONTENT));
    }
    if (r.emotion && !String(r.emotion).startsWith("v2:")) {
      sets.push("emotion=?");
      vals.push(encryptData(r.emotion, CTX_EMOTION));
    }
    if (!sets.length) continue;
    try {
      await pool.execute(`UPDATE nh_bubbles SET ${sets.join(",")} WHERE id=?`, [
        ...vals,
        r.id,
      ]);
      done++;
    } catch (e) {
      console.error(`[neurohub] migrate bubble ${r.id}:`, e.message);
    }
  }
  console.log(`[neurohub] ${done}/${rows.length} boble(r) krypteret`);
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────
async function bootstrap() {
  // Nægter at starte uden ENCRYPTION_KEY — og kører en krypterings-selvtest
  const keyInfo = assertKeyConfigured();
  console.log(
    `[neurohub] ENCRYPTION_KEY OK (kilde: ${keyInfo.source}, ${keyInfo.length} tegn) — selvtest bestået`,
  );
  if (keyInfo.insecure)
    console.warn(
      "[neurohub] ⚠️ ENCRYPTION_KEY er PHP-filens offentlige standardnøgle — brug en rigtig, hemmelig nøgle!",
    );

  pool = await mysql.createPool(NEUROHUB_DB);
  usersPool = await mysql.createPool(USERS_DB);
  console.log("[neurohub] Both DB pools connected");

  try {
    const [rows] = await usersPool.execute("SELECT COUNT(*) AS cnt FROM users");
    console.log(`[neurohub] users table: ${rows[0].cnt} users found`);
  } catch (e) {
    console.error("[neurohub] WARNING: Cannot read users table:", e.message);
  }

  // Schema extensions — kører FØR warmUp(). Bruger information_schema i stedet
  // for "IF NOT EXISTS" (som MySQL 8 ikke kender for ADD COLUMN).
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

  // Krypteret tekst er længere end klartekst → kolonnerne SKAL være TEXT.
  // Kan det ikke lade sig gøre, stopper vi hellere end at gemme noget forkert.
  const okContent = await ensureTextColumn("nh_bubbles", "content");
  const okEmotion = await ensureTextColumn("nh_bubbles", "emotion");
  if (!okContent || !okEmotion) {
    throw new Error(
      "nh_bubbles.content / nh_bubbles.emotion er ikke TEXT-kolonner — kan ikke gemme krypteret tekst.",
    );
  }
  await migrateEncryptBubbles();

  console.log("[neurohub] schema:", JSON.stringify(schema));

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

  // SELECT * så size_w / size_h / emotion_icon_id automatisk følger med
  const [bubbles] = await pool.query("SELECT * FROM nh_bubbles");
  bubbles.forEach((b) => {
    loadedHubs.add(b.hub_id);
    ensureHubBuffers(b.hub_id);
    bubbleCache.get(b.hub_id).set(b.id, normalizeBubbleRow(b)); // dekrypterer
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
  erasedKeys.get(hubId)?.clear();
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

    // Load bubbles (SELECT * picks up emotion_icon_id + size_w + size_h;
    // normalizeBubbleRow dekrypterer teksten)
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

// ── Adgangskontrol til private hubs ───────────────────────────────────────────
// Serveren tjekker selv adgangskoden, når en socket forbinder (klienten sender
// den som auth.access_code). Koden i DB kan være klartekst (gamle hubs) ELLER
// krypteret ("v2:…", kontekst "nh_hubs.access_code") — begge dele virker.
//
// Miljøvariabler (valgfrie):
//   HUB_ACCESS_MODE=enforce | log   (standard: enforce)
//        log = nægter ikke, men skriver i loggen hvem der ville være afvist
//              (til at teste, at PHP-siden sender koden, før du håndhæver)
//   LIST_PRIVATE_HUBS=true          (standard: private hubs skjules i /hubs)
const CTX_ACCESS = "nh_hubs.access_code";
const ACCESS_MODE = String(
  process.env.HUB_ACCESS_MODE || "enforce",
).toLowerCase();
const LIST_PRIVATE_HUBS = process.env.LIST_PRIVATE_HUBS === "true";
const ATTEMPT_WINDOW_MS = 10 * 60 * 1000; // 10 min
const MAX_FAILS_PER_IP_HUB = 5; // forkerte koder pr. IP og hub pr. vindue
const MAX_FAILS_PER_HUB = 60; // forkerte koder i alt pr. hub pr. vindue
const failMap = new Map(); // nøgle -> { count, first }

function failCount(key, now) {
  const r = failMap.get(key);
  if (!r) return 0;
  if (now - r.first > ATTEMPT_WINDOW_MS) {
    failMap.delete(key);
    return 0;
  }
  return r.count;
}
function addFail(key, now) {
  const r = failMap.get(key);
  if (!r || now - r.first > ATTEMPT_WINDOW_MS)
    failMap.set(key, { count: 1, first: now });
  else r.count++;
}
setInterval(() => {
  const now = Date.now();
  for (const [k, r] of failMap)
    if (now - r.first > ATTEMPT_WINDOW_MS) failMap.delete(k);
}, 60_000).unref();

// Bag DigitalOcean/proxy: brug proxyens egen header, ikke klientens
function clientIp(socket) {
  const h = socket.handshake.headers || {};
  const fwd = String(h["x-forwarded-for"] || "")
    .split(",")
    .pop()
    .trim();
  return String(
    h["do-connecting-ip"] || fwd || socket.handshake.address || "unknown",
  );
}

function safeEqual(a, b) {
  const ha = crypto.createHash("sha256").update(String(a)).digest();
  const hb = crypto.createHash("sha256").update(String(b)).digest();
  return crypto.timingSafeEqual(ha, hb);
}

// → { ok:true } | { ok:false, error:"hub_not_found"|"access_denied"|"too_many_attempts" }
async function checkHubAccess(socket, hubId, code) {
  const [rows] = await pool.execute(
    "SELECT id, is_private, access_code FROM nh_hubs WHERE id = ?",
    [hubId],
  );
  if (!rows.length) return { ok: false, error: "hub_not_found" };
  const hub = rows[0];
  if (!hub.is_private) return { ok: true };

  const now = Date.now();
  const kIp = `${clientIp(socket)}|${hubId}`;
  const kHub = `hub|${hubId}`;
  if (
    failCount(kIp, now) >= MAX_FAILS_PER_IP_HUB ||
    failCount(kHub, now) >= MAX_FAILS_PER_HUB
  )
    return { ok: false, error: "too_many_attempts" };

  const stored =
    hub.access_code == null
      ? ""
      : decryptData(String(hub.access_code), CTX_ACCESS).trim();
  const given = code == null ? "" : String(code).trim();

  if (stored === "") {
    // Privat hub uden (læsbar) kode → ingen kommer ind (fail closed)
    console.error(
      `[neurohub] hub "${hubId}" er privat men har ingen læsbar access_code`,
    );
    return { ok: false, error: "access_denied" };
  }
  if (given !== "" && safeEqual(stored, given)) {
    failMap.delete(kIp);
    return { ok: true };
  }
  // Tom kode (side åbnet uden kode) tæller ikke som forsøg — kun forkerte koder
  if (given !== "") {
    addFail(kIp, now);
    addFail(kHub, now);
  }
  return { ok: false, error: "access_denied" };
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
// Skriver KUN de pixels der er ændret siden sidst (dirty) + sletninger.
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
          chunk.forEach((k) => erased.add(k)); // prøv igen næste gang
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
          chunk.forEach((p) => dirty.add(`${p.x},${p.y}`)); // prøv igen næste gang
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
          version: "6.11.0",
          hubAccessMode: ACCESS_MODE,
          encryption: "aes-256-gcm (v2)",
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
        // Private hubs (fx booking-sessioner) afslører ellers id, tidspunkt og
        // eksistens for alle → skjules, medmindre LIST_PRIVATE_HUBS=true.
        const [hubs] = await pool.query(
          `SELECT id, label, is_private FROM nh_hubs ${LIST_PRIVATE_HUBS ? "" : "WHERE is_private = 0"} ORDER BY created_at DESC LIMIT 50`,
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

    // 🔒 Adgangskontrol FØR hubben indlæses. Admin-flaget bruges bevidst ikke
    // til at springe koden over: user_id kommer fra klienten og kan forfalskes.
    let access;
    try {
      access = await checkHubAccess(
        socket,
        socket.hubId,
        socket.handshake.auth?.access_code,
      );
    } catch (e) {
      console.error("[neurohub] checkHubAccess:", e.message);
      return next(new Error("server_error")); // fail closed
    }
    if (!access.ok) {
      if (access.error === "hub_not_found")
        return next(new Error("hub_not_found"));
      console.warn(
        `[neurohub] adgang nægtet til "${socket.hubId}" (${access.error}) fra ${clientIp(socket)} user=${socket.userId}`,
      );
      if (ACCESS_MODE === "enforce") return next(new Error(access.error));
      console.warn(
        "[neurohub] HUB_ACCESS_MODE=log → lader forbindelsen igennem",
      );
    }

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

    // Fælles logik for ét pixel-forsøg. Returnerer intet; resultater samles i `out`.
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
      // Klartekst (til cache/klienter) — krypteres først lige før DB-skrivning
      const content =
        type === "brain"
          ? String(data.content || "").substring(0, MAX_THOUGHT_CHARS)
          : null;
      const emotion =
        type === "emotion"
          ? String(data.emotion || "").substring(0, MAX_EMOTION_CHARS)
          : null;

      const sizeW =
        parseDim(data.size_w ?? null, minW(type), MAX_BUBBLE_W) ?? null;
      const sizeH =
        parseDim(data.size_h ?? null, minH(type), MAX_BUBBLE_H) ?? null;

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

      // Kun forælder der faktisk findes i denne hub tæller
      let parentId = null;
      if (data.parent_id !== undefined && data.parent_id !== null) {
        const pid = parseInt(data.parent_id, 10);
        if (Number.isFinite(pid) && bubbleCache.get(hubId)?.has(pid))
          parentId = pid;
      }

      try {
        // Byg INSERT ud fra de kolonner der faktisk findes
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
        const vals = [
          hubId,
          type,
          x,
          y,
          color,
          encField(content, CTX_CONTENT), // 🔒 krypteret i DB
          encField(emotion, CTX_EMOTION), // 🔒 krypteret i DB
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
        // Cache først, så alt andet (andre brugere, branch_add) kan se boblen
        bubbleCache.get(hubId).set(bubbleId, bubble);
        if (parentId !== null)
          branchCache.get(hubId).add(`${parentId}-${bubbleId}`);

        // Svar opretteren STRAKS (klienten har allerede tegnet boblen)
        if (typeof ack === "function") ack({ ok: true, id: bubbleId, bubble });

        // De andre får boblen + forbindelsen
        socket.to(hubId).emit("bubble_update", bubble);
        if (parentId !== null)
          socket.to(hubId).emit("branch_add", {
            parent_id: parentId,
            child_id: bubbleId,
          });

        // Forbindelsen persisteres bagefter (ikke på den kritiske vej)
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
      const id = parseInt(data.id),
        x = parseInt(data.x),
        y = parseInt(data.y);
      if (isNaN(id) || isNaN(x) || isNaN(y)) return;
      const b = bubbleCache.get(hubId)?.get(id);
      if (!b) return;
      b.x = x;
      b.y = y;
      socket.to(hubId).emit("bubble_moved", { id, x, y });
      if (data.final === false) return; // live-bevægelse: ingen DB-skrivning
      try {
        await pool.execute(
          "UPDATE nh_bubbles SET x=?,y=? WHERE id=? AND hub_id=?",
          [x, y, id, hubId],
        );
      } catch (_) {}
    });

    // bubble_edit — opdaterer kun de felter der faktisk er sendt:
    //   color, size_w, size_h  (alle bobler; size_* = null nulstiller til auto)
    //   emotion, emotion_val, emotion_icon_id  (emotion-bobler)
    //   content  (thought-bobler)
    // Tekstfelter (content / emotion) krypteres før de skrives til DB.
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

      if (data.size_w !== undefined) {
        const w = parseDim(data.size_w, minW(b.type), MAX_BUBBLE_W);
        if (w !== undefined) {
          b.size_w = w;
          if (schema.size_w) {
            sets.push("size_w=?");
            vals.push(w);
          }
        }
      }
      if (data.size_h !== undefined) {
        const h = parseDim(data.size_h, minH(b.type), MAX_BUBBLE_H);
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
          b.emotion = String(data.emotion).substring(0, MAX_EMOTION_CHARS);
          sets.push("emotion=?");
          vals.push(encField(b.emotion, CTX_EMOTION)); // 🔒
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
          if (schema.emotion_icon_id) {
            sets.push("emotion_icon_id=?");
            vals.push(b.emotion_icon_id);
          }
        }
      } else {
        if (data.content !== undefined) {
          b.content = String(data.content).substring(0, MAX_THOUGHT_CHARS);
          sets.push("content=?");
          vals.push(encField(b.content, CTX_CONTENT)); // 🔒
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
    console.log(`[neurohub] v6.11.0 listening on :${PORT}`);
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
  console.error("[neurohub] ⛔ Serveren stoppede: " + (err && err.message));
  console.error("[neurohub] Fatal:", err);
  process.exit(1);
});
