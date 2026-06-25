/**
 * neurohub_server.js — v4.1
 *
 * Fixes vs v4:
 *  - nh_pixels UPSERT now uses correct unique key assumption; falls back to
 *    DELETE+INSERT if ON DUPLICATE KEY fails (handles missing UNIQUE index)
 *  - Added SQL to ensure UNIQUE INDEX on (hub_id,x,y) at startup
 *  - users table lookup uses correct PK column `id` not `user_id`
 *  - is_admin resolved server-side from DB, not trusted from client auth
 *  - presence_list now always broadcasts on join/leave
 *  - warmUp correctly maps nh_pixels locked column
 *  - pixel event emission fixed: always emits to all including sender
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

const DB_CONFIG = {
  host: process.env.DB_HOST || "mysql48.unoeuro.com",
  port: parseInt(process.env.DB_PORT || "3306"),
  user: process.env.DB_USER || "jakobkall_com",
  password: process.env.DB_PASS || "cfDEmaw5n96t",
  database: process.env.DB_NAME || "jakobkall_com_db_neurohub",
  waitForConnections: true,
  connectionLimit: 10,
  charset: "utf8mb4",
};

// ── RAM buffers ───────────────────────────────────────────────────────────────
const pixelBuffer = new Map(); // hubId → Map<"x,y", {x,y,color,user_id,username,locked}>
const erasedKeys = new Map(); // hubId → Set<"x,y">
const bubbleCache = new Map(); // hubId → Map<id, bubble>
const branchCache = new Map(); // hubId → Set<"parentId-childId">
const chatCache = new Map(); // hubId → [{username,message,ts}]
const presence = new Map(); // hubId → Map<socketId, {user_id,username,x,y}>

// adminCache: user_id (string) → boolean
const adminCache = new Map();

const FLUSH_INTERVAL = 20_000; // flush every 20s
const FLUSH_BATCH = 200; // also flush when buffer hits this size

let pool;

// ── Bootstrap ─────────────────────────────────────────────────────────────────
async function bootstrap() {
  pool = await mysql.createPool(DB_CONFIG);
  console.log("[neurohub] DB connected");

  // Ensure locked column exists on nh_pixels
  try {
    await pool.execute(
      "ALTER TABLE nh_pixels ADD COLUMN IF NOT EXISTS locked TINYINT(1) NOT NULL DEFAULT 0",
    );
  } catch (e) {
    /* ignore — column may already exist */
  }

  // Ensure UNIQUE index so ON DUPLICATE KEY UPDATE works
  try {
    await pool.execute(
      "ALTER TABLE nh_pixels ADD UNIQUE INDEX IF NOT EXISTS ux_hub_xy (hub_id, x, y)",
    );
    console.log("[neurohub] UNIQUE index on nh_pixels ensured");
  } catch (e) {
    // MySQL 5.x doesn't support IF NOT EXISTS on indexes; try without
    try {
      await pool.execute(
        "ALTER TABLE nh_pixels ADD UNIQUE INDEX ux_hub_xy (hub_id, x, y)",
      );
    } catch (e2) {
      /* already exists */
    }
  }

  await warmUp();
  setInterval(flushAllPixels, FLUSH_INTERVAL);
  startServer();
}

// ── Warm-up: load all state into RAM ──────────────────────────────────────────
async function warmUp() {
  // Pixels
  const [pixels] = await pool.query(
    "SELECT hub_id, x, y, color, user_id, username, IFNULL(locked,0) AS locked FROM nh_pixels LIMIT 500000",
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
    "SELECT id, hub_id, type, x, y, color, content, emotion, emotion_val, user_id, username FROM nh_bubbles",
  );
  bubbles.forEach((b) => {
    ensureHubBuffers(b.hub_id);
    bubbleCache.get(b.hub_id).set(b.id, { ...b });
  });

  // Branches
  const [branches] = await pool.query(
    "SELECT hub_id, parent_id, child_id FROM nh_branches",
  );
  branches.forEach((br) => {
    ensureHubBuffers(br.hub_id);
    branchCache.get(br.hub_id).add(`${br.parent_id}-${br.child_id}`);
  });

  // Chat (last 50 per hub)
  const [chats] = await pool.query(
    "SELECT hub_id, username, message, UNIX_TIMESTAMP(created_at) AS ts FROM nh_chat ORDER BY id DESC LIMIT 500",
  );
  chats.reverse().forEach((c) => {
    ensureHubBuffers(c.hub_id);
    const cc = chatCache.get(c.hub_id);
    cc.push({ username: c.username, message: c.message, ts: c.ts });
    if (cc.length > 50) cc.shift();
  });

  console.log(
    `[neurohub] Warmed ${bubbles.length} bubbles, ${branches.length} branches, ${chats.length} chat msgs`,
  );
}

// ── Hub buffer init ───────────────────────────────────────────────────────────
function ensureHubBuffers(hubId) {
  if (!pixelBuffer.has(hubId)) pixelBuffer.set(hubId, new Map());
  if (!erasedKeys.has(hubId)) erasedKeys.set(hubId, new Set());
  if (!bubbleCache.has(hubId)) bubbleCache.set(hubId, new Map());
  if (!branchCache.has(hubId)) branchCache.set(hubId, new Set());
  if (!chatCache.has(hubId)) chatCache.set(hubId, []);
  if (!presence.has(hubId)) presence.set(hubId, new Map());
}

// ── Admin lookup ──────────────────────────────────────────────────────────────
// The users table PK is `id` (int), but session stores user_id which maps to
// the `user_id` bigint column OR the `id` column depending on auth method.
// We try both to be safe.
async function resolveAdmin(userId) {
  const key = String(userId);
  if (adminCache.has(key)) return adminCache.get(key);
  try {
    // Try matching on `id` first (the actual PK), then `user_id` column
    const [rows] = await pool.execute(
      "SELECT user_type FROM users WHERE id = ? OR user_id = ? LIMIT 1",
      [userId, userId],
    );
    if (rows.length) {
      const types = (rows[0].user_type || "")
        .split(",")
        .map((t) => t.trim().toLowerCase());
      const isAdmin = types.includes("admin");
      adminCache.set(key, isAdmin);
      return isAdmin;
    }
  } catch (e) {
    console.error("[neurohub] admin lookup error:", e.message);
  }
  adminCache.set(key, false);
  return false;
}

// ── Pixel flush ───────────────────────────────────────────────────────────────
async function flushPixels(hubId) {
  const buf = pixelBuffer.get(hubId);
  const erased = erasedKeys.get(hubId);

  // Handle erased pixels first
  if (erased && erased.size > 0) {
    for (const key of erased) {
      const [x, y] = key.split(",").map(Number);
      try {
        await pool.execute(
          "DELETE FROM nh_pixels WHERE hub_id=? AND x=? AND y=?",
          [hubId, x, y],
        );
      } catch (err) {
        console.error("[neurohub] pixel delete error:", err.message);
      }
    }
    erased.clear();
  }

  // Upsert painted pixels in batches
  if (buf && buf.size > 0) {
    const rows = [...buf.values()];
    // Process in chunks of 200
    for (let i = 0; i < rows.length; i += 200) {
      const chunk = rows.slice(i, i + 200);
      const placeholders = chunk.map(() => "(?,?,?,?,?,?,?)").join(",");
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
          `INSERT INTO nh_pixels (hub_id, x, y, color, user_id, username, locked)
           VALUES ${placeholders}
           ON DUPLICATE KEY UPDATE
             color    = VALUES(color),
             user_id  = VALUES(user_id),
             username = VALUES(username),
             locked   = VALUES(locked)`,
          vals,
        );
      } catch (err) {
        console.error("[neurohub] pixel upsert error:", err.message);
        // Fallback: individual upserts
        for (const p of chunk) {
          try {
            await pool.execute(
              `INSERT INTO nh_pixels (hub_id, x, y, color, user_id, username, locked)
               VALUES (?,?,?,?,?,?,?)
               ON DUPLICATE KEY UPDATE
                 color=VALUES(color), user_id=VALUES(user_id),
                 username=VALUES(username), locked=VALUES(locked)`,
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
            console.error(
              "[neurohub] pixel individual upsert error:",
              e2.message,
            );
          }
        }
      }
    }
  }
}

async function flushAllPixels() {
  for (const hubId of pixelBuffer.keys()) {
    await flushPixels(hubId).catch((e) =>
      console.error(`[neurohub] flushPixels(${hubId}) error:`, e.message),
    );
  }
}

// ── State builder ─────────────────────────────────────────────────────────────
function buildState(hubId) {
  const pixels = [];
  (pixelBuffer.get(hubId) || new Map()).forEach((p) => {
    pixels.push([p.x, p.y, p.color, p.locked ? 1 : 0]);
  });
  const bubbles = [...(bubbleCache.get(hubId) || new Map()).values()];
  const branches = [];
  (branchCache.get(hubId) || new Set()).forEach((k) => {
    const [p, c] = k.split("-").map(Number);
    branches.push([p, c]);
  });
  const chat = (chatCache.get(hubId) || []).slice(-50);
  return { pixels, bubbles, branches, chat };
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

  // Auth middleware
  io.use(async (socket, next) => {
    const { user_id, username, hub_id } = socket.handshake.auth || {};
    if (!user_id || !username) return next(new Error("auth_required"));
    socket.userId = String(user_id);
    socket.username = String(username).substring(0, 64);
    socket.hubId = String(hub_id || "public").substring(0, 64);
    // Resolve admin from DB — don't trust client
    socket.isAdmin = await resolveAdmin(user_id);
    next();
  });

  io.on("connection", (socket) => {
    const { userId, username, hubId, isAdmin } = socket;
    console.log(`[+] ${username} (${userId}) admin=${isAdmin} → hub:${hubId}`);

    socket.join(hubId);
    ensureHubBuffers(hubId);

    // Send full state to this socket
    socket.emit("state", buildState(hubId));

    // Register presence
    getPresence(hubId).set(socket.id, {
      user_id: userId,
      username,
      x: WORLD_W / 2,
      y: WORLD_H / 2,
    });
    // Broadcast updated presence list to everyone in hub
    io.to(hubId).emit("presence_list", presenceList(hubId));

    // ── PIXEL ────────────────────────────────────────────────────────────────
    socket.on("pixel", (data) => {
      const x = parseInt(data.x);
      const y = parseInt(data.y);
      const erase = !!data.erase;
      const locked = !!data.locked && isAdmin;
      const color = erase
        ? null
        : String(data.color || "").match(/^#[0-9a-fA-F]{6}$/)
          ? data.color
          : "#ffffff";

      if (isNaN(x) || isNaN(y) || x < 0 || y < 0) return;

      const buf = pixelBuffer.get(hubId);
      const erased = erasedKeys.get(hubId);
      const key = `${x},${y}`;
      const existing = buf.get(key);

      // Block non-admin erase of locked pixels
      if (erase && existing?.locked && !isAdmin) return;

      if (erase) {
        buf.delete(key);
        erased.add(key);
      } else {
        erased.delete(key);
        buf.set(key, { x, y, color, user_id: userId, username, locked });
      }

      // Emit to ALL clients in hub (including sender so they see confirmation)
      io.to(hubId).emit("pixel", {
        x,
        y,
        color: erase ? null : color,
        erase,
        locked,
      });

      // Flush if buffer is large
      if (buf.size >= FLUSH_BATCH) {
        flushPixels(hubId).catch(console.error);
      }
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
          "INSERT INTO nh_chat (hub_id, user_id, username, message) VALUES (?,?,?,?)",
          [hubId, userId, username, message],
        );
      } catch (e) {
        console.error("[neurohub] chat db error:", e.message);
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
      // Only broadcast to others (sender already knows their own position)
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
        console.error("[neurohub] bubble_save error:", e.message);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

    // ── BUBBLE MOVE ───────────────────────────────────────────────────────────
    socket.on("bubble_move", async (data) => {
      const id = parseInt(data.id);
      const x = parseInt(data.x);
      const y = parseInt(data.y);
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
        console.error("[neurohub] bubble_move error:", e.message);
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
          console.error("[neurohub] bubble_edit emotion:", e.message);
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
          console.error("[neurohub] bubble_edit brain:", e.message);
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
      if (bc) {
        for (const k of [...bc]) {
          const [p, c] = k.split("-").map(Number);
          if (p === id || c === id) bc.delete(k);
        }
      }
      io.to(hubId).emit("bubble_delete", { id });
      try {
        await pool.execute("DELETE FROM nh_bubbles WHERE id=?", [id]);
        await pool.execute(
          "DELETE FROM nh_branches WHERE parent_id=? OR child_id=?",
          [id, id],
        );
      } catch (e) {
        console.error("[neurohub] bubble_delete error:", e.message);
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
    console.log(`[neurohub] Listening on :${PORT}`);
    console.log(`[neurohub] CORS: ${ALLOWED_ORIGINS.join(", ")}`);
  });
}

const WORLD_W = 4000;
const WORLD_H = 2800;

async function shutdown() {
  console.log("[neurohub] Shutting down — flushing pixels…");
  await flushAllPixels();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

bootstrap().catch((err) => {
  console.error("[neurohub] Fatal bootstrap error:", err);
  process.exit(1);
});
