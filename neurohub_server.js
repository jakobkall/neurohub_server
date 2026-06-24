/**
 * neurohub_server.js  — v2
 * Node.js + Socket.IO + MySQL2 realtime server
 *
 * Start:  node neurohub_server.js
 * PM2:    pm2 start neurohub_server.js --name neurohub
 *
 * ENV vars (.env eller Railway):
 *   DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT
 *   PORT            (default 3001)
 *   ALLOWED_ORIGINS (komma-separeret, fx "https://jakobkall.com")
 */

"use strict";

const http = require("http");
const { Server } = require("socket.io");
const mysql = require("mysql2/promise");
require("dotenv").config();

// ── Config ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;

const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "https://jakobkall.com")
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

// ── Pixel buffer ──────────────────────────────────────────────────────────────
// Pixels are held in RAM and written to DB in batches.
// NEVER written one-by-one on each socket event.
//
//   pixelBuffer  Map<hubId, Map<"x,y", {x,y,color,user_id,username}>>
//   erasedKeys   Map<hubId, Set<"x,y">>   — positions deleted since last flush
//
const pixelBuffer = new Map();
const erasedKeys = new Map();

const FLUSH_INTERVAL = 30_000; // 30 seconds
const FLUSH_BATCH = 500; // also flush when this many pixels are dirty

// ── Other RAM caches ──────────────────────────────────────────────────────────
const bubbleCache = new Map(); // Map<hubId, Map<id, bubble>>
const branchCache = new Map(); // Map<hubId, Set<"parentId-childId">>
const chatCache = new Map(); // Map<hubId, Array<msg>>  (last 50)
const presence = new Map(); // Map<hubId, Map<socketId, {user_id,username,x,y}>>

// ── Bootstrap ─────────────────────────────────────────────────────────────────
let pool;
async function bootstrap() {
  pool = await mysql.createPool(DB_CONFIG);
  console.log("[neurohub] DB connected");

  await warmUp();
  setInterval(flushAllPixels, FLUSH_INTERVAL);
  startServer();
}

// ── Warm-up: load existing data from DB into RAM ──────────────────────────────
async function warmUp() {
  // Pixels
  const [pixels] = await pool.query(
    "SELECT hub_id, x, y, color, user_id, username FROM nh_pixels LIMIT 200000",
  );
  pixels.forEach((p) => {
    ensureHubBuffers(p.hub_id);
    pixelBuffer.get(p.hub_id).set(`${p.x},${p.y}`, p);
  });
  console.log(`[neurohub] Warmed ${pixels.length} pixels`);

  // Bubbles
  const [bubbles] = await pool.query(
    "SELECT id, hub_id, type, x, y, color, content, emotion, emotion_val, user_id, username FROM nh_bubbles",
  );
  bubbles.forEach((b) => {
    ensureHubBuffers(b.hub_id);
    bubbleCache.get(b.hub_id).set(b.id, {
      id: b.id,
      type: b.type,
      x: b.x,
      y: b.y,
      color: b.color,
      content: b.content,
      emotion: b.emotion,
      emotion_val: b.emotion_val,
      username: b.username,
      user_id: b.user_id,
    });
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
    `SELECT hub_id, username, message, UNIX_TIMESTAMP(created_at) AS ts
     FROM nh_chat ORDER BY id DESC LIMIT 200`,
  );
  chats.reverse().forEach((c) => {
    ensureHubBuffers(c.hub_id);
    chatCache
      .get(c.hub_id)
      .push({ username: c.username, message: c.message, ts: c.ts });
  });

  console.log(
    `[neurohub] Warmed ${bubbles.length} bubbles, ${branches.length} branches`,
  );
}

function ensureHubBuffers(hubId) {
  if (!pixelBuffer.has(hubId)) pixelBuffer.set(hubId, new Map());
  if (!erasedKeys.has(hubId)) erasedKeys.set(hubId, new Set());
  if (!bubbleCache.has(hubId)) bubbleCache.set(hubId, new Map());
  if (!branchCache.has(hubId)) branchCache.set(hubId, new Set());
  if (!chatCache.has(hubId)) chatCache.set(hubId, []);
  if (!presence.has(hubId)) presence.set(hubId, new Map());
}

// ── Pixel flush (RAM → DB) ────────────────────────────────────────────────────
async function flushPixels(hubId) {
  const buf = pixelBuffer.get(hubId);
  const erased = erasedKeys.get(hubId);
  if ((!buf || buf.size === 0) && (!erased || erased.size === 0)) return;

  // --- Upserts ---
  if (buf && buf.size > 0) {
    const rows = [...buf.values()];
    const placeholders = rows.map(() => "(?,?,?,?,?,?)").join(",");
    const vals = [];
    rows.forEach((p) =>
      vals.push(hubId, p.x, p.y, p.color, p.user_id, p.username),
    );
    try {
      await pool.execute(
        `INSERT INTO nh_pixels (hub_id, x, y, color, user_id, username)
         VALUES ${placeholders}
         ON DUPLICATE KEY UPDATE
           color=VALUES(color),
           user_id=VALUES(user_id),
           username=VALUES(username)`,
        vals,
      );
      console.log(`[neurohub] Flushed ${rows.length} pixels for hub=${hubId}`);
    } catch (err) {
      console.error("[neurohub] Pixel upsert error:", err.message);
    }
  }

  // --- Deletes (erased pixels) ---
  if (erased && erased.size > 0) {
    for (const key of erased) {
      const [x, y] = key.split(",").map(Number);
      try {
        await pool.execute(
          "DELETE FROM nh_pixels WHERE hub_id=? AND x=? AND y=?",
          [hubId, x, y],
        );
      } catch (err) {
        console.error("[neurohub] Pixel delete error:", err.message);
      }
    }
    erased.clear();
  }
}

async function flushAllPixels() {
  for (const hubId of pixelBuffer.keys()) {
    await flushPixels(hubId);
  }
}

// ── Build initial state payload for a new client ──────────────────────────────
function buildState(hubId) {
  const pixels = [];
  (pixelBuffer.get(hubId) || new Map()).forEach((p) => {
    pixels.push([p.x, p.y, p.color]);
  });

  const bubbles = [...(bubbleCache.get(hubId) || new Map()).values()];
  const branches = [];
  (branchCache.get(hubId) || new Set()).forEach((k) => {
    const [p, c] = k.split("-").map(Number);
    branches.push([p, c]);
  });
  const chat = chatCache.get(hubId) || [];

  return { pixels, bubbles, branches, chat };
}

// ── Presence helpers ──────────────────────────────────────────────────────────
function getPresence(hubId) {
  return presence.get(hubId) || new Map();
}
function presenceList(hubId) {
  return [...getPresence(hubId).values()];
}

// ── HTTP + Socket.IO ──────────────────────────────────────────────────────────
function startServer() {
  const httpServer = http.createServer((req, res) => {
    if (req.url === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, ts: Date.now() }));
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

  // ── Auth middleware ──────────────────────────────────────────────────────────
  io.use((socket, next) => {
    const { user_id, username, hub_id } = socket.handshake.auth || {};
    if (!user_id || !username) return next(new Error("auth_required"));
    socket.userId = String(user_id);
    socket.username = String(username).substring(0, 64);
    socket.hubId = String(hub_id || "public").substring(0, 64);
    next();
  });

  // ── Connection ───────────────────────────────────────────────────────────────
  io.on("connection", (socket) => {
    const { userId, username, hubId } = socket;
    console.log(`[+] ${username} (${userId}) → hub:${hubId}`);

    socket.join(hubId);
    ensureHubBuffers(hubId);

    // Send state to new client
    socket.emit("state", buildState(hubId));

    // Register presence
    getPresence(hubId).set(socket.id, {
      user_id: userId,
      username,
      x: 1000,
      y: 700,
    });
    io.to(hubId).emit("presence_list", presenceList(hubId));

    // ── PIXEL ────────────────────────────────────────────────────────────────
    // Pixels go into RAM only. DB write happens in the periodic flush.
    socket.on("pixel", (data) => {
      const x = parseInt(data.x);
      const y = parseInt(data.y);
      const erase = !!data.erase;
      const color = erase
        ? null
        : String(data.color).match(/^#[0-9a-fA-F]{6}$/)
          ? data.color
          : "#ffffff";

      const buf = pixelBuffer.get(hubId);
      const erased = erasedKeys.get(hubId);
      const key = `${x},${y}`;

      if (erase) {
        buf.delete(key);
        erased.add(key); // mark for DELETE on next flush
      } else {
        erased.delete(key); // no longer needs deleting
        buf.set(key, { x, y, color, user_id: userId, username });
      }

      // Broadcast immediately (live update for all clients)
      io.to(hubId).emit("pixel", { x, y, color: erase ? null : color, erase });

      // Trigger early flush if buffer is large enough
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
        // Trim old chat rows (keep last 200 per hub to avoid unbounded growth)
        await pool.execute(
          `DELETE FROM nh_chat WHERE hub_id=? AND id NOT IN (
             SELECT id FROM (SELECT id FROM nh_chat WHERE hub_id=? ORDER BY id DESC LIMIT 200) t
           )`,
          [hubId, hubId],
        );
      } catch (e) {
        console.error("[neurohub] chat db error:", e.message);
      }
    });

    // ── PRESENCE / MOVE ───────────────────────────────────────────────────────
    socket.on("move", (data) => {
      const x = parseInt(data.x) || 1000;
      const y = parseInt(data.y) || 700;
      const p = getPresence(hubId).get(socket.id);
      if (p) {
        p.x = x;
        p.y = y;
      }
      socket.to(hubId).emit("move", { user_id: userId, username, x, y });
    });

    // ── BUBBLE SAVE (create or update) ───────────────────────────────────────
    socket.on("bubble_save", async (data, ack) => {
      const isEdit = !!data.id;
      const type = data.type === "emotion" ? "emotion" : "brain";
      const x = parseInt(data.x) || 100;
      const y = parseInt(data.y) || 100;
      const color = String(data.color || "#00ffcc").match(/^#[0-9a-fA-F]{6}$/)
        ? data.color
        : "#00ffcc";
      const content =
        type === "brain" ? String(data.content || "").substring(0, 500) : null;
      const emotion =
        type === "emotion" ? String(data.emotion || "").substring(0, 64) : null;
      const emotionVal =
        type === "emotion"
          ? Math.min(10, Math.max(1, parseInt(data.emotion_val) || 5))
          : null;
      try {
        let bubbleId;
        if (isEdit) {
          bubbleId = parseInt(data.id);
          await pool.execute(
            `UPDATE nh_bubbles
             SET x=?, y=?, color=?, content=?, emotion=?, emotion_val=?
             WHERE id=?`,
            [x, y, color, content, emotion, emotionVal, bubbleId],
          );
        } else {
          const [res] = await pool.execute(
            `INSERT INTO nh_bubbles
               (hub_id, type, x, y, color, content, emotion, emotion_val, user_id, username)
             VALUES (?,?,?,?,?,?,?,?,?,?)`,
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
          bubbleId = res.insertId;

          // Branch (optional parent link)
          if (data.parent_id) {
            const parentId = parseInt(data.parent_id);
            await pool.execute(
              `INSERT IGNORE INTO nh_branches (hub_id, parent_id, child_id)
               VALUES (?,?,?)`,
              [hubId, parentId, bubbleId],
            );
            branchCache.get(hubId).add(`${parentId}-${bubbleId}`);
            io.to(hubId).emit("branch_add", {
              parent_id: parentId,
              child_id: bubbleId,
            });
          }
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

    // ── BUBBLE MOVE ──────────────────────────────────────────────────────────
    socket.on("bubble_move", async (data) => {
      const id = parseInt(data.id);
      const x = parseInt(data.x);
      const y = parseInt(data.y);
      const b = bubbleCache.get(hubId)?.get(id);
      if (b) {
        b.x = x;
        b.y = y;
      }
      socket.to(hubId).emit("bubble_moved", { id, x, y });
      try {
        await pool.execute("UPDATE nh_bubbles SET x=?, y=? WHERE id=?", [
          x,
          y,
          id,
        ]);
      } catch (e) {
        console.error("[neurohub] bubble_move error:", e.message);
      }
    });

    // ── BUBBLE EDIT (inline text change) ─────────────────────────────────────
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
            "UPDATE nh_bubbles SET emotion=?, emotion_val=? WHERE id=?",
            [b.emotion, b.emotion_val, id],
          );
        } catch (e) {
          console.error("[neurohub] bubble_edit emotion error:", e.message);
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
          console.error("[neurohub] bubble_edit brain error:", e.message);
        }
      }

      io.to(hubId).emit("bubble_update", b);
    });

    // ── BUBBLE DELETE ─────────────────────────────────────────────────────────
    socket.on("bubble_delete", async (data) => {
      const id = parseInt(data.id);
      bubbleCache.get(hubId)?.delete(id);
      branchCache.get(hubId)?.forEach((k) => {
        const [p, c] = k.split("-").map(Number);
        if (p === id || c === id) branchCache.get(hubId).delete(k);
      });
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
      console.log(`[-] ${username} (${userId}) ← hub:${hubId}`);
      getPresence(hubId).delete(socket.id);
      io.to(hubId).emit("presence_list", presenceList(hubId));
    });
  });

  httpServer.listen(PORT, () => {
    console.log(`[neurohub] Listening on port ${PORT}`);
    console.log(`[neurohub] CORS: ${ALLOWED_ORIGINS.join(", ")}`);
  });
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────
async function shutdown() {
  console.log("[neurohub] Shutting down — flushing pixels…");
  await flushAllPixels();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

// ── Start ─────────────────────────────────────────────────────────────────────
bootstrap().catch((err) => {
  console.error("[neurohub] Fatal bootstrap error:", err);
  process.exit(1);
});
