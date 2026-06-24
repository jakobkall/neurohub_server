/**
 * neurohub_server.js
 * Node.js + Socket.IO + MySQL2 realtime server
 *
 * Start:  node neurohub_server.js
 * PM2:    pm2 start neurohub_server.js --name neurohub
 *
 * ENV vars (sæt i .env eller direkte):
 *   DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT
 *   PORT  (default 3001)
 *   ALLOWED_ORIGINS  (komma-separeret liste, fx "https://jakobkall.com")
 */

"use strict";

const http = require("http");
const { Server } = require("socket.io");
const mysql = require("mysql2/promise");
require("dotenv").config();

// ── Config ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT;

const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "https://jakobkall.com")
  .split(",")
  .map((s) => s.trim());

const DB_CONFIG = {
  host: process.env.DB_HOST || "127.0.0.1",
  port: parseInt(process.env.DB_PORT || "3306"),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "",
  database: process.env.DB_NAME || "jakobkall",
  waitForConnections: true,
  connectionLimit: 10,
  charset: "utf8mb4",
};

// ── Pixel buffer (RAM) ─────────────────────────────────────────────────────────
// Structure per hub: Map<hubId, Map<"x,y", {x,y,color,user_id,username}>>
const pixelBuffer = new Map(); // hub_id -> Map<key, pixel>
const FLUSH_INTERVAL = 30_000; // 30 sekunder
const FLUSH_SIZE = 500; // batch-flush ved 500 pixels

// ── Bubble cache (RAM kopi til hurtig broadcast) ───────────────────────────────
// Map<hub_id, Map<id, bubble>>
const bubbleCache = new Map();
const branchCache = new Map(); // Map<hub_id, Set<"parentId-childId">>

// ── Chat history per hub (seneste 50) ─────────────────────────────────────────
const chatCache = new Map(); // Map<hub_id, Array>

// ── Bootstrap ─────────────────────────────────────────────────────────────────
let pool;
async function bootstrap() {
  pool = await mysql.createPool(DB_CONFIG);

  // Opret tabel hvis den mangler
  await pool.execute(`
    CREATE TABLE IF NOT EXISTS neurohub (
      id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
      hub_id      VARCHAR(64)  NOT NULL DEFAULT 'public',
      type        ENUM('pixel','chat','bubble','presence','branch') NOT NULL,
      user_id     BIGINT       NOT NULL,
      username    VARCHAR(64)  NOT NULL,
      x           INT          DEFAULT NULL,
      y           INT          DEFAULT NULL,
      color       VARCHAR(7)   DEFAULT NULL,
      content     TEXT         DEFAULT NULL,
      extra_type  VARCHAR(32)  DEFAULT NULL,
      parent_id   BIGINT UNSIGNED DEFAULT NULL,
      child_id    BIGINT UNSIGNED DEFAULT NULL,
      created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY idx_pixel_pos (hub_id, type, x, y)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);

  console.log("[neurohub] DB connected & table ready");

  // Forvarm RAM med eksisterende data fra DB
  await warmUp();

  // Start flush-timer
  setInterval(() => flushAllPixels(), FLUSH_INTERVAL);

  startServer();
}

// ── Warm-up: Indlæs eksisterende data fra DB i RAM ───────────────────────────
async function warmUp() {
  // Pixels
  const [pixels] = await pool.query(
    "SELECT hub_id, x, y, color, user_id, username FROM neurohub WHERE type='pixel' LIMIT 100000",
  );
  pixels.forEach((p) => {
    if (!pixelBuffer.has(p.hub_id)) pixelBuffer.set(p.hub_id, new Map());
    pixelBuffer.get(p.hub_id).set(`${p.x},${p.y}`, p);
  });
  console.log(`[neurohub] Warmed up ${pixels.length} pixels`);

  // Bubbles
  const [bubbles] = await pool.query(
    "SELECT id, hub_id, x, y, color, content, extra_type, user_id, username FROM neurohub WHERE type='bubble'",
  );
  bubbles.forEach((b) => {
    if (!bubbleCache.has(b.hub_id)) bubbleCache.set(b.hub_id, new Map());
    bubbleCache.get(b.hub_id).set(b.id, {
      id: b.id,
      type: b.extra_type || "brain",
      x: b.x,
      y: b.y,
      color: b.color,
      content: b.content,
      emotion: null,
      emotion_val: null,
      username: b.username,
      user_id: b.user_id,
    });
  });

  // Emotion-felter gemmes i content som JSON for følelse-bobler
  // (ekstra_type = 'emotion', content = JSON-streng)
  bubbleCache.forEach((hMap) => {
    hMap.forEach((b, id) => {
      if (b.type === "emotion" && b.content) {
        try {
          const parsed = JSON.parse(b.content);
          b.emotion = parsed.emotion || null;
          b.emotion_val = parsed.emotion_val || null;
          b.content = null;
        } catch (e) {}
      }
    });
  });

  // Branches
  const [branches] = await pool.query(
    "SELECT hub_id, parent_id, child_id FROM neurohub WHERE type='branch'",
  );
  branches.forEach((br) => {
    if (!branchCache.has(br.hub_id)) branchCache.set(br.hub_id, new Set());
    branchCache.get(br.hub_id).add(`${br.parent_id}-${br.child_id}`);
  });

  // Chat (seneste 50 per hub)
  const [chats] = await pool.query(
    "SELECT hub_id, username, content AS message, UNIX_TIMESTAMP(created_at) AS ts FROM neurohub WHERE type='chat' ORDER BY id DESC LIMIT 50",
  );
  chats.reverse().forEach((c) => {
    if (!chatCache.has(c.hub_id)) chatCache.set(c.hub_id, []);
    chatCache
      .get(c.hub_id)
      .push({ username: c.username, message: c.message, ts: c.ts });
  });

  console.log(
    `[neurohub] Warmed up ${bubbles.length} bubbles, ${branches.length} branches`,
  );
}

// ── Pixel buffer flush ────────────────────────────────────────────────────────
async function flushPixels(hubId) {
  const buf = pixelBuffer.get(hubId);
  if (!buf || buf.size === 0) return;

  // Tag et snapshot og tøm bufferen med det samme (thread-safe nok for Node.js)
  const rows = [...buf.values()];

  try {
    // Bulk INSERT – ON DUPLICATE KEY UPDATE
    const placeholders = rows.map(() => "(?,?,?,?,?,?,?)").join(",");
    const vals = [];
    rows.forEach((p) => {
      vals.push(hubId, "pixel", p.user_id, p.username, p.x, p.y, p.color);
    });

    await pool.execute(
      `INSERT INTO neurohub (hub_id, type, user_id, username, x, y, color)
       VALUES ${placeholders}
       ON DUPLICATE KEY UPDATE color=VALUES(color), user_id=VALUES(user_id), username=VALUES(username)`,
      vals,
    );

    // Håndter sletninger (color = '__erase__' markering)
    const toDelete = rows.filter((p) => p.color === "__erase__");
    if (toDelete.length) {
      for (const p of toDelete) {
        await pool.execute(
          "DELETE FROM neurohub WHERE hub_id=? AND type='pixel' AND x=? AND y=?",
          [hubId, p.x, p.y],
        );
        buf.delete(`${p.x},${p.y}`);
      }
    }

    console.log(`[neurohub] Flushed ${rows.length} pixels for hub=${hubId}`);
  } catch (err) {
    console.error("[neurohub] Pixel flush error:", err.message);
  }
}

async function flushAllPixels() {
  for (const hubId of pixelBuffer.keys()) {
    await flushPixels(hubId);
  }
}

function ensureHubBuffers(hubId) {
  if (!pixelBuffer.has(hubId)) pixelBuffer.set(hubId, new Map());
  if (!bubbleCache.has(hubId)) bubbleCache.set(hubId, new Map());
  if (!branchCache.has(hubId)) branchCache.set(hubId, new Set());
  if (!chatCache.has(hubId)) chatCache.set(hubId, []);
}

// ── Byg initial state til ny klient ───────────────────────────────────────────
function buildState(hubId) {
  const pixels = [];
  const buf = pixelBuffer.get(hubId) || new Map();
  buf.forEach((p, key) => {
    if (p.color !== "__erase__") {
      pixels.push([p.x, p.y, p.color]);
    }
  });

  const bubbles = [];
  (bubbleCache.get(hubId) || new Map()).forEach((b) => bubbles.push(b));

  const branches = [];
  (branchCache.get(hubId) || new Set()).forEach((k) => {
    const [p, c] = k.split("-").map(Number);
    branches.push([p, c]);
  });

  const chat = chatCache.get(hubId) || [];

  return { pixels, bubbles, branches, chat };
}

// ── Online presence (kun RAM) ─────────────────────────────────────────────────
// Map<hub_id, Map<socket.id, {user_id, username, x, y}>>
const presence = new Map();

function getHubPresence(hubId) {
  if (!presence.has(hubId)) presence.set(hubId, new Map());
  return presence.get(hubId);
}

function presenceList(hubId) {
  return [...getHubPresence(hubId).values()];
}

// ── HTTP + Socket.IO server ───────────────────────────────────────────────────
function startServer() {
  const httpServer = http.createServer((req, res) => {
    // Simpel health-check endpoint
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

  // ── Socket middleware: token-auth ───────────────────────────────────────────
  // Klienten sender { user_id, username, hub_id, token } i handshake.auth
  // Token valideres ikke kryptografisk her – PHP-sessionen bør have gjort det.
  // Men du KAN tilføje et HMAC-tjek her senere.
  io.use((socket, next) => {
    const { user_id, username, hub_id } = socket.handshake.auth || {};
    if (!user_id || !username) {
      return next(new Error("auth_required"));
    }
    socket.userId = String(user_id);
    socket.username = String(username).substring(0, 64);
    socket.hubId = String(hub_id || "public").substring(0, 64);
    next();
  });

  // ── Connection ─────────────────────────────────────────────────────────────
  io.on("connection", (socket) => {
    const { userId, username, hubId } = socket;

    console.log(`[+] ${username} (${userId}) → hub:${hubId}`);

    socket.join(hubId);
    ensureHubBuffers(hubId);

    // 1. Send initial state
    socket.emit("state", buildState(hubId));

    // 2. Registrér presence
    getHubPresence(hubId).set(socket.id, {
      user_id: userId,
      username,
      x: 1000,
      y: 700,
    });
    io.to(hubId).emit("presence_list", presenceList(hubId));

    // ── PIXEL ────────────────────────────────────────────────────────────────
    socket.on("pixel", (data) => {
      // data: { x, y, color }  eller { x, y, erase: true }
      const x = parseInt(data.x);
      const y = parseInt(data.y);
      const erase = !!data.erase;
      const color = erase
        ? "__erase__"
        : String(data.color).match(/^#[0-9a-fA-F]{6}$/)
          ? data.color
          : "#ffffff";

      const buf = pixelBuffer.get(hubId);
      if (erase) {
        buf.delete(`${x},${y}`);
      } else {
        buf.set(`${x},${y}`, { x, y, color, user_id: userId, username });
      }

      // Broadcast live til alle i hubben
      io.to(hubId).emit("pixel", { x, y, color: erase ? null : color, erase });

      // Flush ved batch-størrelse
      if (buf.size >= FLUSH_SIZE) {
        flushPixels(hubId).catch(console.error);
      }
    });

    // ── CHAT ─────────────────────────────────────────────────────────────────
    socket.on("chat", async (data) => {
      const message = String(data.message || "")
        .trim()
        .substring(0, 200);
      if (!message) return;

      const msg = { username, message, ts: Math.floor(Date.now() / 1000) };

      // Gem i chat-cache
      const cc = chatCache.get(hubId);
      cc.push(msg);
      if (cc.length > 50) cc.shift();

      // Broadcast
      io.to(hubId).emit("chat", msg);

      // Skriv til DB async (chat er ikke buffered – gemmes straks)
      try {
        await pool.execute(
          "INSERT INTO neurohub (hub_id, type, user_id, username, content) VALUES (?,?,?,?,?)",
          [hubId, "chat", userId, username, message],
        );
      } catch (e) {
        console.error("[neurohub] chat db error:", e.message);
      }
    });

    // ── PRESENCE (bevægelse) – KUN live, gemmes ALDRIG ────────────────────
    socket.on("move", (data) => {
      const x = parseInt(data.x) || 1000;
      const y = parseInt(data.y) || 700;
      const pMap = getHubPresence(hubId);
      const p = pMap.get(socket.id);
      if (p) {
        p.x = x;
        p.y = y;
      }
      // Broadcast kun til andre i hubben (ikke afsender)
      socket.to(hubId).emit("move", { user_id: userId, username, x, y });
    });

    // ── BUBBLE OPRET / OPDATER ────────────────────────────────────────────
    socket.on("bubble_save", async (data, ack) => {
      const isEdit = !!data.id;
      const type = data.type === "emotion" ? "emotion" : "brain";
      const x = parseInt(data.x) || 100;
      const y = parseInt(data.y) || 100;
      const color = String(data.color || "#00ffcc").match(/^#[0-9a-fA-F]{6}$/)
        ? data.color
        : "#00ffcc";
      const content =
        type === "brain"
          ? String(data.content || "").substring(0, 500)
          : JSON.stringify({
              emotion: String(data.emotion || "").substring(0, 64),
              emotion_val: Math.min(
                10,
                Math.max(1, parseInt(data.emotion_val) || 5),
              ),
            });

      try {
        let bubbleId;
        if (isEdit) {
          bubbleId = parseInt(data.id);
          await pool.execute(
            "UPDATE neurohub SET x=?, y=?, color=?, content=? WHERE id=? AND type='bubble'",
            [x, y, color, content, bubbleId],
          );
        } else {
          const [res] = await pool.execute(
            "INSERT INTO neurohub (hub_id, type, user_id, username, x, y, color, content, extra_type) VALUES (?,?,?,?,?,?,?,?,?)",
            [hubId, "bubble", userId, username, x, y, color, content, type],
          );
          bubbleId = res.insertId;

          // Branch
          if (data.parent_id) {
            const parentId = parseInt(data.parent_id);
            await pool.execute(
              "INSERT IGNORE INTO neurohub (hub_id, type, user_id, username, parent_id, child_id) VALUES (?,?,?,?,?,?)",
              [hubId, "branch", userId, username, parentId, bubbleId],
            );
            if (!branchCache.has(hubId)) branchCache.set(hubId, new Set());
            branchCache.get(hubId).add(`${parentId}-${bubbleId}`);
            io.to(hubId).emit("branch_add", {
              parent_id: parentId,
              child_id: bubbleId,
            });
          }
        }

        // Opdater cache
        const bMap = bubbleCache.get(hubId);
        const parsedContent = type === "emotion" ? null : content;
        let emotion = null,
          emotion_val = null;
        if (type === "emotion") {
          try {
            const j = JSON.parse(content);
            emotion = j.emotion;
            emotion_val = j.emotion_val;
          } catch (e) {}
        }
        const bubble = {
          id: bubbleId,
          type,
          x,
          y,
          color,
          content: parsedContent,
          emotion,
          emotion_val,
          username,
          user_id: userId,
        };
        bMap.set(bubbleId, bubble);

        // Broadcast
        io.to(hubId).emit("bubble_update", bubble);

        if (typeof ack === "function") ack({ ok: true, id: bubbleId });
      } catch (e) {
        console.error("[neurohub] bubble_save error:", e.message);
        if (typeof ack === "function") ack({ ok: false });
      }
    });

    // ── BUBBLE FLYT ──────────────────────────────────────────────────────────
    socket.on("bubble_move", async (data) => {
      const id = parseInt(data.id);
      const x = parseInt(data.x);
      const y = parseInt(data.y);
      const bMap = bubbleCache.get(hubId);
      const b = bMap?.get(id);
      if (b) {
        b.x = x;
        b.y = y;
      }
      socket.to(hubId).emit("bubble_moved", { id, x, y });
      try {
        await pool.execute(
          "UPDATE neurohub SET x=?, y=? WHERE id=? AND type='bubble'",
          [x, y, id],
        );
      } catch (e) {
        console.error("[neurohub] bubble_move error:", e.message);
      }
    });

    // ── BUBBLE SLET ───────────────────────────────────────────────────────────
    socket.on("bubble_delete", async (data) => {
      const id = parseInt(data.id);
      bubbleCache.get(hubId)?.delete(id);
      branchCache.get(hubId)?.forEach((k) => {
        const [p, c] = k.split("-").map(Number);
        if (p === id || c === id) branchCache.get(hubId).delete(k);
      });
      io.to(hubId).emit("bubble_delete", { id });
      try {
        await pool.execute(
          "DELETE FROM neurohub WHERE id=? AND type='bubble'",
          [id],
        );
        await pool.execute(
          "DELETE FROM neurohub WHERE type='branch' AND (parent_id=? OR child_id=?)",
          [id, id],
        );
      } catch (e) {
        console.error("[neurohub] bubble_delete error:", e.message);
      }
    });

    // ── BUBBLE EDIT (inline tekst) ────────────────────────────────────────────
    socket.on("bubble_edit", async (data) => {
      const id = parseInt(data.id);
      const bMap = bubbleCache.get(hubId);
      const b = bMap?.get(id);
      if (!b) return;

      let content;
      if (b.type === "emotion") {
        b.emotion = String(data.emotion || b.emotion || "").substring(0, 64);
        b.emotion_val = parseInt(data.emotion_val ?? b.emotion_val ?? 5);
        content = JSON.stringify({
          emotion: b.emotion,
          emotion_val: b.emotion_val,
        });
      } else {
        b.content = String(data.content || "").substring(0, 500);
        content = b.content;
      }
      io.to(hubId).emit("bubble_update", b);
      try {
        await pool.execute(
          "UPDATE neurohub SET content=? WHERE id=? AND type='bubble'",
          [content, id],
        );
      } catch (e) {
        console.error("[neurohub] bubble_edit error:", e.message);
      }
    });

    // ── DISCONNECT ────────────────────────────────────────────────────────────
    socket.on("disconnect", () => {
      console.log(`[-] ${username} (${userId}) ← hub:${hubId}`);
      getHubPresence(hubId).delete(socket.id);
      io.to(hubId).emit("presence_list", presenceList(hubId));
    });
  });

  httpServer.listen(PORT, () => {
    console.log(`[neurohub] Server kører på port ${PORT}`);
    console.log(`[neurohub] CORS tillader: ${ALLOWED_ORIGINS.join(", ")}`);
  });
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────
process.on("SIGTERM", async () => {
  console.log("[neurohub] SIGTERM – flusher pixels og lukker…");
  await flushAllPixels();
  process.exit(0);
});
process.on("SIGINT", async () => {
  console.log("[neurohub] SIGINT – flusher pixels og lukker…");
  await flushAllPixels();
  process.exit(0);
});

// ── Start ─────────────────────────────────────────────────────────────────────
bootstrap().catch((err) => {
  console.error("[neurohub] Fatal bootstrap error:", err);
  process.exit(1);
});
