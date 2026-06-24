/**
 * neurohub_server.js — v4
 * Node.js + Socket.IO + MySQL2
 *
 * Nyheder vs v3:
 *  - nh_pixels.locked TINYINT — kun admins kan slette låste pixels
 *  - pixel-event sender { locked } felt
 *  - state sender [x, y, color, locked] tuples
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
// pixelBuffer: Map<hubId, Map<"x,y", {x,y,color,user_id,username,locked}>>
// erasedKeys:  Map<hubId, Set<"x,y">>
const pixelBuffer = new Map();
const erasedKeys = new Map();
const bubbleCache = new Map();
const branchCache = new Map();
const chatCache = new Map();
const presence = new Map();

const FLUSH_INTERVAL = 30_000;
const FLUSH_BATCH = 500;

let pool;
async function bootstrap() {
  pool = await mysql.createPool(DB_CONFIG);
  console.log("[neurohub] DB connected");

  // Ensure locked column exists
  try {
    await pool.execute(
      "ALTER TABLE nh_pixels ADD COLUMN IF NOT EXISTS locked TINYINT(1) NOT NULL DEFAULT 0",
    );
  } catch (e) {
    /* column may already exist on some MySQL versions */
  }

  await warmUp();
  setInterval(flushAllPixels, FLUSH_INTERVAL);
  startServer();
}

async function warmUp() {
  const [pixels] = await pool.query(
    "SELECT hub_id, x, y, color, user_id, username, locked FROM nh_pixels LIMIT 500000",
  );
  pixels.forEach((p) => {
    ensureHubBuffers(p.hub_id);
    pixelBuffer.get(p.hub_id).set(`${p.x},${p.y}`, p);
  });
  console.log(`[neurohub] Warmed ${pixels.length} pixels`);

  const [bubbles] = await pool.query(
    "SELECT id, hub_id, type, x, y, color, content, emotion, emotion_val, user_id, username FROM nh_bubbles",
  );
  bubbles.forEach((b) => {
    ensureHubBuffers(b.hub_id);
    bubbleCache.get(b.hub_id).set(b.id, { ...b });
  });

  const [branches] = await pool.query(
    "SELECT hub_id, parent_id, child_id FROM nh_branches",
  );
  branches.forEach((br) => {
    ensureHubBuffers(br.hub_id);
    branchCache.get(br.hub_id).add(`${br.parent_id}-${br.child_id}`);
  });

  const [chats] = await pool.query(
    "SELECT hub_id, username, message, UNIX_TIMESTAMP(created_at) AS ts FROM nh_chat ORDER BY id DESC LIMIT 200",
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

// ── Pixel flush ───────────────────────────────────────────────────────────────
async function flushPixels(hubId) {
  const buf = pixelBuffer.get(hubId);
  const erased = erasedKeys.get(hubId);
  if ((!buf || buf.size === 0) && (!erased || erased.size === 0)) return;

  if (buf && buf.size > 0) {
    const rows = [...buf.values()];
    const placeholders = rows.map(() => "(?,?,?,?,?,?,?)").join(",");
    const vals = [];
    rows.forEach((p) =>
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
           color=VALUES(color), user_id=VALUES(user_id),
           username=VALUES(username), locked=VALUES(locked)`,
        vals,
      );
    } catch (err) {
      console.error("[neurohub] Pixel upsert error:", err.message);
    }
  }

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
  for (const hubId of pixelBuffer.keys()) await flushPixels(hubId);
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
  const chat = chatCache.get(hubId) || [];
  return { pixels, bubbles, branches, chat };
}

function getPresence(hubId) {
  return presence.get(hubId) || new Map();
}
function presenceList(hubId) {
  return [...getPresence(hubId).values()];
}

// ── Server ────────────────────────────────────────────────────────────────────
function startServer() {
  const httpServer = http.createServer((req, res) => {
    if (req.url === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
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

  io.use((socket, next) => {
    const { user_id, username, hub_id, is_admin } = socket.handshake.auth || {};
    if (!user_id || !username) return next(new Error("auth_required"));
    socket.userId = String(user_id);
    socket.username = String(username).substring(0, 64);
    socket.hubId = String(hub_id || "public").substring(0, 64);
    socket.isAdmin = !!is_admin;
    next();
  });

  io.on("connection", (socket) => {
    const { userId, username, hubId, isAdmin } = socket;
    console.log(`[+] ${username} (${userId}) admin=${isAdmin} → hub:${hubId}`);

    socket.join(hubId);
    ensureHubBuffers(hubId);
    socket.emit("state", buildState(hubId));

    getPresence(hubId).set(socket.id, {
      user_id: userId,
      username,
      x: WORLD_W / 2,
      y: WORLD_H / 2,
    });
    io.to(hubId).emit("presence_list", presenceList(hubId));

    // ── PIXEL ────────────────────────────────────────────────────────────────
    socket.on("pixel", (data) => {
      const x = parseInt(data.x);
      const y = parseInt(data.y);
      const erase = !!data.erase;
      const locked = !!data.locked && isAdmin; // only admins can lock
      const color = erase
        ? null
        : String(data.color).match(/^#[0-9a-fA-F]{6}$/)
          ? data.color
          : "#ffffff";

      const buf = pixelBuffer.get(hubId);
      const erased = erasedKeys.get(hubId);
      const key = `${x},${y}`;
      const existing = buf.get(key);

      // Block erase of locked pixels from non-admins
      if (erase && existing?.locked && !isAdmin) {
        // Silently deny — client already shows toast
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
      const x = parseInt(data.x) || 2000;
      const y = parseInt(data.y) || 1400;
      const p = getPresence(hubId).get(socket.id);
      if (p) {
        p.x = x;
        p.y = y;
      }
      socket.to(hubId).emit("move", { user_id: userId, username, x, y });
    });

    // ── BUBBLE SAVE ───────────────────────────────────────────────────────────
    socket.on("bubble_save", async (data, ack) => {
      const isEdit = !!data.id;
      const type = data.type === "emotion" ? "emotion" : "brain";
      const x = parseInt(data.x) || 100;
      const y = parseInt(data.y) || 100;
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
        let bubbleId;
        if (isEdit) {
          bubbleId = parseInt(data.id);
          await pool.execute(
            "UPDATE nh_bubbles SET x=?,y=?,color=?,content=?,emotion=?,emotion_val=? WHERE id=?",
            [x, y, color, content, emotion, emotionVal, bubbleId],
          );
        } else {
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
          bubbleId = res.insertId;
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
      const id = parseInt(data.id),
        x = parseInt(data.x),
        y = parseInt(data.y);
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

const WORLD_W = 4000,
  WORLD_H = 2800; // for default spawn position

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
