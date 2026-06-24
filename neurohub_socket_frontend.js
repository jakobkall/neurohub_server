/**
 * neurohub_socket_frontend.js
 * Erstat den eksisterende <script> blok i neurohub.php med denne fil.
 * Inkluder socket.io INDEN dette script:
 *   <script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
 *
 * Alle postAction() kald er FJERNET.
 * Al kommunikation sker nu via socket.emit() / socket.on()
 */

// ── Config (injiceret fra PHP) ────────────────────────────────────────────────
// ME og HUB_ID kommer fra PHP via:
//   const ME     = <?= $userJson ?>;
//   const HUB_ID = <?= $hubIdJson ?>;
//   const WS_URL = <?= json_encode($wsUrl) ?>;
//   (Tilføj $wsUrl = 'https://jakobkall.com:3001'; i dit PHP)

const WORLD_W = 2000;
const WORLD_H = 1400;
const CELL = 8;
const COLS = Math.floor(WORLD_W / CELL);
const ROWS = Math.floor(WORLD_H / CELL);

// ── State ─────────────────────────────────────────────────────────────────────
let mode = "draw";
let drawColor = "#ffffff";
let bubbleColor = "#00ffcc";
let zoom = 1.5;
let panX = 0,
  panY = 0;
let isPainting = false;
let isPanning = false;
let panStartX = 0,
  panStartY = 0;
let panStartPX = 0,
  panStartPY = 0;
let myAvatarX = WORLD_W / 2,
  myAvatarY = WORLD_H / 2;
let hoverCell = null;
let pendingBubble = null;
let editBubbleId = null;
let bubbleColorSel = "#00ffcc";
let keysHeld = {};
window.isUserDragging = false;

const pixelMap = new Map(); // "x,y" -> color
const bubbleMap = new Map(); // id    -> bubble obj
const branchSet = new Set(); // "parentId-childId"
const usersMap = new Map(); // user_id -> {username,x,y}
window._floats = {}; // userId  -> {msg, ts}

// ── Canvases ──────────────────────────────────────────────────────────────────
const worldEl = document.getElementById("world");
const gridCvs = document.getElementById("grid-canvas");
const pixCvs = document.getElementById("pixel-canvas");
const ovlCvs = document.getElementById("overlay-canvas");
const gCtx = gridCvs.getContext("2d");
const pCtx = pixCvs.getContext("2d");
const oCtx = ovlCvs.getContext("2d");
const miniCvs = document.getElementById("minimap");
const mCtx = miniCvs.getContext("2d");

function initCanvases() {
  const dpr = window.devicePixelRatio || 1;
  [gridCvs, pixCvs, ovlCvs].forEach((c) => {
    c.width = WORLD_W * dpr;
    c.height = WORLD_H * dpr;
    c.style.width = WORLD_W + "px";
    c.style.height = WORLD_H + "px";
    c.getContext("2d").scale(dpr, dpr);
  });
  drawGrid();
}

function drawGrid() {
  gCtx.clearRect(0, 0, WORLD_W, WORLD_H);
  gCtx.strokeStyle = "rgba(255,255,255,0.04)";
  gCtx.lineWidth = 1;
  for (let x = 0; x <= WORLD_W; x += CELL) {
    gCtx.beginPath();
    gCtx.moveTo(x, 0);
    gCtx.lineTo(x, WORLD_H);
    gCtx.stroke();
  }
  for (let y = 0; y <= WORLD_H; y += CELL) {
    gCtx.beginPath();
    gCtx.moveTo(0, y);
    gCtx.lineTo(WORLD_W, y);
    gCtx.stroke();
  }
}

function redrawPixels() {
  pCtx.clearRect(0, 0, WORLD_W, WORLD_H);
  pixelMap.forEach((color, key) => {
    const [cx, cy] = key.split(",").map(Number);
    pCtx.fillStyle = color;
    pCtx.fillRect(cx * CELL, cy * CELL, CELL, CELL);
  });
}

function paintOnePixel(cx, cy, color) {
  pCtx.clearRect(cx * CELL, cy * CELL, CELL, CELL);
  if (color) {
    pCtx.fillStyle = color;
    pCtx.fillRect(cx * CELL, cy * CELL, CELL, CELL);
  }
}

// ── Socket.IO ─────────────────────────────────────────────────────────────────
let socket;
function initSocket() {
  socket = io(WS_URL, {
    auth: { user_id: ME.id, username: ME.username, hub_id: HUB_ID },
    transports: ["websocket"],
    reconnectionAttempts: 10,
    reconnectionDelay: 2000,
  });

  socket.on("connect", () => {
    console.log("[socket] Forbundet:", socket.id);
    document.getElementById("online-dot")?.classList.add("online");
  });

  socket.on("disconnect", (reason) => {
    console.warn("[socket] Afbrudt:", reason);
  });

  // ── Initial state fra server ─────────────────────────────────────────────
  socket.on("state", (data) => {
    // Pixels
    pixelMap.clear();
    data.pixels.forEach(([x, y, c]) => pixelMap.set(`${x},${y}`, c));
    redrawPixels();

    // Bubbles
    bubbleMap.clear();
    data.bubbles.forEach((b) => bubbleMap.set(b.id, b));
    branchSet.clear();
    data.branches.forEach(([p, c]) => branchSet.add(`${p}-${c}`));
    renderBubbles();

    // Chat
    renderChat(data.chat || []);

    drawOverlay();
    drawMinimap();
  });

  // ── Realtime: en anden bruger har malet ───────────────────────────────────
  socket.on("pixel", (data) => {
    const key = `${data.x},${data.y}`;
    if (data.erase || !data.color) {
      pixelMap.delete(key);
      paintOnePixel(data.x, data.y, null);
    } else {
      pixelMap.set(key, data.color);
      paintOnePixel(data.x, data.y, data.color);
    }
    drawMinimap();
  });

  // ── Realtime: chat besked ────────────────────────────────────────────────
  socket.on("chat", (msg) => {
    appendChatMsg(msg);
    // Float besked ved afsenders avatar
    const u = Array.from(usersMap.values()).find(
      (u) => u.username === msg.username,
    );
    if (u) showFloatMsg(u.user_id, msg.message);
    if (msg.username === ME.username) showFloatMsg(ME.id, msg.message);
  });

  // ── Realtime: bevægelse (presence) ───────────────────────────────────────
  socket.on("move", (data) => {
    if (data.user_id == ME.id) return;
    usersMap.set(data.user_id, {
      user_id: data.user_id,
      username: data.username,
      x: data.x,
      y: data.y,
    });
    drawOverlay();
  });

  // ── Realtime: presence list (connect/disconnect) ──────────────────────────
  socket.on("presence_list", (list) => {
    usersMap.clear();
    list.forEach((u) => usersMap.set(u.user_id, u));
    document.getElementById("online-count").textContent =
      list.length + " online";
    renderUsersList(list);
    drawOverlay();
  });

  // ── Realtime: bubble opdateret/oprettet ───────────────────────────────────
  socket.on("bubble_update", (b) => {
    bubbleMap.set(b.id, b);
    if (!window.isUserDragging) renderBubbles();
  });

  // ── Realtime: bubble slettet ──────────────────────────────────────────────
  socket.on("bubble_delete", (data) => {
    bubbleMap.delete(data.id);
    branchSet.forEach((k) => {
      const [p, c] = k.split("-").map(Number);
      if (p === data.id || c === data.id) branchSet.delete(k);
    });
    if (!window.isUserDragging) renderBubbles();
  });

  // ── Realtime: bubble flyttet af en anden ─────────────────────────────────
  socket.on("bubble_moved", (data) => {
    const b = bubbleMap.get(data.id);
    if (b) {
      b.x = data.x;
      b.y = data.y;
    }
    if (!window.isUserDragging) renderBubbles();
  });

  // ── Realtime: ny forgrening ───────────────────────────────────────────────
  socket.on("branch_add", (data) => {
    branchSet.add(`${data.parent_id}-${data.child_id}`);
    if (!window.isUserDragging) renderBubbles();
  });
}

// ── Pixel tegning (lokal + socket) ────────────────────────────────────────────
function paintCell(cx, cy) {
  const key = `${cx},${cy}`;
  const erase = mode === "erase";

  if (erase) {
    pixelMap.delete(key);
    paintOnePixel(cx, cy, null);
    socket.emit("pixel", { x: cx, y: cy, erase: true });
  } else {
    pixelMap.set(key, drawColor);
    paintOnePixel(cx, cy, drawColor);
    socket.emit("pixel", { x: cx, y: cy, color: drawColor });
  }
  drawMinimap();
}

// ── Transform ─────────────────────────────────────────────────────────────────
function applyTransform() {
  worldEl.style.transform = `translate(${panX}px,${panY}px) scale(${zoom})`;
}
function screenToWorld(sx, sy) {
  const rect = document.getElementById("world-wrap").getBoundingClientRect();
  return {
    wx: (sx - rect.left - panX) / zoom,
    wy: (sy - rect.top - panY) / zoom,
  };
}
function resetView() {
  zoom = 1.5;
  const w = document.getElementById("world-wrap");
  panX = (w.clientWidth - WORLD_W * zoom) / 2;
  panY = (w.clientHeight - WORLD_H * zoom) / 2;
  applyTransform();
  updateZoomIndicator();
}
function updateZoomIndicator() {
  document.getElementById("zoom-indicator").textContent =
    `zoom ${zoom.toFixed(1)}×`;
}
function centerOnAvatar() {
  const w = document.getElementById("world-wrap");
  panX = w.clientWidth / 2 - myAvatarX * zoom;
  panY = w.clientHeight / 2 - myAvatarY * zoom;
  applyTransform();
}

// ── Avatar bevægelse ──────────────────────────────────────────────────────────
const AVATAR_SPEED = 110;
let lastFrameTime = performance.now();
let isMoving = false;
let lastSentX = -1,
  lastSentY = -1;

function updateAvatarPhysics() {
  if (!isMoving) return;
  const now = performance.now();
  const dt = (now - lastFrameTime) / 1000;
  lastFrameTime = now;
  let dx = 0,
    dy = 0;
  if (keysHeld["ArrowUp"]) dy -= 1;
  if (keysHeld["ArrowDown"]) dy += 1;
  if (keysHeld["ArrowLeft"]) dx -= 1;
  if (keysHeld["ArrowRight"]) dx += 1;
  if (dx !== 0 || dy !== 0) {
    const len = Math.sqrt(dx * dx + dy * dy);
    myAvatarX = Math.max(
      0,
      Math.min(WORLD_W, myAvatarX + (dx / len) * AVATAR_SPEED * dt),
    );
    myAvatarY = Math.max(
      0,
      Math.min(WORLD_H, myAvatarY + (dy / len) * AVATAR_SPEED * dt),
    );
    drawOverlay();
    centerOnAvatar();
    // Send bevægelse til server (throttled – kun hvis vi har bevæget os ≥ 4px)
    const rx = Math.round(myAvatarX),
      ry = Math.round(myAvatarY);
    if (Math.abs(rx - lastSentX) > 4 || Math.abs(ry - lastSentY) > 4) {
      socket?.emit("move", { x: rx, y: ry });
      lastSentX = rx;
      lastSentY = ry;
    }
  }
  requestAnimationFrame(updateAvatarPhysics);
}

document.addEventListener("keydown", (e) => {
  if (
    e.target.tagName === "INPUT" ||
    e.target.tagName === "TEXTAREA" ||
    e.target.isContentEditable
  )
    return;
  if (e.key.startsWith("Arrow")) {
    keysHeld[e.key] = true;
    if (!isMoving) {
      isMoving = true;
      lastFrameTime = performance.now();
      requestAnimationFrame(updateAvatarPhysics);
    }
  }
  if (e.key === "Enter" && e.target.tagName !== "TEXTAREA")
    document.getElementById("chat-input")?.focus();
});
document.addEventListener("keyup", (e) => {
  delete keysHeld[e.key];
  if (!Object.keys(keysHeld).some((k) => k.startsWith("Arrow")))
    isMoving = false;
});

// ── Heartbeat til server (presence, hvert 5. sek) ────────────────────────────
setInterval(() => {
  socket?.emit("move", { x: Math.round(myAvatarX), y: Math.round(myAvatarY) });
}, 5000);

// ── Mouse ─────────────────────────────────────────────────────────────────────
const wrap = document.getElementById("world-wrap");
wrap.addEventListener("mousedown", (e) => {
  if (e.button === 1 || mode === "move") {
    isPanning = true;
    panStartX = e.clientX;
    panStartY = e.clientY;
    panStartPX = panX;
    panStartPY = panY;
    wrap.style.cursor = "grabbing";
    return;
  }
  if (e.button !== 0) return;
  const { wx, wy } = screenToWorld(e.clientX, e.clientY);
  if (mode === "draw" || mode === "erase") {
    isPainting = true;
    const cx = Math.floor(wx / CELL),
      cy = Math.floor(wy / CELL);
    if (cx >= 0 && cx < COLS && cy >= 0 && cy < ROWS) paintCell(cx, cy);
  } else if (mode === "brain" || mode === "emotion") {
    openModal(wx, wy, mode);
  }
});
wrap.addEventListener("mousemove", (e) => {
  if (isPanning) {
    panX = panStartPX + (e.clientX - panStartX);
    panY = panStartPY + (e.clientY - panStartY);
    applyTransform();
    return;
  }
  const { wx, wy } = screenToWorld(e.clientX, e.clientY);
  const cx = Math.floor(wx / CELL),
    cy = Math.floor(wy / CELL);
  hoverCell = cx >= 0 && cx < COLS && cy >= 0 && cy < ROWS ? { cx, cy } : null;
  drawOverlay();
  if (isPainting && (mode === "draw" || mode === "erase") && hoverCell)
    paintCell(cx, cy);
});
wrap.addEventListener("mouseup", () => {
  isPainting = false;
  isPanning = false;
  wrap.style.cursor = "";
});
wrap.addEventListener("mouseleave", () => {
  isPainting = false;
  isPanning = false;
  hoverCell = null;
});
wrap.addEventListener(
  "wheel",
  (e) => {
    e.preventDefault();
    const rect = wrap.getBoundingClientRect();
    const mx = e.clientX - rect.left,
      my = e.clientY - rect.top;
    const delta = e.deltaY < 0 ? 1.1 : 0.9;
    const nz = Math.min(4, Math.max(0.3, zoom * delta));
    panX = mx - (mx - panX) * (nz / zoom);
    panY = my - (my - panY) * (nz / zoom);
    zoom = nz;
    applyTransform();
    updateZoomIndicator();
  },
  { passive: false },
);

// ── Mode / UI ─────────────────────────────────────────────────────────────────
function setMode(m) {
  mode = m;
  ["draw", "erase", "brain", "emotion", "move"].forEach((id) => {
    document.getElementById("mode-" + id)?.classList.toggle("active", id === m);
  });
  wrap.style.cursor =
    m === "move"
      ? "grab"
      : m === "draw" || m === "erase"
        ? "crosshair"
        : "cell";
  hoverCell = null;
  drawOverlay();
}
function togglePanel() {
  const p = document.getElementById("right-panel"),
    btn = document.getElementById("toggle-panel");
  p.classList.toggle("hidden");
  btn.textContent = p.classList.contains("hidden") ? "▶" : "◀";
}
function switchTab(name) {
  document.querySelectorAll(".panel-tab").forEach((t, i) => {
    const tabs = ["chat", "tools", "users"];
    t.classList.toggle("active", tabs[i] === name);
  });
  document.querySelectorAll(".panel-section").forEach((s) => {
    s.classList.toggle("active", s.id === `tab-${name}`);
  });
}

// ── Chat ──────────────────────────────────────────────────────────────────────
const chatInput = document.getElementById("chat-input");
const chatSend = document.getElementById("chat-send");

function appendChatMsg(m) {
  const log = document.getElementById("chat-log");
  const div = document.createElement("div");
  div.className = "chat-msg";
  div.innerHTML = `<span class="cn">${escHtml(m.username)}</span> <span class="cm">${escHtml(m.message)}</span>`;
  log.appendChild(div);
  if (log.children.length > 50) log.firstChild.remove();
  log.scrollTop = log.scrollHeight;
}
function renderChat(msgs) {
  document.getElementById("chat-log").innerHTML = "";
  msgs.forEach(appendChatMsg);
}
async function sendChat() {
  const msg = chatInput.value.trim();
  if (!msg) return;
  chatInput.value = "";
  socket.emit("chat", { message: msg });
  showFloatMsg(ME.id, msg);
}
chatSend.onclick = sendChat;
chatInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") sendChat();
});

function showFloatMsg(userId, msg) {
  window._floats[userId] = { msg, ts: Date.now() };
}

// ── Palette ───────────────────────────────────────────────────────────────────
const PALETTE = [
  "#000000",
  "#1a1a2e",
  "#16213e",
  "#0f3460",
  "#533483",
  "#9146FF",
  "#e94560",
  "#ff0055",
  "#ff6b6b",
  "#ffd93d",
  "#ffdd00",
  "#6bcb77",
  "#00ffcc",
  "#4d96ff",
  "#00b4d8",
  "#90e0ef",
  "#ffffff",
  "#cccccc",
  "#888888",
  "#444444",
  "#ff9f1c",
  "#f72585",
  "#7209b7",
  "#3a0ca3",
];
const BPALETTE = [
  "#00ffcc",
  "#ff66cc",
  "#ffdd00",
  "#4d96ff",
  "#ff0055",
  "#6bcb77",
  "#f72585",
  "#9146FF",
  "#ff9f1c",
  "#ffffff",
  "#888888",
  "#cccccc",
];

function buildPalette() {
  const cg = document.getElementById("color-grid");
  cg.innerHTML = "";
  PALETTE.forEach((c) => {
    const sw = document.createElement("div");
    sw.className = "swatch" + (c === drawColor ? " selected" : "");
    sw.style.background = c;
    sw.onclick = () => {
      drawColor = c;
      document
        .querySelectorAll(".swatch")
        .forEach((s) => s.classList.remove("selected"));
      sw.classList.add("selected");
      document.getElementById("custom-color").value = c;
      if (mode !== "brain" && mode !== "emotion") setMode("draw");
    };
    cg.appendChild(sw);
  });
  const bg = document.getElementById("bubble-color-grid");
  bg.innerHTML = "";
  BPALETTE.forEach((c) => {
    const sw = document.createElement("div");
    sw.className = "b-swatch" + (c === bubbleColorSel ? " selected" : "");
    sw.style.background = c;
    sw.onclick = () => {
      bubbleColorSel = c;
      bubbleColor = c;
      document
        .querySelectorAll(".b-swatch")
        .forEach((s) => s.classList.remove("selected"));
      sw.classList.add("selected");
    };
    bg.appendChild(sw);
  });
  document.getElementById("custom-color").addEventListener("input", (e) => {
    drawColor = e.target.value;
    document
      .querySelectorAll(".swatch")
      .forEach((s) => s.classList.remove("selected"));
    if (mode !== "brain" && mode !== "emotion") setMode("draw");
  });
}
document.getElementById("modal-slider").addEventListener("input", (e) => {
  document.getElementById("modal-slider-val").textContent = e.target.value;
});

// ── Bubble rendering ──────────────────────────────────────────────────────────
const bubbleLayer = document.getElementById("bubble-layer");

function renderBubbles() {
  if (window.isUserDragging) return;
  bubbleLayer.innerHTML = "";
  drawOverlay();

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("width", WORLD_W);
  svg.setAttribute("height", WORLD_H);
  svg.style.cssText =
    "position:absolute;top:0;left:0;pointer-events:none;z-index:1;";
  bubbleLayer.appendChild(svg);

  bubbleMap.forEach((b) => {
    const node = document.createElement("div");
    node.className = "bubble-node";
    node.style.left = b.x + "px";
    node.style.top = b.y + "px";
    node.dataset.id = b.id;
    node.style.position = "absolute";
    node.style.zIndex = "2";

    // Drag
    node.addEventListener("mousedown", function (e) {
      if (
        e.target.classList.contains("branch-btn") ||
        e.target.classList.contains("del-btn") ||
        e.target.getAttribute("contenteditable") === "true"
      )
        return;
      e.preventDefault();
      window.isUserDragging = true;
      const startX = e.clientX - b.x,
        startY = e.clientY - b.y;
      let hasMoved = false;
      function onMove(me) {
        hasMoved = true;
        b.x = Math.min(WORLD_W - 80, Math.max(80, me.clientX - startX));
        b.y = Math.min(WORLD_H - 80, Math.max(80, me.clientY - startY));
        node.style.left = b.x + "px";
        node.style.top = b.y + "px";
        updateSvgLines();
      }
      async function onUp() {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        if (hasMoved) {
          bubbleMap.set(b.id, b);
          socket.emit("bubble_move", { id: b.id, x: b.x, y: b.y });
        }
        setTimeout(() => {
          window.isUserDragging = false;
        }, 300);
      }
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    });

    // Box
    const box = document.createElement("div");
    box.className = "bubble-box" + (b.type === "emotion" ? " emotion-box" : "");
    box.style.borderColor = b.color;
    box.style.boxShadow = `4px 4px 0 ${b.color}44`;

    const tag = document.createElement("div");
    tag.className = "bubble-tag";
    tag.style.color = b.color;
    tag.textContent = b.type === "emotion" ? "❤ følelse" : "💭 brainstorm";
    box.appendChild(tag);

    const cont = document.createElement("div");
    cont.className = "bubble-content";
    cont.style.cursor = "text";
    if (b.type === "emotion") {
      const span = document.createElement("span");
      span.contentEditable = "true";
      span.style.cssText = "display:inline-block;min-width:50px;outline:none;";
      span.textContent = b.emotion || "Skriv følelse…";
      span.addEventListener("blur", () => {
        b.emotion = span.textContent;
        bubbleMap.set(b.id, b);
        socket.emit("bubble_edit", {
          id: b.id,
          emotion: b.emotion,
          emotion_val: b.emotion_val,
        });
      });
      cont.appendChild(span);
      if (b.emotion_val) {
        const sw = document.createElement("div");
        sw.className = "bubble-slider-wrap";
        const sl = document.createElement("div");
        sl.className = "bubble-slider-label";
        sl.textContent = `intensitet: ${b.emotion_val}/10`;
        const bar = document.createElement("div");
        bar.style.cssText =
          "height:6px;background:#1a1a2e;border:1px solid #333;margin-top:3px;";
        const fill = document.createElement("div");
        fill.style.cssText = `height:100%;width:${b.emotion_val * 10}%;background:${b.color};`;
        bar.appendChild(fill);
        sw.appendChild(sl);
        sw.appendChild(bar);
        cont.appendChild(sw);
      }
    } else {
      cont.contentEditable = "true";
      cont.style.outline = "none";
      cont.textContent = b.content || "Skriv noget…";
      cont.addEventListener("blur", () => {
        b.content = cont.textContent;
        bubbleMap.set(b.id, b);
        socket.emit("bubble_edit", { id: b.id, content: b.content });
      });
    }
    box.appendChild(cont);

    const branchBtn = document.createElement("div");
    branchBtn.className = "branch-btn";
    branchBtn.textContent = "+";
    branchBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      startBranch(b.id, b.x, b.y, b.type);
    });
    box.appendChild(branchBtn);

    const delBtn = document.createElement("div");
    delBtn.className = "del-btn";
    delBtn.textContent = "✕";
    delBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      deleteBubble(b.id);
    });
    box.appendChild(delBtn);

    node.appendChild(box);
    bubbleLayer.appendChild(node);
  });

  function updateSvgLines() {
    svg.innerHTML = "";
    branchSet.forEach((key) => {
      const [pid, cid] = key.split("-").map(Number);
      const p = bubbleMap.get(pid),
        c = bubbleMap.get(cid);
      if (!p || !c) return;
      const pN = bubbleLayer.querySelector(`.bubble-node[data-id='${pid}']`);
      const cN = bubbleLayer.querySelector(`.bubble-node[data-id='${cid}']`);
      const pW = (pN ? pN.offsetWidth : 150) / 2,
        pH = (pN ? pN.offsetHeight : 80) / 2;
      const cW = (cN ? cN.offsetWidth : 150) / 2,
        cH = (cN ? cN.offsetHeight : 80) / 2;
      const line = document.createElementNS(
        "http://www.w3.org/2000/svg",
        "line",
      );
      line.setAttribute("x1", p.x + pW);
      line.setAttribute("y1", p.y + pH);
      line.setAttribute("x2", c.x + cW);
      line.setAttribute("y2", c.y + cH);
      line.setAttribute("stroke", "#00ffcc99");
      line.setAttribute("stroke-width", "2");
      line.setAttribute("stroke-dasharray", "5,5");
      svg.appendChild(line);
    });
  }
  updateSvgLines();
}

function startBranch(parentId, px, py, type) {
  openModal(
    Math.min(WORLD_W - 60, px + 180),
    Math.min(WORLD_H - 60, py + 60),
    type,
    null,
    parentId,
  );
}

function deleteBubble(id) {
  bubbleMap.delete(id);
  branchSet.forEach((k) => {
    const [p, c] = k.split("-").map(Number);
    if (p === id || c === id) branchSet.delete(k);
  });
  renderBubbles();
  socket.emit("bubble_delete", { id });
}

// ── Modal ─────────────────────────────────────────────────────────────────────
function openModal(wx, wy, type, editId, parentId) {
  const modal = document.getElementById("bubble-modal"),
    box = document.getElementById("bubble-modal-box");
  editBubbleId = editId || null;
  pendingBubble = editId
    ? null
    : {
        x: Math.round(wx),
        y: Math.round(wy),
        type,
        parentId: parentId || null,
      };
  document.getElementById("modal-title").textContent =
    type === "emotion" ? "❤ følelse boble" : "💭 brainstorm boble";
  box.className = type === "emotion" ? "emotion" : "";
  const bF = document.getElementById("modal-brain-field"),
    eF = document.getElementById("modal-emotion-field"),
    sF = document.getElementById("modal-slider-field");
  if (type === "emotion") {
    bF.style.display = "none";
    eF.style.display = "";
    sF.style.display = "";
    if (editId) {
      const b = bubbleMap.get(editId);
      document.getElementById("modal-emotion").value = b?.emotion || "";
      document.getElementById("modal-slider").value = b?.emotion_val || 5;
      document.getElementById("modal-slider-val").textContent =
        b?.emotion_val || 5;
    } else {
      document.getElementById("modal-emotion").value = "";
      document.getElementById("modal-slider").value = 5;
      document.getElementById("modal-slider-val").textContent = "5";
    }
  } else {
    bF.style.display = "";
    eF.style.display = "none";
    sF.style.display = "none";
    if (editId) {
      const b = bubbleMap.get(editId);
      document.getElementById("modal-content").value = b?.content || "";
    } else document.getElementById("modal-content").value = "";
  }
  modal.classList.add("open");
  setTimeout(() => {
    (type === "emotion"
      ? document.getElementById("modal-emotion")
      : document.getElementById("modal-content")
    ).focus();
  }, 50);
}
function closeModal() {
  document.getElementById("bubble-modal").classList.remove("open");
  pendingBubble = null;
  editBubbleId = null;
}

function saveModal() {
  const isEdit = editBubbleId !== null;
  const b = isEdit ? bubbleMap.get(editBubbleId) : pendingBubble;
  if (!b) {
    closeModal();
    return;
  }
  const type = b.type;
  const content = document.getElementById("modal-content").value.trim();
  const emotion = document.getElementById("modal-emotion").value.trim();
  const val = parseInt(document.getElementById("modal-slider").value) || 5;
  const color = bubbleColorSel;
  const payload = {
    type,
    content,
    emotion,
    emotion_val: val,
    color,
    x: isEdit ? b.x : pendingBubble?.x || 100,
    y: isEdit ? b.y : pendingBubble?.y || 100,
  };
  if (isEdit) payload.id = editBubbleId;
  if (!isEdit && pendingBubble?.parentId)
    payload.parent_id = pendingBubble.parentId;

  // Optimistisk lokal tilføjelse
  if (!isEdit) {
    const tmpId = "tmp_" + Date.now();
    bubbleMap.set(tmpId, {
      id: tmpId,
      type,
      content,
      emotion,
      emotion_val: val,
      color,
      x: payload.x,
      y: payload.y,
      username: ME.username,
      user_id: ME.id,
    });
    renderBubbles();
  }
  // Emit til server – serveren broadcaster 'bubble_update' med det rigtige id tilbage
  socket.emit("bubble_save", payload, (res) => {
    if (!isEdit && res?.id) {
      bubbleMap.delete(
        Array.from(bubbleMap.keys()).find((k) => String(k).startsWith("tmp_")),
      );
    }
  });
  closeModal();
}

// ── Avatar draw ───────────────────────────────────────────────────────────────
function drawAvatar(ctx, wx, wy, name, col) {
  const S = 28;
  ctx.imageSmoothingEnabled = true;
  ctx.save();
  ctx.translate(wx - S / 2, wy - S - 4);
  const sf = S / 24;
  ctx.scale(sf, sf);
  const brainPath = new Path2D(
    "M12,2C8.13,2,5,5.13,5,9c0,2.38,1.19,4.47,3,5.74V17c0,1.1,0.9,2,2,2h4c1.1,0,2,-0.9,2,-2v-2.26c1.81,-1.27,3,-3.36,3,-5.74C19,5.13,15.87,2,12,2z M12,14c-2.76,0-5,-2.24-5,-5s2.24-5,5-5s5,2.24,5,5S14.76,14,12,14z",
  );
  ctx.fillStyle = col;
  ctx.fill(brainPath);
  ctx.restore();
  ctx.font = "bold 10px sans-serif";
  ctx.textBaseline = "top";
  const tw = ctx.measureText(name).width;
  ctx.fillStyle = "#0d0d1acc";
  ctx.fillRect(wx - tw / 2 - 4, wy - S - 20, tw + 8, 12);
  ctx.fillStyle = col;
  ctx.fillText(name, wx - tw / 2, wy - S - 19);
}

function drawOverlay() {
  oCtx.clearRect(0, 0, WORLD_W, WORLD_H);
  if (hoverCell && (mode === "draw" || mode === "erase")) {
    const { cx, cy } = hoverCell;
    oCtx.strokeStyle = mode === "erase" ? "#ff0055" : "#ffffff";
    oCtx.lineWidth = 2;
    oCtx.strokeRect(cx * CELL + 1, cy * CELL + 1, CELL - 2, CELL - 2);
  }
  usersMap.forEach((u, uid) => {
    if (uid == ME.id) return;
    drawAvatar(oCtx, u.x, u.y, u.username, "#00ffcc");
    if (window._floats[uid])
      drawFloatMsg(oCtx, u.x, u.y, window._floats[uid].msg, "#ffdd00");
  });
  drawAvatar(oCtx, myAvatarX, myAvatarY, ME.username, "#ffdd00");
  if (window._floats[ME.id])
    drawFloatMsg(
      oCtx,
      myAvatarX,
      myAvatarY,
      window._floats[ME.id].msg,
      "#ffdd00",
    );
}

function drawFloatMsg(ctx, wx, wy, msg, col) {
  const maxW = 140,
    pad = 6,
    lineH = 11;
  ctx.font = "9px Courier New";
  const words = msg.split(" ");
  const lines = [];
  let cur = "";
  words.forEach((w) => {
    const t = cur ? cur + " " + w : w;
    if (ctx.measureText(t).width > maxW - pad * 2) {
      if (cur) lines.push(cur);
      cur = w;
    } else cur = t;
  });
  if (cur) lines.push(cur);
  const bw = Math.min(
    maxW,
    Math.max(...lines.map((l) => ctx.measureText(l).width)) + pad * 2,
  );
  const bh = lines.length * lineH + pad * 2,
    bx = wx - bw / 2,
    by = wy - 32 - bh - 8;
  ctx.fillStyle = "#0d0d1add";
  ctx.fillRect(bx, by, bw, bh);
  ctx.strokeStyle = col;
  ctx.lineWidth = 2;
  ctx.strokeRect(bx, by, bw, bh);
  ctx.fillStyle = col;
  lines.forEach((l, i) =>
    ctx.fillText(l, bx + pad, by + pad + lineH * (i + 1) - 2),
  );
}

// ── Minimap ───────────────────────────────────────────────────────────────────
function drawMinimap() {
  const MW = miniCvs.width,
    MH = miniCvs.height;
  mCtx.fillStyle = "#0a0a14";
  mCtx.fillRect(0, 0, MW, MH);
  pixelMap.forEach((color, key) => {
    const [cx, cy] = key.split(",").map(Number);
    mCtx.fillStyle = color;
    mCtx.fillRect(
      Math.round(((cx * CELL) / WORLD_W) * MW),
      Math.round(((cy * CELL) / WORLD_H) * MH),
      Math.max(1, Math.round((CELL / WORLD_W) * MW)),
      Math.max(1, Math.round((CELL / WORLD_H) * MH)),
    );
  });
  const wEl = document.getElementById("world-wrap");
  const vx = -panX / zoom,
    vy = -panY / zoom,
    vw = wEl.clientWidth / zoom,
    vh = wEl.clientHeight / zoom;
  mCtx.strokeStyle = "#00ffcc";
  mCtx.lineWidth = 1;
  mCtx.strokeRect(
    (vx / WORLD_W) * MW,
    (vy / WORLD_H) * MH,
    (vw / WORLD_W) * MW,
    (vh / WORLD_H) * MH,
  );
  mCtx.fillStyle = "#ffdd00";
  mCtx.fillRect(
    (myAvatarX / WORLD_W) * MW - 2,
    (myAvatarY / WORLD_H) * MH - 2,
    4,
    4,
  );
}

// ── Float msg cleanup ─────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  Object.keys(window._floats).forEach((k) => {
    if (now - window._floats[k].ts > 7000) delete window._floats[k];
  });
  drawOverlay();
}, 1000);

// ── Users list ────────────────────────────────────────────────────────────────
function renderUsersList(users) {
  const ul = document.getElementById("users-list");
  ul.innerHTML = "";
  users.forEach((u) => {
    const div = document.createElement("div");
    div.className = "user-item";
    const dot = document.createElement("div");
    dot.className = "user-dot";
    const name = document.createElement("span");
    name.textContent = u.username;
    if (u.user_id == ME.id) name.style.color = "#ffdd00";
    div.appendChild(dot);
    div.appendChild(name);
    ul.appendChild(div);
  });
}

function escHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// ── Boot ──────────────────────────────────────────────────────────────────────
function boot() {
  initCanvases();
  buildPalette();
  resetView();
  setMode("draw");
  initSocket();
  drawMinimap();
}
boot();
