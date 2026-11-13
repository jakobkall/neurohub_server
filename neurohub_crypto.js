/**
 * neurohub_crypto.js — Node-port af  public_html/includes/crypto.inc.php
 *
 * Placering: samme mappe som neurohub_server.js (fx /public_html/hub/js/).
 *
 * Algoritme (samme som PHP):
 *   · AES-256-GCM
 *   · nøgle  = SHA-256( ENCRYPTION_KEY )  (32 bytes)
 *   · iv     = 12 tilfældige bytes, tag = 16 bytes
 *   · rå bytes = iv . tag . ciphertext
 *   · context (AAD) == $context i PHP
 *
 * To formater:
 *   · encryptData / decryptData     → "v2:" + base64(...)  (som PHP, bruges til login-token)
 *   · encryptStored / decryptStored → base64(...) UDEN prefix (bruges til boble-tekst i DB)
 *     decryptStored kan også læse værdier med "v2:" og gammel klartekst.
 *
 * ENCRYPTION_KEY læses fra process.env — og hvis den ikke er sat derfra, fra
 * coaching/.env. Stien kan overstyres med ENCRYPTION_ENV_FILE=/absolut/sti/.env
 *
 * Hvis ENCRYPTION_KEY mangler, nægter serveren at starte (ingen offentlig standardnøgle).
 */
"use strict";

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const CIPHER = "aes-256-gcm";
const B64_RE = /^[A-Za-z0-9+/]+={0,2}$/;

// ── .env-loader ──────────────────────────────────────────────────────────────
function loadEnvFile(file) {
  try {
    if (!file || !fs.existsSync(file)) return false;
    const lines = fs.readFileSync(file, "utf8").split(/\r?\n/);
    for (const raw of lines) {
      const line = raw.trim();
      if (line === "" || line[0] === "#" || !line.includes("=")) continue;
      const i = line.indexOf("=");
      const name = line.slice(0, i).trim();
      const value = line
        .slice(i + 1)
        .trim()
        .replace(/^["' ]+|["' ]+$/g, "");
      if (!(name in process.env)) process.env[name] = value;
    }
    return true;
  } catch (_) {
    return false;
  }
}

if (!process.env.ENCRYPTION_KEY) {
  const candidates = [
    process.env.ENCRYPTION_ENV_FILE,
    path.resolve(__dirname, "../../coaching/.env"),
    path.resolve(__dirname, "../coaching/.env"),
    path.resolve(__dirname, "coaching/.env"),
    path.resolve(__dirname, ".env"),
  ].filter(Boolean);
  for (const f of candidates) {
    loadEnvFile(f);
    if (process.env.ENCRYPTION_KEY) break;
  }
}

function rawKeyMaterial() {
  const raw = process.env.ENCRYPTION_KEY;
  if (typeof raw !== "string" || raw === "") {
    throw new Error(
      "ENCRYPTION_KEY er ikke sat (hverken i miljøet eller i coaching/.env). " +
        "Sæt ENCRYPTION_KEY eller ENCRYPTION_ENV_FILE.",
    );
  }
  return raw;
}

function assertKeyConfigured() {
  rawKeyMaterial();
  return true;
}

function encryptionKey() {
  return crypto.createHash("sha256").update(rawKeyMaterial(), "utf8").digest();
}

// Gammel nøgle: PHP afkortede/paddede selv til 32 bytes (nul-bytes)
function legacyKey() {
  const k = Buffer.alloc(32, 0);
  Buffer.from(rawKeyMaterial(), "utf8").copy(k, 0, 0, 32);
  return k;
}

function isEncrypted(v) {
  return typeof v === "string" && v.startsWith("v2:");
}

// ── Kernen: rå bytes <-> base64 ──────────────────────────────────────────────
function encryptToBase64(data, context) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv(CIPHER, encryptionKey(), iv, {
    authTagLength: 16,
  });
  if (context) cipher.setAAD(Buffer.from(String(context), "utf8"));
  const ct = Buffer.concat([
    cipher.update(String(data), "utf8"),
    cipher.final(),
  ]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, ct]).toString("base64");
}

function tryDecrypt(ct, key, iv, tag, context) {
  try {
    const d = crypto.createDecipheriv(CIPHER, key, iv, { authTagLength: 16 });
    d.setAuthTag(tag);
    if (context) d.setAAD(Buffer.from(String(context), "utf8"));
    return Buffer.concat([d.update(ct), d.final()]).toString("utf8");
  } catch (_) {
    return null;
  }
}

// Returnerer klartekst eller null hvis base64-strengen ikke kan dekrypteres
function decryptFromBase64(b64, context) {
  if (typeof b64 !== "string" || b64.length < 40 || !B64_RE.test(b64))
    return null;
  const buf = Buffer.from(b64, "base64");
  if (buf.length < 28) return null;
  const iv = buf.subarray(0, 12);
  const tag = buf.subarray(12, 28);
  const ct = buf.subarray(28);
  for (const key of [encryptionKey(), legacyKey()]) {
    let plain = tryDecrypt(ct, key, iv, tag, context);
    if (plain !== null) return plain;
    if (context) {
      plain = tryDecrypt(ct, key, iv, tag, "");
      if (plain !== null) return plain;
    }
  }
  return null;
}

// ── Format MED "v2:" (kompatibelt med PHP encryptData/decryptData) ───────────
function encryptData(data, context = "") {
  if (data === null || data === undefined || data === "") return "";
  return "v2:" + encryptToBase64(data, context);
}

function decryptData(data, context = "") {
  if (data === null || data === undefined || data === "") return "";
  if (!isEncrypted(data)) return data;
  const plain = decryptFromBase64(data.slice(3), context);
  if (plain !== null) return plain;
  console.warn(
    "[neurohub] decryptData: kunne ikke dekryptere en værdi (forkert nøgle/kontekst?)",
  );
  return "";
}

// ── Format UDEN prefix (bruges til boble-tekst i neurohub-databasen) ─────────
/** Krypterer til ren base64. null/"" → "". */
function encryptStored(data, context = "") {
  if (data === null || data === undefined || data === "") return "";
  return encryptToBase64(data, context);
}

/**
 * Undersøger en værdi fra databasen.
 *   state "empty" : tom
 *   state "plain" : ikke krypteret (gammel klartekst)
 *   state "ok"    : krypteret og dekrypteret (text = klartekst, hasPrefix = havde "v2:")
 *   state "bad"   : ligner krypteret men kan ikke dekrypteres (forkert nøgle?)
 */
function inspectStored(value, context = "") {
  if (value === null || value === undefined || value === "")
    return { state: "empty", text: "", hasPrefix: false };
  const s = String(value);
  const hasPrefix = s.startsWith("v2:");
  const body = hasPrefix ? s.slice(3) : s;
  const looksB64 = body.length >= 40 && B64_RE.test(body);
  if (!looksB64)
    return hasPrefix
      ? { state: "bad", text: "", hasPrefix }
      : { state: "plain", text: s, hasPrefix };
  const plain = decryptFromBase64(body, context);
  if (plain !== null) return { state: "ok", text: plain, hasPrefix };
  return { state: "bad", text: "", hasPrefix };
}

/**
 * Læser en gemt værdi: krypteret (med eller uden "v2:") → klartekst,
 * gammel klartekst → uændret, ikke-dekrypterbar → "" (og advarsel).
 */
function decryptStored(value, context = "") {
  const r = inspectStored(value, context);
  if (r.state === "bad") {
    console.warn(
      "[neurohub] decryptStored: kunne ikke dekryptere en værdi (forkert nøgle/kontekst?)",
    );
    return "";
  }
  return r.text;
}

module.exports = {
  encryptData,
  decryptData,
  isEncrypted,
  assertKeyConfigured,
  encryptStored,
  decryptStored,
  inspectStored,
};
