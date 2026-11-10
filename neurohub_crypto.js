/**
 * neurohub_crypto.js — Node-port af  public_html/includes/crypto.inc.php
 *
 * Placering: samme mappe som neurohub_server.js (fx /public_html/hub/js/).
 *
 * Er 100 % format-kompatibel med PHP-filen, så PHP og Node kan læse hinandens data:
 *   · AES-256-GCM
 *   · nøgle  = SHA-256( ENCRYPTION_KEY )  (32 bytes)
 *   · iv     = 12 tilfældige bytes, tag = 16 bytes
 *   · format = "v2:" + base64( iv . tag . ciphertext )
 *   · $context i PHP == `context` her (bruges som AAD)
 *
 * ENCRYPTION_KEY læses fra process.env — og hvis den ikke er sat derfra, fra
 * den samme .env som PHP bruger (coaching/.env). Stien kan overstyres med
 *   ENCRYPTION_ENV_FILE=/absolut/sti/til/.env
 *
 * Forskel fra PHP: hvis ENCRYPTION_KEY mangler, falder PHP tilbage til en
 * hardkodet standardnøgle. Det gør Node IKKE — serveren nægter at starte,
 * så personfølsomme data aldrig krypteres med en offentlig nøgle.
 */
"use strict";

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const CIPHER = "aes-256-gcm";

// ── .env-loader (samme logik som loadEnv() i PHP) ────────────────────────────
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
    path.resolve(__dirname, "../../coaching/.env"), // /public_html/hub/js → /public_html/coaching/.env
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

// Kaldes ved opstart, så fejlen kommer med det samme og ikke ved første boble.
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

/** Krypterer med AES-256-GCM. null/"" → "" (som i PHP). */
function encryptData(data, context = "") {
  if (data === null || data === undefined || data === "") return "";
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
  return "v2:" + Buffer.concat([iv, tag, ct]).toString("base64");
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

/**
 * Dekrypterer "v2:"-data. Data der IKKE starter med "v2:" returneres uændret
 * (gamle, endnu ikke migrerede klartekst-rækker). Kan en v2-værdi ikke
 * dekrypteres, returneres "" (som i PHP) og der logges en advarsel.
 */
function decryptData(data, context = "") {
  if (data === null || data === undefined || data === "") return "";
  if (!isEncrypted(data)) return data;

  const buf = Buffer.from(data.slice(3), "base64");
  if (buf.length < 28) return "";
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
  console.warn(
    "[neurohub] decryptData: kunne ikke dekryptere en værdi (forkert nøgle/kontekst?)",
  );
  return "";
}

module.exports = { encryptData, decryptData, isEncrypted, assertKeyConfigured };
