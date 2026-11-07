/**
 * neurohub_encrypt_access_codes.js — engangsscript
 *
 * Krypterer eksisterende klartekst-adgangskoder i nh_hubs.access_code med den
 * centrale nøgle (kontekst "nh_hubs.access_code", samme "v2:"-format som PHP).
 *
 *   node neurohub_encrypt_access_codes.js           ← TØR-KØRSEL (ændrer intet)
 *   node neurohub_encrypt_access_codes.js --apply   ← krypterer og gemmer
 *
 * ⚠️ KØR KUN MED --apply EFTER at PHP-siden er opdateret til at:
 *      · gemme nye koder med  encryptData($code, 'nh_hubs.access_code')
 *      · vise/sende/sammenligne koder via  decryptData($row['access_code'], 'nh_hubs.access_code')
 *    Ellers vil PHP vise "v2:…" i stedet for koden. Node-serveren er klar nu
 *    (den forstår både klartekst og krypteret).
 *
 * Tag en backup af nh_hubs først. Scriptet kan køres flere gange: rækker der
 * allerede er krypteret ("v2:") røres ikke.
 *
 * Bruger de samme DB_*-miljøvariabler og ENCRYPTION_KEY som serveren.
 */
"use strict";

const mysql = require("mysql2/promise");
require("dotenv").config();
const {
  encryptData,
  decryptData,
  assertKeyConfigured,
} = require("./neurohub_crypto");

const CTX = "nh_hubs.access_code";
const APPLY = process.argv.includes("--apply");

(async () => {
  assertKeyConfigured();
  const pool = await mysql.createPool({
    host: process.env.DB_HOST || "mysql48.unoeuro.com",
    port: parseInt(process.env.DB_PORT || "3306"),
    user: process.env.DB_USER || "jakobkall_com",
    password: process.env.DB_PASS,
    database: process.env.DB_NAME || "jakobkall_com_db_neurohub",
    charset: "utf8mb4",
  });

  // Krypteret kode er længere end klartekst → kolonnen skal være bred nok
  const [cols] = await pool.query(
    "SELECT DATA_TYPE AS t, CHARACTER_MAXIMUM_LENGTH AS len FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'nh_hubs' AND COLUMN_NAME = 'access_code'",
  );
  if (!cols.length) throw new Error("nh_hubs.access_code findes ikke");
  const { t, len } = cols[0];
  const isText = ["text", "mediumtext", "longtext"].includes(
    String(t).toLowerCase(),
  );
  const tooNarrow = !isText && Number(len) < 255;
  if (tooNarrow) {
    console.log(`Kolonnen er ${t}(${len}) — skal udvides til VARCHAR(255).`);
    if (APPLY) {
      await pool.query(
        "ALTER TABLE nh_hubs MODIFY COLUMN access_code VARCHAR(255) NULL",
      );
      console.log("→ udvidet til VARCHAR(255)");
    }
  }

  const [rows] = await pool.query(
    "SELECT id, access_code FROM nh_hubs WHERE access_code IS NOT NULL AND access_code <> '' AND access_code NOT LIKE 'v2:%'",
  );
  console.log(
    `${rows.length} hub(s) med klartekst-kode.${APPLY ? "" : "  (TØR-KØRSEL — intet gemmes)"}`,
  );

  let done = 0;
  for (const r of rows) {
    const enc = encryptData(r.access_code, CTX);
    if (decryptData(enc, CTX) !== r.access_code) {
      console.error(`  ✗ ${r.id}: rundtur-test fejlede, springes over`);
      continue;
    }
    console.log(`  ${APPLY ? "✓" : "·"} ${r.id}`); // koden skrives bevidst ikke i loggen
    if (APPLY) {
      await pool.execute("UPDATE nh_hubs SET access_code = ? WHERE id = ?", [
        enc,
        r.id,
      ]);
      done++;
    }
  }
  console.log(
    APPLY
      ? `Færdig: ${done}/${rows.length} krypteret.`
      : "Kør igen med --apply for at gemme.",
  );
  await pool.end();
})().catch((e) => {
  console.error("Fejl:", e.message);
  process.exit(1);
});
