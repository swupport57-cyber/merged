import { createClient } from '@whatsmeow-node/whatsmeow-node';
import { TelegramClient, Api } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { CustomFile } from 'telegram/client/uploads.js';
import { computeCheck } from 'telegram/Password.js';
import { execFile } from 'child_process';
import { promisify } from 'util';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import http from 'http';
import crypto from 'crypto';
import pg from 'pg';

const execFileAsync = promisify(execFile);
const { Pool } = pg;

// =====================================================================
// config
// =====================================================================
const PORT = process.env.PORT || 3000;
const APP_BASE_URL = process.env.APP_BASE_URL;
const BRIDGE_SECRET = process.env.BRIDGE_SECRET;

// WhatsApp store. sqlite (default) needs a persistent disk at DATA_DIR.
// postgres needs SUPABASE_DB_URL = DIRECT or SESSION-pooler string (port 5432, NOT 6543).
const WA_STORE_MODE = (process.env.STORE_MODE || 'sqlite').toLowerCase();
const DATA_DIR = process.env.DATA_DIR || '/data';

// Telegram (enabled only when all three are set)
const TG_API_ID = parseInt(process.env.TG_API_ID, 10);
const TG_API_HASH = process.env.TG_API_HASH;
const SUPABASE_DB_URL = process.env.SUPABASE_DB_URL;
const TG_ENABLED = !!(TG_API_ID && TG_API_HASH && SUPABASE_DB_URL);

const INIT_TIMEOUT = 25000;
const CONNECT_TIMEOUT = 20000;
const PAIR_TIMEOUT = 20000;
const LOGIN_TTL_MS = 10 * 60 * 1000;
const MAX_JSON_BODY = 1_000_000;
const MAX_VIDEO_BODY = 100 * 1024 * 1024;

if (!APP_BASE_URL || !BRIDGE_SECRET) {
  console.error('Missing required env vars: APP_BASE_URL, BRIDGE_SECRET');
  process.exit(1);
}
if (WA_STORE_MODE === 'postgres' && !SUPABASE_DB_URL) {
  console.error('STORE_MODE=postgres requires SUPABASE_DB_URL');
  process.exit(1);
}
if (!TG_ENABLED) console.warn('Telegram disabled (need TG_API_ID, TG_API_HASH, SUPABASE_DB_URL)');

const pool = SUPABASE_DB_URL
  ? new Pool({ connectionString: SUPABASE_DB_URL, connectionTimeoutMillis: 10000 })
  : null;
pool?.on('error', (e) => console.error('pg pool error:', e.message));

// =====================================================================
// shared helpers
// =====================================================================
class HttpError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

const safeId = (userId) =>
  String(userId).toLowerCase().replace(/[^a-z0-9_]/g, '_').slice(0, 40) || 'default';

function withTimeout(promise, ms, label) {
  let t;
  const timeout = new Promise((_, rej) => {
    t = setTimeout(() => rej(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}

function safeEqual(a, b) {
  const A = Buffer.from(String(a || ''));
  const B = Buffer.from(String(b || ''));
  return A.length === B.length && crypto.timingSafeEqual(A, B);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function json(res, code, body) {
  if (res.headersSent) return;
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

function parseJson(buf) {
  try { return JSON.parse(buf.toString('utf8') || '{}'); }
  catch { throw new HttpError(400, 'Invalid JSON body'); }
}

async function readBody(req, limit) {
  const chunks = [];
  let size = 0;
  for await (const c of req) {
    size += c.length;
    if (size > limit) throw new HttpError(413, 'Body too large');
    chunks.push(c);
  }
  return Buffer.concat(chunks);
}

async function fetchResultAudio(resultId) {
  const r = await fetch(`${APP_BASE_URL}/api/result/${encodeURIComponent(resultId)}`, {
    signal: AbortSignal.timeout(30000),
  });
  if (!r.ok) throw new HttpError(502, `Audio fetch failed: ${r.status}`);
  return Buffer.from(await r.arrayBuffer());
}

function tmpPaths(prefix, inExt, outExt) {
  const stamp = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  return {
    inPath: path.join(os.tmpdir(), `${prefix}-in-${stamp}${inExt}`),
    outPath: path.join(os.tmpdir(), `${prefix}-out-${stamp}${outExt}`),
  };
}

// audio -> OGG/Opus/16k/mono (voice note format for WhatsApp and Telegram)
async function toOggOpus(buf, prefix) {
  const { inPath, outPath } = tmpPaths(prefix, '.in', '.ogg');
  const cleanup = async () => {
    await fs.unlink(inPath).catch(() => {});
    await fs.unlink(outPath).catch(() => {});
  };
  await fs.writeFile(inPath, buf);
  try {
    await execFileAsync('ffmpeg', [
      '-y', '-i', inPath, '-vn',
      '-c:a', 'libopus', '-b:a', '32k', '-ar', '16000', '-ac', '1',
      '-application', 'voip', '-avoid_negative_ts', 'make_zero', '-map_metadata', '-1',
      outPath,
    ], { timeout: 30000 });
  } catch (e) {
    await cleanup();
    throw new HttpError(500, `Audio conversion failed: ${String(e.stderr || e.message).slice(-300)}`);
  }
  return { outPath, cleanup };
}

// =====================================================================
// WHATSAPP
// =====================================================================
async function waStoreUrl(key) {
  if (WA_STORE_MODE === 'sqlite') {
    await fs.mkdir(DATA_DIR, { recursive: true });
    return `file:${path.join(DATA_DIR, `wa_${key}.db`)}?_foreign_keys=on`;
  }
  const schema = `wa_${key}`; // key is whitelisted chars only
  await withTimeout(pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`), 10000, 'create schema');
  const u = new URL(SUPABASE_DB_URL);
  u.searchParams.set('sslmode', 'require');
  u.searchParams.set('connect_timeout', '10');
  u.searchParams.set('options', `-csearch_path=${schema}`);
  return u.toString();
}

async function waListStored() {
  try {
    if (WA_STORE_MODE === 'sqlite') {
      const files = await fs.readdir(DATA_DIR).catch(() => []);
      return files.filter((f) => /^wa_.+\.db$/.test(f)).map((f) => f.slice(3, -3));
    }
    const r = await pool.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'wa\\_%'`
    );
    return r.rows.map((x) => x.schema_name.slice(3));
  } catch (e) {
    console.error('waListStored failed:', e.message);
    return [];
  }
}

const waClients = new Map(); // key -> entry (fully initialised only)
const waPending = new Map(); // key -> Promise (dedupes concurrent init)

async function waGetOrCreate(userId) {
  const key = safeId(userId); // ALWAYS key by sanitized id (one client per store file)
  if (waClients.has(key)) return waClients.get(key);
  if (waPending.has(key)) return waPending.get(key);

  const p = (async () => {
    console.log(`[wa ${key}] preparing store (${WA_STORE_MODE})...`);
    const client = createClient({ store: await waStoreUrl(key), commandTimeout: 30000 });
    const entry = { client, key, jid: null, connected: false };

    client.on('connected', (ev) => {
      entry.jid = ev?.jid || entry.jid;
      entry.connected = true;
      console.log(`[wa ${key}] CONNECTED jid=${entry.jid}`);
    });
    client.on('disconnected', () => { entry.connected = false; console.log(`[wa ${key}] disconnected`); });
    client.on('pair_success', (ev) => {
      entry.jid = ev?.jid || entry.jid;
      console.log(`[wa ${key}] PAIR SUCCESS jid=${entry.jid}`);
    });
    client.on('logged_out', () => {
      entry.connected = false;
      entry.jid = null;
      console.log(`[wa ${key}] logged out from phone`);
      waClients.delete(key);
    });
    client.on('error', (err) => console.error(`[wa ${key}] error:`, err?.message || err));

    try {
      console.log(`[wa ${key}] init...`);
      const info = await withTimeout(client.init(), INIT_TIMEOUT, 'client.init');
      if (info?.jid) entry.jid = info.jid;
      console.log(`[wa ${key}] init ok jid=${entry.jid || 'none (unpaired)'}`);
    } catch (e) {
      try { await client.close?.(); } catch {}
      throw e; // never cache a broken client
    }
    waClients.set(key, entry);
    return entry;
  })().finally(() => waPending.delete(key));

  waPending.set(key, p);
  return p;
}

async function waEnsureConnected(entry) {
  if (entry.connected) return;
  try {
    await withTimeout(entry.client.connect(), CONNECT_TIMEOUT, 'connect');
  } catch (e) {
    if (!/already connected/i.test(String(e?.message))) throw e;
  }
  for (let i = 0; i < 20 && !entry.connected && entry.jid; i++) await sleep(250);
}

async function waRestoreAll() {
  const keys = await waListStored();
  console.log(`[wa] restoring ${keys.length} stored session(s)`);
  for (const key of keys) {
    try {
      const entry = await waGetOrCreate(key);
      if (entry.jid) await waEnsureConnected(entry);
    } catch (e) {
      console.error(`[wa ${key}] restore failed:`, e.message);
    }
  }
}

async function waPair(ctx) {
  const { userId, phone } = parseJson(ctx.body);
  if (!userId || !phone) throw new HttpError(400, 'userId and phone required');
  const cleanPhone = String(phone).replace(/[^0-9]/g, '');
  if (cleanPhone.length < 8) throw new HttpError(400, 'Phone must include country code, digits only');

  const t0 = Date.now();
  const log = (m) => console.log(`[wa pair ${safeId(userId)}] +${Date.now() - t0}ms ${m}`);

  log('getOrCreate...');
  const entry = await waGetOrCreate(userId);
  log('store opened');

  if (entry.jid) {
    await waEnsureConnected(entry);
    if (entry.connected) {
      log('already paired');
      return json(ctx.res, 200, { ok: true, alreadyPaired: true, jid: entry.jid });
    }
  }

  try {
    await withTimeout(entry.client.connect(), CONNECT_TIMEOUT, 'connect');
  } catch (e) {
    if (!/already connected/i.test(String(e?.message))) throw e;
  }
  log('websocket connected');

  const code = await withTimeout(entry.client.pairCode(cleanPhone), PAIR_TIMEOUT, 'pairCode');
  log(`got code ${code}`);
  json(ctx.res, 200, { ok: true, code });
}

async function waStatus(ctx) {
  const userId = ctx.url.searchParams.get('userId');
  if (!userId) throw new HttpError(400, 'userId required');
  const key = safeId(userId);

  let entry = waClients.get(key);
  if (!entry) {
    const stored = await waListStored();
    if (stored.includes(key)) entry = await waGetOrCreate(userId).catch(() => null);
  }
  if (entry?.jid && !entry.connected) await waEnsureConnected(entry).catch(() => {});

  json(ctx.res, 200, {
    ok: true,
    connected: !!entry?.connected,
    paired: !!entry?.jid,
    jid: entry?.jid || null,
  });
}

async function waSend(ctx) {
  const { userId, phone, resultId } = parseJson(ctx.body);
  if (!userId || !phone || !resultId) throw new HttpError(400, 'userId, phone, resultId required');

  const entry = await waGetOrCreate(userId);
  if (!entry.jid) throw new HttpError(409, 'User not paired');
  await waEnsureConnected(entry);
  if (!entry.connected) throw new HttpError(409, 'WhatsApp not connected yet, retry shortly');

  const to = String(phone).replace(/[^0-9]/g, '') + '@s.whatsapp.net';
  const { outPath, cleanup } = await toOggOpus(await fetchResultAudio(resultId), 'wa');
  try {
    const bytes = (await fs.stat(outPath)).size;
    const media = await entry.client.uploadMedia(outPath, 'audio');
    const base = {
      directPath: media.directPath,
      mediaKey: media.mediaKey,
      fileEncSHA256: media.fileEncSHA256,
      fileSHA256: media.fileSHA256,
      fileLength: String(media.fileLength),
      mimetype: 'audio/ogg; codecs=opus',
    };
    const mediaUrl = media.URL ?? media.url;
    // Protobuf JSON names are URL / PTT (uppercase); lowercase kept as fallback.
    const variants = [
      { ...base, URL: mediaUrl, PTT: true },
      { ...base, url: mediaUrl, ptt: true },
    ];
    let lastErr;
    for (const audioMessage of variants) {
      try {
        await entry.client.sendRawMessage(to, { audioMessage });
        lastErr = null;
        break;
      } catch (e) {
        lastErr = e;
        if (!/unknown field/i.test(String(e?.message))) break;
        console.warn(`[wa ${entry.key}] proto field mismatch, trying next variant: ${e.message}`);
      }
    }
    if (lastErr) throw lastErr;
    json(ctx.res, 200, { ok: true, to, bytes });
  } finally {
    await cleanup();
  }
}

async function waLogout(ctx) {
  const { userId } = parseJson(ctx.body);
  if (!userId) throw new HttpError(400, 'userId required');
  const key = safeId(userId);
  const entry = waClients.get(key);
  if (entry) {
    try { await entry.client.logout?.(); } catch {}
    try { await entry.client.close?.(); } catch {}
    waClients.delete(key);
  }
  json(ctx.res, 200, { ok: true });
}

// =====================================================================
// TELEGRAM
// =====================================================================
const tgClients = new Map();       // userId -> { client, connected }
const tgRestoring = new Map();     // userId -> Promise
const tgPendingLogins = new Map(); // userId -> { client, phone, phoneCodeHash, createdAt }
const tgSchemasReady = new Set();

const tgSchemaFor = (userId) => `tg_${safeId(userId)}`;

async function tgEnsureSchema(userId) {
  const schema = tgSchemaFor(userId);
  if (tgSchemasReady.has(schema)) return schema;
  await pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${schema}.sessions (
      user_id TEXT PRIMARY KEY,
      session_string TEXT NOT NULL,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  tgSchemasReady.add(schema);
  return schema;
}

async function tgGetSession(userId) {
  const schema = await tgEnsureSchema(userId);
  const r = await pool.query(`SELECT session_string FROM ${schema}.sessions WHERE user_id = $1`, [userId]);
  return r.rows[0]?.session_string || '';
}

async function tgSaveSession(userId, sessionString) {
  const schema = await tgEnsureSchema(userId);
  await pool.query(
    `INSERT INTO ${schema}.sessions (user_id, session_string, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (user_id) DO UPDATE SET session_string = $2, updated_at = NOW()`,
    [userId, sessionString]
  );
}

async function tgListStoredUserIds() {
  try {
    const schemas = await pool.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'tg\\_%'`
    );
    const ids = [];
    for (const { schema_name } of schemas.rows) {
      try {
        const r = await pool.query(`SELECT user_id FROM "${schema_name}".sessions`);
        for (const row of r.rows) ids.push(row.user_id);
      } catch { /* schema without sessions table */ }
    }
    return ids;
  } catch (e) {
    console.error('tgListStoredUserIds failed:', e.message);
    return [];
  }
}

function newTgClient(sessionString = '') {
  const client = new TelegramClient(new StringSession(sessionString), TG_API_ID, TG_API_HASH, {
    connectionRetries: 5,
    useWSS: false,
  });
  client.setLogLevel?.('error');
  return client;
}

async function tgDispose(client) {
  try { await client.disconnect(); } catch {}
  try { await client.destroy?.(); } catch {}
}

// Restore a logged-in client from its saved session. Returns null if none saved.
async function tgRestore(userId) {
  if (tgClients.has(userId)) return tgClients.get(userId);
  if (tgRestoring.has(userId)) return tgRestoring.get(userId);

  const p = (async () => {
    const sessionString = await tgGetSession(userId);
    if (!sessionString) return null;

    const client = newTgClient(sessionString);
    try {
      await withTimeout(client.connect(), CONNECT_TIMEOUT, 'telegram connect');
      if (!(await client.isUserAuthorized())) throw new Error('Saved session is no longer authorized');
    } catch (e) {
      await tgDispose(client);
      console.error(`[tg ${userId}] restore failed:`, e.message);
      throw new HttpError(401, 'Saved Telegram session is invalid. Please log in again.');
    }
    const entry = { client, connected: true };
    tgClients.set(userId, entry);
    console.log(`[tg ${userId}] restored from saved session`);
    return entry;
  })().finally(() => tgRestoring.delete(userId));

  tgRestoring.set(userId, p);
  return p;
}

async function tgActive(userId) {
  const entry = await tgRestore(userId);
  if (!entry) return null;
  if (!entry.client.connected) {
    try { await withTimeout(entry.client.connect(), CONNECT_TIMEOUT, 'telegram reconnect'); }
    catch (e) { throw new HttpError(503, `Telegram reconnect failed: ${e.message}`); }
  }
  return entry;
}

async function tgFinishLogin(userId, client) {
  await tgSaveSession(userId, client.session.save());
  const old = tgClients.get(userId);
  if (old && old.client !== client) await tgDispose(old.client);
  tgClients.set(userId, { client, connected: true });
  tgPendingLogins.delete(userId);
}

function tgMapError(err) {
  const m = err?.errorMessage || err?.message || String(err);
  if (/PHONE_NUMBER_INVALID/.test(m)) return new HttpError(400, 'Invalid phone number (use international format).');
  if (/PHONE_CODE_INVALID/.test(m)) return new HttpError(400, 'Invalid code. Try again.');
  if (/PHONE_CODE_EXPIRED/.test(m)) return new HttpError(400, 'Code expired. Start over.');
  if (/PASSWORD_HASH_INVALID/.test(m)) return new HttpError(400, 'Wrong password.');
  if (/FLOOD/.test(m)) return new HttpError(429, `Too many attempts, wait and retry. (${m})`);
  if (/PHONE_NUMBER_BANNED/.test(m)) return new HttpError(403, 'This phone number is banned by Telegram.');
  return new HttpError(500, m);
}

function requireTg() {
  if (!TG_ENABLED) throw new HttpError(503, 'Telegram is not configured on this server');
}

async function tgStartLogin(ctx) {
  requireTg();
  const { userId, phone } = parseJson(ctx.body);
  if (!userId || !phone) throw new HttpError(400, 'userId and phone required');
  const cleanPhone = String(phone).replace(/[^0-9+]/g, '');
  if (cleanPhone.replace(/\D/g, '').length < 8) throw new HttpError(400, 'Invalid phone number');

  const old = tgPendingLogins.get(userId);
  if (old) { tgPendingLogins.delete(userId); await tgDispose(old.client); }

  const client = newTgClient('');
  try {
    await withTimeout(client.connect(), CONNECT_TIMEOUT, 'telegram connect');
    const result = await client.invoke(new Api.auth.SendCode({
      phoneNumber: cleanPhone,
      apiId: TG_API_ID,
      apiHash: TG_API_HASH,
      settings: new Api.CodeSettings({}),
    }));
    tgPendingLogins.set(userId, { client, phone: cleanPhone, phoneCodeHash: result.phoneCodeHash, createdAt: Date.now() });
  } catch (e) {
    await tgDispose(client);
    throw tgMapError(e);
  }
  json(ctx.res, 200, { ok: true, message: 'Code sent. Check your Telegram app.' });
}

async function tgVerify(ctx) {
  requireTg();
  const { userId, code } = parseJson(ctx.body);
  if (!userId || !code) throw new HttpError(400, 'userId and code required');
  const pending = tgPendingLogins.get(userId);
  if (!pending) throw new HttpError(400, 'No pending login. Start over.');

  try {
    await pending.client.invoke(new Api.auth.SignIn({
      phoneNumber: pending.phone,
      phoneCodeHash: pending.phoneCodeHash,
      phoneCode: String(code).replace(/\s/g, ''),
    }));
    await tgFinishLogin(userId, pending.client);
    json(ctx.res, 200, { ok: true, message: 'Logged in successfully.' });
  } catch (err) {
    const m = err?.errorMessage || err?.message || '';
    if (m.includes('SESSION_PASSWORD_NEEDED')) {
      return json(ctx.res, 200, { ok: true, passwordNeeded: true, message: '2FA password required.' });
    }
    if (/PHONE_CODE_EXPIRED/.test(m)) {
      tgPendingLogins.delete(userId);
      await tgDispose(pending.client);
    }
    throw tgMapError(err);
  }
}

async function tgVerifyPassword(ctx) {
  requireTg();
  const { userId, password } = parseJson(ctx.body);
  if (!userId || !password) throw new HttpError(400, 'userId and password required');
  const pending = tgPendingLogins.get(userId);
  if (!pending) throw new HttpError(400, 'No pending login. Start over.');

  try {
    const pwd = await pending.client.invoke(new Api.account.GetPassword());
    const check = await computeCheck(pwd, password);
    await pending.client.invoke(new Api.auth.CheckPassword({ password: check }));
    await tgFinishLogin(userId, pending.client);
    json(ctx.res, 200, { ok: true, message: 'Logged in successfully.' });
  } catch (err) {
    throw tgMapError(err);
  }
}

async function tgStatus(ctx) {
  requireTg();
  const userId = ctx.url.searchParams.get('userId');
  if (!userId) throw new HttpError(400, 'userId required');

  let hasSaved = false;
  try { hasSaved = !!(await tgGetSession(userId)); } catch {}

  let entry = tgClients.get(userId);
  if (!entry && hasSaved) entry = await tgRestore(userId).catch(() => null);

  json(ctx.res, 200, { ok: true, connected: !!entry?.connected, hasSavedSession: hasSaved });
}

async function tgSend(ctx) {
  requireTg();
  const { userId, to, resultId } = parseJson(ctx.body);
  if (!userId || !to || !resultId) throw new HttpError(400, 'userId, to, resultId required');

  const entry = await tgActive(userId);
  if (!entry) throw new HttpError(409, 'Not logged in. Please log in first.');

  const { outPath, cleanup } = await toOggOpus(await fetchResultAudio(resultId), 'tg');
  try {
    const entity = await entry.client.getEntity(to);
    const stat = await fs.stat(outPath);
    const file = new CustomFile(path.basename(outPath), stat.size, outPath);
    const media = await entry.client.uploadFile({ file, workers: 1 });
    // /tg/send is audio only. Video notes go through /tg/send-video.
    await entry.client.sendFile(entity, { file: media, voiceNote: true });
    json(ctx.res, 200, { ok: true, to, mode: 'voice' });
  } finally {
    await cleanup();
  }
}

function parseMultipart(buf, contentType) {
  const m = /boundary=(?:"([^"]+)"|([^;]+))/i.exec(contentType || '');
  if (!m) throw new HttpError(400, 'multipart/form-data required');
  const boundary = Buffer.from('--' + (m[1] || m[2]).trim());
  const fields = {};
  const files = {};

  let pos = buf.indexOf(boundary);
  while (pos !== -1) {
    let start = pos + boundary.length;
    if (buf[start] === 0x2d && buf[start + 1] === 0x2d) break; // closing boundary
    if (buf[start] === 0x0d && buf[start + 1] === 0x0a) start += 2;
    const next = buf.indexOf(boundary, start);
    if (next === -1) break;
    const part = buf.subarray(start, Math.max(start, next - 2)); // strip CRLF before boundary
    const he = part.indexOf('\r\n\r\n');
    if (he !== -1) {
      const head = part.subarray(0, he).toString('utf8');
      const data = part.subarray(he + 4);
      const name = /name="([^"]*)"/i.exec(head)?.[1];
      const filename = /filename="([^"]*)"/i.exec(head)?.[1];
      if (name) {
        if (filename !== undefined) files[name] = { filename, data };
        else fields[name] = data.toString('utf8').trim();
      }
    }
    pos = next;
  }
  return { fields, files };
}

async function tgSendVideo(ctx) {
  requireTg();
  const { fields, files } = parseMultipart(ctx.body, ctx.req.headers['content-type']);
  const video = files.video;
  if (!video?.data?.length || !fields.userId || !fields.to) {
    throw new HttpError(400, 'video, userId, to required');
  }

  const entry = await tgActive(fields.userId);
  if (!entry) throw new HttpError(409, 'Not logged in.');

  // Input and output MUST have different names, or ffmpeg refuses in-place edits.
  const ext = (path.extname(video.filename).replace(/[^.a-z0-9]/gi, '').slice(0, 8)) || '.mp4';
  const { inPath, outPath } = tmpPaths('tgvid', ext, '.mp4');
  await fs.writeFile(inPath, video.data);

  try {
    try {
      await execFileAsync('ffmpeg', [
        '-y', '-i', inPath,
        '-t', '60',
        '-vf', 'scale=480:480:force_original_aspect_ratio=increase,crop=480:480',
        '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '28', '-pix_fmt', 'yuv420p',
        '-c:a', 'aac', '-b:a', '64k',
        '-movflags', '+faststart',
        outPath,
      ], { timeout: 180000 });
    } catch (e) {
      throw new HttpError(500, `Video conversion failed: ${String(e.stderr || e.message).slice(-300)}`);
    }

    const entity = await entry.client.getEntity(fields.to);
    const stat = await fs.stat(outPath);
    const file = new CustomFile(path.basename(outPath), stat.size, outPath);
    const media = await entry.client.uploadFile({ file, workers: 1 });
    await entry.client.sendFile(entity, { file: media, videoNote: true });
    json(ctx.res, 200, { ok: true, to: fields.to, bytes: stat.size });
  } finally {
    await fs.unlink(inPath).catch(() => {});
    await fs.unlink(outPath).catch(() => {});
  }
}

async function tgRestoreAll() {
  if (!TG_ENABLED) return;
  const ids = await tgListStoredUserIds();
  console.log(`[tg] restoring ${ids.length} stored session(s)`);
  for (const id of ids) {
    try { await tgRestore(id); } catch { /* already logged */ }
  }
}

// drop abandoned login attempts so sockets don't pile up
setInterval(() => {
  const now = Date.now();
  for (const [userId, p] of tgPendingLogins) {
    if (now - p.createdAt > LOGIN_TTL_MS) {
      tgPendingLogins.delete(userId);
      tgDispose(p.client);
    }
  }
}, 60_000).unref();

// =====================================================================
// router
// =====================================================================
const routes = {
  // WhatsApp
  'POST /pair':   { limit: MAX_JSON_BODY, fn: waPair },
  'GET /status':  { limit: 0,             fn: waStatus },
  'POST /send':   { limit: MAX_JSON_BODY, fn: waSend },
  'POST /logout': { limit: MAX_JSON_BODY, fn: waLogout },
  // Telegram
  'POST /tg/start-login':     { limit: MAX_JSON_BODY,  fn: tgStartLogin },
  'POST /tg/verify':          { limit: MAX_JSON_BODY,  fn: tgVerify },
  'POST /tg/verify-password': { limit: MAX_JSON_BODY,  fn: tgVerifyPassword },
  'GET /tg/status':           { limit: 0,              fn: tgStatus },
  'POST /tg/send':            { limit: MAX_JSON_BODY,  fn: tgSend },
  'POST /tg/send-video':      { limit: MAX_VIDEO_BODY, fn: tgSendVideo },
};

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Bridge-Secret');

  if (req.method === 'OPTIONS') { res.writeHead(204).end(); return; }

  try {
    const url = new URL(req.url, `http://localhost:${PORT}`);

    if (req.method === 'GET' && url.pathname === '/health') {
      return json(res, 200, { ok: true, wa: waClients.size, tg: tgClients.size, tgEnabled: TG_ENABLED });
    }

    if (!safeEqual(req.headers['x-bridge-secret'], BRIDGE_SECRET)) {
      return json(res, 401, { ok: false, error: 'Unauthorized' });
    }

    const route = routes[`${req.method} ${url.pathname}`];
    if (!route) return json(res, 404, { ok: false, error: 'not found' });

    const body = route.limit ? await readBody(req, route.limit) : Buffer.alloc(0);
    await route.fn({ req, res, url, body });
  } catch (err) {
    const status = err instanceof HttpError ? err.status : 500;
    if (status >= 500) console.error('request failed:', err);
    json(res, status, { ok: false, error: err?.message || String(err) });
  }
});

server.requestTimeout = 10 * 60 * 1000;

server.listen(PORT, () => {
  console.log(`Bridge on :${PORT} (wa store=${WA_STORE_MODE}, telegram=${TG_ENABLED ? 'on' : 'off'})`);
  waRestoreAll().catch((e) => console.error('waRestoreAll error:', e));
  tgRestoreAll().catch((e) => console.error('tgRestoreAll error:', e));
});

process.on('unhandledRejection', (e) => console.error('unhandledRejection:', e));

async function shutdown() {
  for (const { client } of waClients.values()) { try { await client.close?.(); } catch {} }
  for (const { client } of tgClients.values()) { await tgDispose(client); }
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
