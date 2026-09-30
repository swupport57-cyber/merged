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
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*'; // set to your app's origin in production

const WA_STORE_MODE = (process.env.STORE_MODE || 'sqlite').toLowerCase();
const DATA_DIR = process.env.DATA_DIR || '/data';

const TG_API_ID = parseInt(process.env.TG_API_ID, 10);
const TG_API_HASH = process.env.TG_API_HASH;
const SUPABASE_DB_URL = process.env.SUPABASE_DB_URL;
const TG_ENABLED = !!(TG_API_ID && TG_API_HASH && SUPABASE_DB_URL);

// Set PURGE_LEGACY=true ONCE to delete all old userId-keyed sessions (wa_* / tg_* not owned by a device).
const PURGE_LEGACY = String(process.env.PURGE_LEGACY || '').toLowerCase() === 'true';

const INIT_TIMEOUT = 25000;
const CONNECT_TIMEOUT = 20000;
const PAIR_TIMEOUT = 20000;
const LOGIN_TTL_MS = 10 * 60 * 1000;
const PENDING_DEVICE_TTL_MS = 30 * 60 * 1000;
const MAX_PENDING_DEVICES = 200;
const MAX_JSON_BODY = 1_000_000;
const MAX_VIDEO_BODY = 100 * 1024 * 1024;
const DEVICE_HEADER = 'x-device-token';

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
  constructor(status, message, extra = {}) { super(message); this.status = status; this.extra = extra; }
}
const reauth = (reason = 'unknown') => new HttpError(
  401,
  reason === 'missing_token'
    ? 'Please sign in again on this device. (the server did not receive your device token)'
    : 'Please sign in again on this device.',
  { reauth: true, reason }
);

// Token can come from the X-Device-Token header, "Authorization: Bearer", or a JSON body field "deviceToken".
function extractToken(req, body) {
  const h = req.headers[DEVICE_HEADER];
  if (typeof h === 'string' && h) return h;
  const auth = req.headers.authorization;
  if (typeof auth === 'string' && auth.startsWith('Bearer ')) return auth.slice(7).trim();
  const ct = String(req.headers['content-type'] || '');
  if (body?.length && ct.includes('application/json')) {
    try {
      const t = JSON.parse(body.toString('utf8')).deviceToken;
      if (typeof t === 'string' && t) return t;
    } catch { /* ignore */ }
  }
  return null;
}

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
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
const short = (sid) => String(sid).slice(0, 8);

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
// DEVICE REGISTRY  (who owns which session)
//
// Identity comes from a random secret token issued by THIS server to the
// device that started the sign-in. The client-supplied userId is ignored.
// Only a hash of the token is stored. No token / unknown token => sign in again.
// =====================================================================
const devices = new Map(); // sid -> { sid, platform, tokenHash, verified, createdAt, lastSeen }
const byHash = new Map();  // tokenHash -> sid
const DEVICES_FILE = path.join(DATA_DIR, 'devices.json');
let fileWriteChain = Promise.resolve();

async function registryLoad() {
  let rows = [];
  if (pool) {
    await pool.query(`CREATE TABLE IF NOT EXISTS bridge_devices (
      sid TEXT PRIMARY KEY, platform TEXT NOT NULL, token_hash TEXT UNIQUE NOT NULL,
      verified BOOLEAN NOT NULL DEFAULT FALSE, created_at BIGINT NOT NULL, last_seen BIGINT NOT NULL)`);
    const r = await pool.query(
      `SELECT sid, platform, token_hash AS "tokenHash", verified, created_at AS "createdAt", last_seen AS "lastSeen" FROM bridge_devices`
    );
    rows = r.rows.map((x) => ({ ...x, createdAt: Number(x.createdAt), lastSeen: Number(x.lastSeen) }));
  } else {
    await fs.mkdir(DATA_DIR, { recursive: true });
    rows = JSON.parse(await fs.readFile(DEVICES_FILE, 'utf8').catch(() => '[]'));
  }
  for (const rec of rows) { devices.set(rec.sid, rec); byHash.set(rec.tokenHash, rec.sid); }
  console.log(`[registry] loaded ${devices.size} device(s) (${pool ? 'postgres' : 'file'})`);
}

function registryPersistFile() {
  fileWriteChain = fileWriteChain.then(async () => {
    const tmp = `${DEVICES_FILE}.tmp`;
    await fs.writeFile(tmp, JSON.stringify([...devices.values()]));
    await fs.rename(tmp, DEVICES_FILE);
  }).catch((e) => console.error('registry file write failed:', e.message));
  return fileWriteChain;
}

async function registrySave(rec) {
  if (pool) {
    await pool.query(
      `INSERT INTO bridge_devices (sid, platform, token_hash, verified, created_at, last_seen)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (sid) DO UPDATE SET verified = $4, last_seen = $6`,
      [rec.sid, rec.platform, rec.tokenHash, rec.verified, rec.createdAt, rec.lastSeen]
    );
  } else {
    await registryPersistFile();
  }
}

async function registryDelete(sid) {
  if (pool) await pool.query(`DELETE FROM bridge_devices WHERE sid = $1`, [sid]);
  else await registryPersistFile();
}

async function issueDevice(platform) {
  const pending = [...devices.values()].filter((d) => !d.verified).length;
  if (pending >= MAX_PENDING_DEVICES) throw new HttpError(503, 'Too many sign-ins in progress, try again in a few minutes.');
  const token = crypto.randomBytes(32).toString('base64url');
  const rec = {
    sid: crypto.randomBytes(16).toString('hex'), // 32 hex chars, also used in store/schema names
    platform,
    tokenHash: sha(token),
    verified: false,
    createdAt: Date.now(),
    lastSeen: Date.now(),
  };
  devices.set(rec.sid, rec);
  byHash.set(rec.tokenHash, rec.sid);
  await registrySave(rec);
  return { token, rec };
}

function deviceFromReq(req, platform) {
  const token = req.deviceToken;
  if (typeof token !== 'string' || !token || token.length > 200) { req.authReason = 'missing_token'; return null; }
  const sid = byHash.get(sha(token));
  const rec = sid && devices.get(sid);
  if (!rec) { req.authReason = 'unknown_token'; return null; }
  if (rec.platform !== platform) { req.authReason = 'wrong_platform'; return null; }
  rec.lastSeen = Date.now();
  return rec;
}

const requireVerifiedDevice = (req, platform) => {
  const rec = deviceFromReq(req, platform);
  if (!rec) throw reauth(req.authReason);
  if (!rec.verified) throw reauth('not_verified');
  return rec;
};

async function markVerified(sid) {
  const rec = devices.get(sid);
  if (rec && !rec.verified) {
    rec.verified = true;
    await registrySave(rec).catch((e) => console.error('markVerified save failed:', e.message));
    console.log(`[registry] device ${short(sid)} verified`);
  }
}

// Fully remove a device: disconnect, delete its stored session, forget its token.
async function purgeDevice(sid) {
  const rec = devices.get(sid);
  if (!rec) return;
  devices.delete(sid);
  byHash.delete(rec.tokenHash);
  try {
    if (rec.platform === 'wa') {
      const e = waClients.get(sid);
      if (e) {
        try { await e.client.logout?.(); } catch {}
        try { await e.client.close?.(); } catch {}
        waClients.delete(sid);
      }
      await waDeleteStore(sid).catch((e2) => console.error('waDeleteStore failed:', e2.message));
    } else {
      const e = tgClients.get(sid);
      if (e) { await tgDispose(e.client); tgClients.delete(sid); }
      const p = tgPendingLogins.get(sid);
      if (p) { await tgDispose(p.client); tgPendingLogins.delete(sid); }
      await tgDropSchema(sid).catch((e2) => console.error('tgDropSchema failed:', e2.message));
    }
  } finally {
    await registryDelete(sid).catch((e) => console.error('registryDelete failed:', e.message));
  }
}

// =====================================================================
// WHATSAPP  (keyed by sid)
// =====================================================================
async function waStoreUrl(sid) {
  if (WA_STORE_MODE === 'sqlite') {
    await fs.mkdir(DATA_DIR, { recursive: true });
    return `file:${path.join(DATA_DIR, `wa_${sid}.db`)}?_foreign_keys=on`;
  }
  const schema = `wa_${sid}`; // sid is hex, safe
  await withTimeout(pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`), 10000, 'create schema');
  const u = new URL(SUPABASE_DB_URL);
  u.searchParams.set('sslmode', 'require');
  u.searchParams.set('connect_timeout', '10');
  u.searchParams.set('options', `-csearch_path=${schema}`);
  return u.toString();
}

async function waDeleteStore(sid) {
  if (WA_STORE_MODE === 'sqlite') {
    for (const suffix of ['', '-wal', '-shm', '-journal']) {
      await fs.unlink(path.join(DATA_DIR, `wa_${sid}.db${suffix}`)).catch(() => {});
    }
  } else {
    await pool.query(`DROP SCHEMA IF EXISTS wa_${sid} CASCADE`);
  }
}

const waClients = new Map(); // sid -> entry
const waPending = new Map(); // sid -> Promise

async function waGetOrCreate(sid) {
  if (waClients.has(sid)) return waClients.get(sid);
  if (waPending.has(sid)) return waPending.get(sid);

  const p = (async () => {
    const tag = `wa ${short(sid)}`;
    console.log(`[${tag}] preparing store (${WA_STORE_MODE})...`);
    const client = createClient({ store: await waStoreUrl(sid), commandTimeout: 30000 });
    const entry = { client, sid, jid: null, connected: false };

    client.on('connected', (ev) => {
      entry.jid = ev?.jid || entry.jid;
      entry.connected = true;
      console.log(`[${tag}] CONNECTED`);
      markVerified(sid);
    });
    client.on('disconnected', () => { entry.connected = false; console.log(`[${tag}] disconnected`); });
    client.on('pair_success', (ev) => {
      entry.jid = ev?.jid || entry.jid;
      console.log(`[${tag}] PAIR SUCCESS`);
      markVerified(sid);
    });
    client.on('logged_out', () => {
      console.log(`[${tag}] logged out from phone, removing device`);
      entry.connected = false;
      entry.jid = null;
      waClients.delete(sid);
      purgeDevice(sid);
    });
    client.on('error', (err) => console.error(`[${tag}] error:`, err?.message || err));

    try {
      const info = await withTimeout(client.init(), INIT_TIMEOUT, 'client.init');
      if (info?.jid) entry.jid = info.jid;
      console.log(`[${tag}] init ok paired=${!!entry.jid}`);
    } catch (e) {
      try { await client.close?.(); } catch {}
      throw e; // never cache a broken client
    }
    waClients.set(sid, entry);
    return entry;
  })().finally(() => waPending.delete(sid));

  waPending.set(sid, p);
  return p;
}

async function waEnsureConnected(entry, expectLogin = false) {
  if (entry.connected) return;
  try {
    await withTimeout(entry.client.connect(), CONNECT_TIMEOUT, 'connect');
  } catch (e) {
    if (!/already connected/i.test(String(e?.message))) throw e;
  }
  for (let i = 0; i < 24 && !entry.connected && (entry.jid || expectLogin); i++) await sleep(250);
}

async function waRestoreAll() {
  const sids = [...devices.values()].filter((d) => d.platform === 'wa' && d.verified).map((d) => d.sid);
  console.log(`[wa] restoring ${sids.length} device session(s)`);
  for (const sid of sids) {
    try {
      const entry = await waGetOrCreate(sid);
      await waEnsureConnected(entry, true);
    } catch (e) {
      console.error(`[wa ${short(sid)}] restore failed:`, e.message);
    }
  }
}

async function waPair(ctx) {
  const { phone } = parseJson(ctx.body); // any userId in the body is ignored on purpose
  const cleanPhone = String(phone || '').replace(/[^0-9]/g, '');
  if (cleanPhone.length < 8) throw new HttpError(400, 'Phone must include country code, digits only');

  let rec = deviceFromReq(ctx.req, 'wa');
  let newToken = null;
  if (!rec) {
    const issued = await issueDevice('wa');
    rec = issued.rec;
    newToken = issued.token;
  }

  const t0 = Date.now();
  const log = (m) => console.log(`[wa pair ${short(rec.sid)}] +${Date.now() - t0}ms ${m}`);

  try {
    log('getOrCreate...');
    const entry = await waGetOrCreate(rec.sid);
    log('store opened');

    if (rec.verified) {
      await waEnsureConnected(entry, true);
      if (entry.connected) {
        log('already paired');
        return json(ctx.res, 200, { ok: true, alreadyPaired: true, jid: entry.jid, ...(newToken && { deviceToken: newToken }) });
      }
    }

    try {
      await withTimeout(entry.client.connect(), CONNECT_TIMEOUT, 'connect');
    } catch (e) {
      if (!/already connected/i.test(String(e?.message))) throw e;
    }
    log('websocket connected');

    const code = await withTimeout(entry.client.pairCode(cleanPhone), PAIR_TIMEOUT, 'pairCode');
    log('got code');
    json(ctx.res, 200, { ok: true, code, ...(newToken && { deviceToken: newToken }) });
  } catch (e) {
    if (newToken) await purgeDevice(rec.sid); // don't leave orphan sessions behind
    throw e;
  }
}

async function waStatus(ctx) {
  const rec = deviceFromReq(ctx.req, 'wa');
  if (!rec) return json(ctx.res, 200, { ok: true, connected: false, paired: false, signInRequired: true, reason: ctx.req.authReason });

  let entry = waClients.get(rec.sid);
  if (!entry && rec.verified) entry = await waGetOrCreate(rec.sid).catch(() => null);
  if (entry && (entry.jid || rec.verified) && !entry.connected) await waEnsureConnected(entry, rec.verified).catch(() => {});
  if (entry?.connected && !rec.verified) await markVerified(rec.sid);

  json(ctx.res, 200, {
    ok: true,
    connected: !!entry?.connected,
    paired: rec.verified && (!!entry?.jid || !!entry?.connected),
    jid: rec.verified ? entry?.jid || null : null,
    signInRequired: false,
  });
}

async function waSend(ctx) {
  const rec = requireVerifiedDevice(ctx.req, 'wa');
  const { phone, resultId } = parseJson(ctx.body);
  if (!phone || !resultId) throw new HttpError(400, 'phone, resultId required');

  const entry = await waGetOrCreate(rec.sid);
  await waEnsureConnected(entry, true);
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
        console.warn(`[wa ${short(rec.sid)}] proto field mismatch, trying next variant`);
      }
    }
    if (lastErr) throw lastErr;
    json(ctx.res, 200, { ok: true, to, bytes });
  } finally {
    await cleanup();
  }
}

async function waLogout(ctx) {
  const rec = deviceFromReq(ctx.req, 'wa');
  if (rec) await purgeDevice(rec.sid);
  json(ctx.res, 200, { ok: true });
}

// =====================================================================
// TELEGRAM  (keyed by sid)
// =====================================================================
const tgClients = new Map();       // sid -> { client, connected }
const tgRestoring = new Map();     // sid -> Promise
const tgPendingLogins = new Map(); // sid -> { client, phone, phoneCodeHash, createdAt }
const tgSchemasReady = new Set();

const tgSchemaFor = (sid) => `tg_${sid}`;

async function tgEnsureSchema(sid) {
  const schema = tgSchemaFor(sid);
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

async function tgDropSchema(sid) {
  tgSchemasReady.delete(tgSchemaFor(sid));
  if (pool) await pool.query(`DROP SCHEMA IF EXISTS ${tgSchemaFor(sid)} CASCADE`);
}

async function tgGetSession(sid) {
  const schema = await tgEnsureSchema(sid);
  const r = await pool.query(`SELECT session_string FROM ${schema}.sessions WHERE user_id = $1`, [sid]);
  return r.rows[0]?.session_string || '';
}

async function tgSaveSession(sid, sessionString) {
  const schema = await tgEnsureSchema(sid);
  await pool.query(
    `INSERT INTO ${schema}.sessions (user_id, session_string, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (user_id) DO UPDATE SET session_string = $2, updated_at = NOW()`,
    [sid, sessionString]
  );
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

async function tgRestore(sid) {
  if (tgClients.has(sid)) return tgClients.get(sid);
  if (tgRestoring.has(sid)) return tgRestoring.get(sid);

  const p = (async () => {
    const sessionString = await tgGetSession(sid);
    if (!sessionString) return null;

    const client = newTgClient(sessionString);
    try {
      await withTimeout(client.connect(), CONNECT_TIMEOUT, 'telegram connect');
      if (!(await client.isUserAuthorized())) throw new Error('Saved session is no longer authorized');
    } catch (e) {
      await tgDispose(client);
      console.error(`[tg ${short(sid)}] restore failed:`, e.message);
      throw reauth();
    }
    const entry = { client, connected: true };
    tgClients.set(sid, entry);
    console.log(`[tg ${short(sid)}] restored from saved session`);
    return entry;
  })().finally(() => tgRestoring.delete(sid));

  tgRestoring.set(sid, p);
  return p;
}

async function tgActive(sid) {
  const entry = await tgRestore(sid);
  if (!entry) return null;
  if (!entry.client.connected) {
    try { await withTimeout(entry.client.connect(), CONNECT_TIMEOUT, 'telegram reconnect'); }
    catch (e) { throw new HttpError(503, `Telegram reconnect failed: ${e.message}`); }
  }
  return entry;
}

async function tgFinishLogin(sid, client) {
  await tgSaveSession(sid, client.session.save());
  const old = tgClients.get(sid);
  if (old && old.client !== client) await tgDispose(old.client);
  tgClients.set(sid, { client, connected: true });
  tgPendingLogins.delete(sid);
  await markVerified(sid);
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

// For verify steps: the device must hold the token that started this login.
function requirePendingLogin(req) {
  const rec = deviceFromReq(req, 'tg');
  if (!rec) throw reauth(req.authReason);
  const pending = tgPendingLogins.get(rec.sid);
  if (!pending) throw new HttpError(400, 'No pending login. Start over.');
  return { rec, pending };
}

async function tgStartLogin(ctx) {
  requireTg();
  const { phone } = parseJson(ctx.body); // any userId in the body is ignored on purpose
  const cleanPhone = String(phone || '').replace(/[^0-9+]/g, '');
  if (cleanPhone.replace(/\D/g, '').length < 8) throw new HttpError(400, 'Invalid phone number');

  let rec = deviceFromReq(ctx.req, 'tg');
  let newToken = null;
  if (!rec) {
    const issued = await issueDevice('tg');
    rec = issued.rec;
    newToken = issued.token;
  }

  const old = tgPendingLogins.get(rec.sid);
  if (old) { tgPendingLogins.delete(rec.sid); await tgDispose(old.client); }

  const client = newTgClient('');
  try {
    await withTimeout(client.connect(), CONNECT_TIMEOUT, 'telegram connect');
    const result = await client.invoke(new Api.auth.SendCode({
      phoneNumber: cleanPhone,
      apiId: TG_API_ID,
      apiHash: TG_API_HASH,
      settings: new Api.CodeSettings({}),
    }));
    tgPendingLogins.set(rec.sid, { client, phone: cleanPhone, phoneCodeHash: result.phoneCodeHash, createdAt: Date.now() });
  } catch (e) {
    await tgDispose(client);
    if (newToken) await purgeDevice(rec.sid);
    throw tgMapError(e);
  }
  json(ctx.res, 200, { ok: true, message: 'Code sent. Check your Telegram app.', ...(newToken && { deviceToken: newToken }) });
}

async function tgVerify(ctx) {
  requireTg();
  const { code } = parseJson(ctx.body);
  if (!code) throw new HttpError(400, 'code required');
  const { rec, pending } = requirePendingLogin(ctx.req);

  try {
    await pending.client.invoke(new Api.auth.SignIn({
      phoneNumber: pending.phone,
      phoneCodeHash: pending.phoneCodeHash,
      phoneCode: String(code).replace(/\s/g, ''),
    }));
    await tgFinishLogin(rec.sid, pending.client);
    json(ctx.res, 200, { ok: true, message: 'Logged in successfully.' });
  } catch (err) {
    const m = err?.errorMessage || err?.message || '';
    if (m.includes('SESSION_PASSWORD_NEEDED')) {
      return json(ctx.res, 200, { ok: true, passwordNeeded: true, message: '2FA password required.' });
    }
    if (/PHONE_CODE_EXPIRED/.test(m)) {
      tgPendingLogins.delete(rec.sid);
      await tgDispose(pending.client);
    }
    throw tgMapError(err);
  }
}

async function tgVerifyPassword(ctx) {
  requireTg();
  const { password } = parseJson(ctx.body);
  if (!password) throw new HttpError(400, 'password required');
  const { rec, pending } = requirePendingLogin(ctx.req);

  try {
    const pwd = await pending.client.invoke(new Api.account.GetPassword());
    const check = await computeCheck(pwd, password);
    await pending.client.invoke(new Api.auth.CheckPassword({ password: check }));
    await tgFinishLogin(rec.sid, pending.client);
    json(ctx.res, 200, { ok: true, message: 'Logged in successfully.' });
  } catch (err) {
    throw tgMapError(err);
  }
}

async function tgStatus(ctx) {
  requireTg();
  const rec = deviceFromReq(ctx.req, 'tg');
  if (!rec) return json(ctx.res, 200, { ok: true, connected: false, hasSavedSession: false, signInRequired: true, reason: ctx.req.authReason });

  let entry = tgClients.get(rec.sid);
  if (!entry && rec.verified) entry = await tgRestore(rec.sid).catch(() => null);

  json(ctx.res, 200, {
    ok: true,
    connected: !!entry?.connected,
    hasSavedSession: rec.verified,
    signInRequired: rec.verified && !entry,
  });
}

async function tgSend(ctx) {
  requireTg();
  const rec = requireVerifiedDevice(ctx.req, 'tg');
  const { to, resultId } = parseJson(ctx.body);
  if (!to || !resultId) throw new HttpError(400, 'to, resultId required');

  const entry = await tgActive(rec.sid);
  if (!entry) throw reauth();

  const { outPath, cleanup } = await toOggOpus(await fetchResultAudio(resultId), 'tg');
  try {
    const entity = await entry.client.getEntity(to);
    const stat = await fs.stat(outPath);
    const file = new CustomFile(path.basename(outPath), stat.size, outPath);
    const media = await entry.client.uploadFile({ file, workers: 1 });
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
    if (buf[start] === 0x2d && buf[start + 1] === 0x2d) break;
    if (buf[start] === 0x0d && buf[start + 1] === 0x0a) start += 2;
    const next = buf.indexOf(boundary, start);
    if (next === -1) break;
    const part = buf.subarray(start, Math.max(start, next - 2));
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
  const rec = requireVerifiedDevice(ctx.req, 'tg');
  const { fields, files } = parseMultipart(ctx.body, ctx.req.headers['content-type']);
  const video = files.video;
  if (!video?.data?.length || !fields.to) throw new HttpError(400, 'video, to required');

  const entry = await tgActive(rec.sid);
  if (!entry) throw reauth();

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

async function tgLogout(ctx) {
  requireTg();
  const rec = deviceFromReq(ctx.req, 'tg');
  if (rec) {
    const entry = tgClients.get(rec.sid);
    if (entry) { try { await entry.client.invoke(new Api.auth.LogOut()); } catch {} }
    await purgeDevice(rec.sid);
  }
  json(ctx.res, 200, { ok: true });
}

async function tgRestoreAll() {
  if (!TG_ENABLED) return;
  const sids = [...devices.values()].filter((d) => d.platform === 'tg' && d.verified).map((d) => d.sid);
  console.log(`[tg] restoring ${sids.length} device session(s)`);
  for (const sid of sids) {
    try { await tgRestore(sid); } catch { /* already logged */ }
  }
}

// =====================================================================
// maintenance
// =====================================================================
async function purgeLegacy() {
  const keep = new Set();
  for (const d of devices.values()) keep.add(`${d.platform}_${d.sid}`);
  let dropped = 0;

  if (pool) {
    const r = await pool.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name ~ '^(wa|tg)_[a-z0-9_]+$'`
    );
    for (const { schema_name } of r.rows) {
      if (!keep.has(schema_name)) {
        await pool.query(`DROP SCHEMA IF EXISTS "${schema_name}" CASCADE`);
        dropped++;
        console.log(`[purge-legacy] dropped schema ${schema_name}`);
      }
    }
    tgSchemasReady.clear();
  }
  const files = await fs.readdir(DATA_DIR).catch(() => []);
  for (const f of files) {
    const m = /^(wa_[a-z0-9_]+)\.db(-wal|-shm|-journal)?$/.exec(f);
    if (m && !keep.has(m[1])) {
      await fs.unlink(path.join(DATA_DIR, f)).catch(() => {});
      dropped++;
      console.log(`[purge-legacy] deleted ${f}`);
    }
  }
  console.log(`[purge-legacy] done, removed ${dropped} legacy item(s)`);
}

setInterval(() => {
  const now = Date.now();
  for (const [sid, p] of tgPendingLogins) {
    if (now - p.createdAt > LOGIN_TTL_MS) { tgPendingLogins.delete(sid); tgDispose(p.client); }
  }
  for (const rec of [...devices.values()]) {
    if (!rec.verified && now - rec.createdAt > PENDING_DEVICE_TTL_MS) {
      console.log(`[registry] purging unverified device ${short(rec.sid)}`);
      purgeDevice(rec.sid);
    }
  }
}, 60_000).unref();

// =====================================================================
// router
// =====================================================================
const routes = {
  'POST /pair':   { limit: MAX_JSON_BODY, fn: waPair },
  'GET /status':  { limit: 0,             fn: waStatus },
  'POST /send':   { limit: MAX_JSON_BODY, fn: waSend },
  'POST /logout': { limit: MAX_JSON_BODY, fn: waLogout },
  'POST /tg/start-login':     { limit: MAX_JSON_BODY,  fn: tgStartLogin },
  'POST /tg/verify':          { limit: MAX_JSON_BODY,  fn: tgVerify },
  'POST /tg/verify-password': { limit: MAX_JSON_BODY,  fn: tgVerifyPassword },
  'GET /tg/status':           { limit: 0,              fn: tgStatus },
  'POST /tg/send':            { limit: MAX_JSON_BODY,  fn: tgSend },
  'POST /tg/send-video':      { limit: MAX_VIDEO_BODY, fn: tgSendVideo },
  'POST /tg/logout':          { limit: MAX_JSON_BODY,  fn: tgLogout },
};

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Bridge-Secret, X-Device-Token, Authorization');

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

    res.setHeader('Cache-Control', 'no-store');
    const body = route.limit ? await readBody(req, route.limit) : Buffer.alloc(0);
    req.deviceToken = extractToken(req, body);
    await route.fn({ req, res, url, body });
  } catch (err) {
    const status = err instanceof HttpError ? err.status : 500;
    if (err?.extra?.reauth) console.warn(`[auth] ${req.method} ${String(req.url).split('?')[0]} -> ${err.extra.reason}`);
    if (status >= 500) console.error('request failed:', err);
    json(res, status, { ok: false, error: err?.message || String(err), ...(err instanceof HttpError ? err.extra : {}) });
  }
});

server.requestTimeout = 10 * 60 * 1000;

async function main() {
  await registryLoad();
  if (PURGE_LEGACY) await purgeLegacy().catch((e) => console.error('purgeLegacy failed:', e));
  server.listen(PORT, () => {
    console.log(`Bridge on :${PORT} (wa store=${WA_STORE_MODE}, telegram=${TG_ENABLED ? 'on' : 'off'})`);
    waRestoreAll().catch((e) => console.error('waRestoreAll error:', e));
    tgRestoreAll().catch((e) => console.error('tgRestoreAll error:', e));
  });
}
main().catch((e) => { console.error('fatal startup error:', e); process.exit(1); });

process.on('unhandledRejection', (e) => console.error('unhandledRejection:', e));

async function shutdown() {
  for (const { client } of waClients.values()) { try { await client.close?.(); } catch {} }
  for (const { client } of tgClients.values()) { await tgDispose(client); }
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
