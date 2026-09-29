import { createClient } from '@whatsmeow-node/whatsmeow-node';
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

// ================= config =================
const PORT = process.env.PORT || 3000;
const APP_BASE_URL = process.env.APP_BASE_URL;
const BRIDGE_SECRET = process.env.BRIDGE_SECRET;

// STORE_MODE=sqlite  (default, most reliable; needs a persistent disk at DATA_DIR)
// STORE_MODE=postgres (needs SUPABASE_DB_URL = DIRECT or SESSION-pooler string, port 5432, NOT 6543)
const STORE_MODE = (process.env.STORE_MODE || 'sqlite').toLowerCase();
const DATA_DIR = process.env.DATA_DIR || '/data';
const SUPABASE_DB_URL = process.env.SUPABASE_DB_URL;

const INIT_TIMEOUT = 25000;
const CONNECT_TIMEOUT = 20000;
const PAIR_TIMEOUT = 20000;

if (!APP_BASE_URL || !BRIDGE_SECRET) {
  console.error('Missing required env vars: APP_BASE_URL, BRIDGE_SECRET');
  process.exit(1);
}
if (STORE_MODE === 'postgres' && !SUPABASE_DB_URL) {
  console.error('STORE_MODE=postgres requires SUPABASE_DB_URL');
  process.exit(1);
}

const adminPool = STORE_MODE === 'postgres'
  ? new Pool({ connectionString: SUPABASE_DB_URL, connectionTimeoutMillis: 10000 })
  : null;

// ================= helpers =================
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

// ================= store =================
async function storeUrlForUser(userId) {
  const safe = safeId(userId);

  if (STORE_MODE === 'sqlite') {
    await fs.mkdir(DATA_DIR, { recursive: true });
    return `file:${path.join(DATA_DIR, `wa_${safe}.db`)}?_foreign_keys=on`;
  }

  const schema = `wa_${safe}`; // whitelisted chars only
  await withTimeout(adminPool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`), 10000, 'create schema');
  const u = new URL(SUPABASE_DB_URL);
  u.searchParams.set('sslmode', 'require');
  u.searchParams.set('connect_timeout', '10');
  u.searchParams.set('options', `-csearch_path=${schema}`);
  return u.toString();
}

// Which users already have a stored session (used to restore after restart)
async function listStoredUsers() {
  try {
    if (STORE_MODE === 'sqlite') {
      const files = await fs.readdir(DATA_DIR).catch(() => []);
      return files
        .filter((f) => /^wa_.+\.db$/.test(f))
        .map((f) => f.slice(3, -3));
    }
    const r = await adminPool.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'wa\\_%'`
    );
    return r.rows.map((x) => x.schema_name.slice(3));
  } catch (e) {
    console.error('listStoredUsers failed:', e.message);
    return [];
  }
}

// ================= client registry =================
const clients = new Map(); // userId -> entry (only fully initialised clients)
const pending = new Map(); // userId -> Promise (dedupes concurrent init)

async function getOrCreateClient(userId) {
  if (clients.has(userId)) return clients.get(userId);
  if (pending.has(userId)) return pending.get(userId);

  const p = (async () => {
    console.log(`[${userId}] preparing store (${STORE_MODE})...`);
    const storeUrl = await storeUrlForUser(userId);

    const client = createClient({ store: storeUrl, commandTimeout: 30000 });
    const entry = { client, jid: null, connected: false, pairedAt: null };

    client.on('connected', (ev) => {
      entry.jid = ev?.jid || entry.jid;
      entry.connected = true;
      console.log(`[${userId}] CONNECTED jid=${entry.jid}`);
    });
    client.on('disconnected', () => {
      entry.connected = false;
      console.log(`[${userId}] disconnected`);
    });
    client.on('pair_success', (ev) => {
      entry.jid = ev?.jid || entry.jid;
      entry.pairedAt = Date.now();
      console.log(`[${userId}] PAIR SUCCESS jid=${entry.jid}`);
    });
    client.on('logged_out', () => {
      entry.connected = false;
      entry.jid = null;
      console.log(`[${userId}] logged out from phone`);
      clients.delete(userId);
    });
    client.on('error', (err) => console.error(`[${userId}] error:`, err?.message || err));

    try {
      console.log(`[${userId}] init...`);
      const info = await withTimeout(client.init(), INIT_TIMEOUT, 'client.init');
      if (info?.jid) entry.jid = info.jid; // already-paired store returns its jid
      console.log(`[${userId}] init ok jid=${entry.jid || 'none (unpaired)'}`);
    } catch (e) {
      try { await client.close?.(); } catch {}
      throw e; // never cache a broken client
    }

    clients.set(userId, entry);
    return entry;
  })().finally(() => pending.delete(userId));

  pending.set(userId, p);
  return p;
}

async function ensureConnected(entry, userId) {
  if (entry.connected) return;
  try {
    await withTimeout(entry.client.connect(), CONNECT_TIMEOUT, 'connect');
  } catch (e) {
    if (!/already connected/i.test(String(e?.message))) throw e;
  }
  // give the 'connected' event a moment to land
  for (let i = 0; i < 20 && !entry.connected && entry.jid; i++) {
    await new Promise((r) => setTimeout(r, 250));
  }
  console.log(`[${userId}] ensureConnected done connected=${entry.connected}`);
}

// Restore every previously-paired user on boot so /status and /send survive restarts
async function restoreSessions() {
  const users = await listStoredUsers();
  console.log(`restoring ${users.length} stored session(s)`);
  for (const userId of users) {
    try {
      const entry = await getOrCreateClient(userId);
      if (entry.jid) await ensureConnected(entry, userId);
    } catch (e) {
      console.error(`[${userId}] restore failed:`, e.message);
    }
  }
}

// ================= audio =================
async function toOggOpus(wavBuf) {
  const stamp = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const inPath = path.join(os.tmpdir(), `wa-${stamp}.in`);
  const outPath = path.join(os.tmpdir(), `wa-${stamp}.ogg`);
  await fs.writeFile(inPath, wavBuf);
  try {
    await execFileAsync('ffmpeg', [
      '-y', '-i', inPath, '-vn',
      '-c:a', 'libopus', '-b:a', '32k', '-ar', '16000', '-ac', '1',
      '-application', 'voip', '-avoid_negative_ts', 'make_zero', '-map_metadata', '-1',
      outPath,
    ], { timeout: 30000 });
    return { outPath, cleanup: async () => { await fs.unlink(inPath).catch(() => {}); await fs.unlink(outPath).catch(() => {}); } };
  } catch (e) {
    await fs.unlink(inPath).catch(() => {});
    await fs.unlink(outPath).catch(() => {});
    throw e;
  }
}

// ================= server =================
const json = (res, code, body) => {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
};

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Bridge-Secret');

  if (req.method === 'OPTIONS') { res.writeHead(204).end(); return; }

  const url = new URL(req.url, `http://localhost:${PORT}`);

  if (req.method === 'GET' && url.pathname === '/health') {
    return json(res, 200, { ok: true, clients: clients.size });
  }

  if (!safeEqual(req.headers['x-bridge-secret'], BRIDGE_SECRET)) {
    return json(res, 401, { ok: false, error: 'Unauthorized' });
  }

  let raw = '';
  try {
    const chunks = [];
    let size = 0;
    for await (const c of req) {
      size += c.length;
      if (size > 1_000_000) return json(res, 413, { ok: false, error: 'Body too large' });
      chunks.push(c);
    }
    raw = Buffer.concat(chunks).toString('utf8');
  } catch (e) {
    return json(res, 400, { ok: false, error: 'Bad body' });
  }

  try {
    // ---------- POST /pair ----------
    if (req.method === 'POST' && url.pathname === '/pair') {
      const { userId, phone } = JSON.parse(raw || '{}');
      if (!userId || !phone) return json(res, 400, { ok: false, error: 'userId and phone required' });

      const cleanPhone = String(phone).replace(/[^0-9]/g, '');
      if (cleanPhone.length < 8) return json(res, 400, { ok: false, error: 'Phone must include country code, digits only' });

      const t0 = Date.now();
      const log = (m) => console.log(`[pair ${userId}] +${Date.now() - t0}ms ${m}`);

      log('getOrCreateClient...');
      const entry = await getOrCreateClient(userId);
      log('store opened');

      // Already logged in on a previous run
      if (entry.jid) {
        await ensureConnected(entry, userId);
        if (entry.connected) {
          log('already paired');
          return json(res, 200, { ok: true, alreadyPaired: true, jid: entry.jid });
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
      return json(res, 200, { ok: true, code });
    }

    // ---------- GET /status ----------
    if (req.method === 'GET' && url.pathname === '/status') {
      const userId = url.searchParams.get('userId');
      if (!userId) return json(res, 400, { ok: false, error: 'userId required' });

      let entry = clients.get(userId);
      if (!entry) {
        // After a restart: only revive users that actually have a stored session
        const stored = await listStoredUsers();
        if (stored.includes(safeId(userId))) {
          entry = await getOrCreateClient(userId).catch(() => null);
          if (entry?.jid && !entry.connected) await ensureConnected(entry, userId).catch(() => {});
        }
      } else if (entry.jid && !entry.connected) {
        await ensureConnected(entry, userId).catch(() => {});
      }

      return json(res, 200, {
        ok: true,
        connected: !!entry?.connected,
        paired: !!entry?.jid,
        jid: entry?.jid || null,
      });
    }

    // ---------- POST /send ----------
    if (req.method === 'POST' && url.pathname === '/send') {
      const { userId, phone, resultId } = JSON.parse(raw || '{}');
      if (!userId || !phone || !resultId) {
        return json(res, 400, { ok: false, error: 'userId, phone, resultId required' });
      }

      let entry = clients.get(userId) || (await getOrCreateClient(userId));
      if (!entry.jid) return json(res, 409, { ok: false, error: 'User not paired' });
      await ensureConnected(entry, userId);
      if (!entry.connected) return json(res, 409, { ok: false, error: 'WhatsApp not connected yet, retry shortly' });

      const to = String(phone).replace(/[^0-9]/g, '') + '@s.whatsapp.net';

      const audioRes = await fetch(`${APP_BASE_URL}/api/result/${encodeURIComponent(resultId)}`);
      if (!audioRes.ok) return json(res, 502, { ok: false, error: `Audio fetch failed: ${audioRes.status}` });
      const inputBuf = Buffer.from(await audioRes.arrayBuffer());

      const { outPath, cleanup } = await toOggOpus(inputBuf);
      try {
        const oggBuf = await fs.readFile(outPath);
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
        // Protobuf JSON names: URL / PTT (uppercase). Fallback to lowercase if this build differs.
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
            console.warn(`[${userId}] proto field mismatch, trying next variant: ${e.message}`);
          }
        }
        if (lastErr) throw lastErr;
        return json(res, 200, { ok: true, to, bytes: oggBuf.length });
      } finally {
        await cleanup();
      }
    }

    // ---------- POST /logout ----------
    if (req.method === 'POST' && url.pathname === '/logout') {
      const { userId } = JSON.parse(raw || '{}');
      const entry = clients.get(userId);
      if (entry) {
        try { await entry.client.logout?.(); } catch {}
        try { await entry.client.close?.(); } catch {}
        clients.delete(userId);
      }
      return json(res, 200, { ok: true });
    }

    return json(res, 404, { ok: false, error: 'not found' });
  } catch (err) {
    console.error('request failed:', err);
    return json(res, 500, { ok: false, error: err?.message || String(err) });
  }
});

server.listen(PORT, () => {
  console.log(`WA bridge on :${PORT} (store=${STORE_MODE})`);
  restoreSessions().catch((e) => console.error('restoreSessions error:', e));
});

process.on('unhandledRejection', (e) => console.error('unhandledRejection:', e));
process.on('SIGTERM', async () => {
  for (const { client } of clients.values()) { try { await client.close?.(); } catch {} }
  process.exit(0);
});
