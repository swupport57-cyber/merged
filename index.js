import { createClient, WhatsmeowError } from '@whatsmeow-node/whatsmeow-node';
import { TelegramClient, Api } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { CustomFile } from 'telegram/client/uploads.js';
import { computeCheck } from 'telegram/Password.js';
import { exec } from 'child_process';
import { promisify } from 'util';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import http from 'http';
import pg from 'pg';

const execAsync = promisify(exec);
const { Pool } = pg;

// ---- config ----
const PORT = process.env.PORT || 3000;
const SUPABASE_DB_URL = process.env.SUPABASE_DB_URL;
const APP_BASE_URL = process.env.APP_BASE_URL;
const BRIDGE_SECRET = process.env.BRIDGE_SECRET;
const TG_API_ID = parseInt(process.env.TG_API_ID, 10);
const TG_API_HASH = process.env.TG_API_HASH;

if (!SUPABASE_DB_URL || !APP_BASE_URL || !BRIDGE_SECRET) {
  console.error('Missing required env: SUPABASE_DB_URL, APP_BASE_URL, BRIDGE_SECRET');
  process.exit(1);
}

const adminPool = new Pool({ connectionString: SUPABASE_DB_URL });

/* ═══════════════════════════════════════════════════════════════
   WHATSAPP STATE (strictly device-based)
   ═══════════════════════════════════════════════════════════════ */

// In-memory WhatsApp clients, keyed by "userId:deviceId".
const waClients = new Map();

// userId -> deviceId that is currently paired. When a request arrives with a
// different deviceId, the old pairing is invalidated before the new one is
// created.
const waPairedDevice = new Map();

// Track in-flight pairing attempts so concurrent /pair calls don't stomp
// on each other.
const waPairingInFlight = new Map(); // userId -> Promise

function waCacheKey(userId, deviceId) {
  return `${userId}:${deviceId}`;
}

function waSchemaFor(userId, deviceId) {
  const u = String(userId).toLowerCase().replace(/[^a-z0-9_]/g, '_').slice(0, 24) || 'default';
  const d = String(deviceId).toLowerCase().replace(/[^a-z0-9_]/g, '_').slice(0, 24) || 'default';
  return `wa_${u}_${d}`;
}

async function waStoreUrlForUser(userId, deviceId) {
  const schema = waSchemaFor(userId, deviceId);
  await adminPool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
  const sep = SUPABASE_DB_URL.includes('?') ? '&' : '?';
  return `${SUPABASE_DB_URL}${sep}options=-csearch_path%3D${schema}`;
}

async function waDestroyStore(userId, deviceId) {
  const schema = waSchemaFor(userId, deviceId);
  try {
    await adminPool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    console.log(`[WA ${userId}/${deviceId}] dropped schema ${schema}`);
  } catch (e) {
    console.error(`[WA ${userId}/${deviceId}] drop schema failed:`, e.message);
  }
}

async function waDestroyAllStoresForUser(userId) {
  const prefix = `wa_${String(userId).toLowerCase().replace(/[^a-z0-9_]/g, '_').slice(0, 24)}_`;
  try {
    const res = await adminPool.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE $1`,
      [prefix + '%']
    );
    for (const row of res.rows) {
      await adminPool.query(`DROP SCHEMA IF EXISTS ${row.schema_name} CASCADE`);
      console.log(`[WA ${userId}] dropped stale schema ${row.schema_name}`);
    }
  } catch (e) {
    console.error(`[WA ${userId}] destroy-all failed:`, e.message);
  }
}

async function waDropClient(userId, deviceId) {
  const key = waCacheKey(userId, deviceId);
  const entry = waClients.get(key);
  if (!entry) return;
  try {
    if (entry.client) {
      try { await entry.client.disconnect(); } catch (e) {}
      try { if (typeof entry.client.close === 'function') await entry.client.close(); } catch (e) {}
    }
  } catch (e) {
    console.error(`[WA ${key}] client close failed:`, e.message);
  }
  waClients.delete(key);
}

async function getOrCreateWaClient(userId, deviceId) {
  const key = waCacheKey(userId, deviceId);

  // Strictly device-based: if this user is paired on another device, drop
  // the old pairing before creating the new one.
  const knownDevice = waPairedDevice.get(userId);
  if (knownDevice && knownDevice !== deviceId) {
    console.log(`[WA ${userId}] device changed ${knownDevice} -> ${deviceId}, invalidating`);
    await waDropClient(userId, knownDevice);
    await waDestroyAllStoresForUser(userId);
    waPairedDevice.delete(userId);
  }

  if (waClients.has(key)) return waClients.get(key);

  const storeUrl = await waStoreUrlForUser(userId, deviceId);
  const client = createClient({
    store: storeUrl,
    commandTimeout: 60000, // bumped from 120000? keep 120s for initial sync
  });

  const entry = { client, jid: null, connected: false, deviceId, loggedOut: false };
  waClients.set(key, entry);

  client.on('connected', ({ jid }) => {
    entry.jid = jid;
    entry.connected = true;
    entry.loggedOut = false;
    waPairedDevice.set(userId, deviceId);
    console.log(`[WA ${userId}/${deviceId}] connected as ${jid}`);
  });

  client.on('disconnected', () => {
    entry.connected = false;
    console.log(`[WA ${userId}/${deviceId}] disconnected (auto-reconnect will handle)`);
  });

  // CRITICAL: session was revoked. Must clear everything so the next
  // pairing starts clean.
  client.on('logged_out', ({ reason }) => {
    console.warn(`[WA ${userId}/${deviceId}] logged_out reason=${reason} — invalidating session`);
    entry.connected = false;
    entry.loggedOut = true;
    waPairedDevice.delete(userId);
    waClients.delete(key);
    waDestroyStore(userId, deviceId).catch(() => {});
  });

  // CRITICAL: protocol-level error. 401/device_removed means the same as
  // logged_out. Log everything for debugging.
  client.on('stream_error', ({ code }) => {
    console.warn(`[WA ${userId}/${deviceId}] stream_error code=${code}`);
    if (String(code) === '401' || String(code).includes('replaced') || String(code).includes('device_removed')) {
      entry.connected = false;
      entry.loggedOut = true;
      waPairedDevice.delete(userId);
      waClients.delete(key);
      waDestroyStore(userId, deviceId).catch(() => {});
    }
  });

  client.on('error', (err) => {
    const m = err?.message || String(err);
    console.error(`[WA ${userId}/${deviceId}] error:`, m);
    if (err instanceof WhatsmeowError && err.code === 'ERR_TIMEOUT') {
      console.error(`[WA ${userId}/${deviceId}] command timed out — increase commandTimeout or retry`);
    }
  });

  await client.init();
  return entry;
}

function waEntryForDevice(userId, deviceId) {
  const knownDevice = waPairedDevice.get(userId);
  if (knownDevice && knownDevice !== deviceId) return null;
  return waClients.get(waCacheKey(userId, deviceId)) || null;
}

/* ═══════════════════════════════════════════════════════════════
   TELEGRAM STATE (GramJS) — unchanged
   ═══════════════════════════════════════════════════════════════ */

const tgClients = new Map();
const tgPendingLogins = new Map();

function tgSchemaFor(userId) {
  const safe = String(userId).toLowerCase().replace(/[^a-z0-9_]/g, '_').slice(0, 40) || 'default';
  return `tg_${safe}`;
}

async function tgEnsureSchema(userId) {
  const schema = tgSchemaFor(userId);
  await adminPool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
  await adminPool.query(`
    CREATE TABLE IF NOT EXISTS ${schema}.sessions (
      user_id TEXT PRIMARY KEY,
      session_string TEXT NOT NULL,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  return schema;
}

async function tgGetUserSession(userId) {
  const schema = await tgEnsureSchema(userId);
  const res = await adminPool.query(
    `SELECT session_string FROM ${schema}.sessions WHERE user_id = $1`,
    [userId]
  );
  return res.rows[0]?.session_string || '';
}

async function tgSaveUserSession(userId, sessionString) {
  const schema = await tgEnsureSchema(userId);
  await adminPool.query(
    `INSERT INTO ${schema}.sessions (user_id, session_string, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (user_id) DO UPDATE SET session_string = $2, updated_at = NOW()`,
    [userId, sessionString]
  );
}

async function getOrCreateTgClient(userId) {
  if (tgClients.has(userId)) return tgClients.get(userId);

  const sessionString = await tgGetUserSession(userId);
  const session = new StringSession(sessionString);

  const client = new TelegramClient(session, TG_API_ID, TG_API_HASH, {
    connectionRetries: 5,
    useWSS: false,
  });

  const entry = { client, connected: false };
  tgClients.set(userId, entry);

  if (sessionString) {
    try {
      await client.connect();
      const me = await client.getMe();
      if (me) {
        entry.connected = true;
        console.log(`[TG ${userId}] reconnected from saved session`);
      } else {
        throw new Error('Session present but not authorized');
      }
    } catch (e) {
      console.error(`[TG ${userId}] reconnect failed:`, e.message);
      tgClients.delete(userId);
      throw new Error('Saved session is invalid — please log in again.');
    }
  }

  return entry;
}

/* ═══════════════════════════════════════════════════════════════
   HELPERS
   ═══════════════════════════════════════════════════════════════ */

function splitMultipart(buf, boundary) {
  const delim = Buffer.from('\r\n' + boundary);
  const parts = [];
  let start = 0;
  while (true) {
    const idx = buf.indexOf(delim, start);
    if (idx === -1) {
      parts.push(buf.slice(start));
      break;
    }
    if (idx > start) parts.push(buf.slice(start, idx));
    start = idx + delim.length;
  }
  return parts;
}

/* ═══════════════════════════════════════════════════════════════
   SERVER
   ═══════════════════════════════════════════════════════════════ */

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Bridge-Secret');

  if (req.method === 'OPTIONS') {
    res.writeHead(204).end();
    return;
  }

  const chunks = [];
  for await (const c of req) chunks.push(c);
  const rawBodyBuffer = Buffer.concat(chunks);
  const raw = rawBodyBuffer.toString('utf8');

  res.setHeader('Content-Type', 'application/json');

  const providedSecret = req.headers['x-bridge-secret'];
  if (providedSecret !== BRIDGE_SECRET) {
    res.writeHead(401).end(JSON.stringify({ ok: false, error: 'Unauthorized' }));
    return;
  }

  const url = new URL(req.url, `http://localhost:${PORT}`);

  try {
    /* ═══════════════ WHATSAPP ROUTES ═══════════════ */

    // ---- POST /pair ----
    if (req.method === 'POST' && url.pathname === '/pair') {
      const { userId, deviceId, phone } = JSON.parse(raw || '{}');
      if (!userId || !deviceId || !phone) {
        res.writeHead(400).end(JSON.stringify({ ok: false, error: 'userId, deviceId, phone required' }));
        return;
      }

      // STRICT DEVICE GUARD: refuse if this user is paired on another device.
      const knownDevice = waPairedDevice.get(userId);
      if (knownDevice && knownDevice !== deviceId) {
        console.log(`[WA ${userId}] blocked: already paired on ${knownDevice}, requested from ${deviceId}`);
        res.writeHead(409).end(JSON.stringify({
          ok: false,
          error: 'This number is already linked to another device. Unlink it there first.',
          code: 'ALREADY_PAIRED_ELSEWHERE',
        }));
        return;
      }

      // Prevent concurrent pairing attempts for the same user.
      if (waPairingInFlight.has(userId)) {
        res.writeHead(429).end(JSON.stringify({
          ok: false,
          error: 'Pairing already in progress for this account. Wait a moment.',
          code: 'PAIRING_IN_FLIGHT',
        }));
        return;
      }

      const cleanPhone = String(phone).replace(/[^0-9]/g, '');
      if (cleanPhone.length < 10 || cleanPhone.length > 15) {
        res.writeHead(400).end(JSON.stringify({
          ok: false,
          error: 'Invalid phone number. Use 10–15 digits with country code.',
          code: 'BAD_PHONE',
        }));
        return;
      }

      const pairingPromise = (async () => {
        const entry = await getOrCreateWaClient(userId, deviceId);

        if (entry.connected) {
          return { ok: true, alreadyPaired: true, jid: entry.jid };
        }

        // Step 1: connect. This is async; returns before handshake completes.
        try {
          await entry.client.connect();
        } catch (e) {
          const m = String(e?.message || e);
          if (!/already connected/i.test(m)) throw e;
        }

        // Step 2: WAIT for the connection to actually be ready.
        // pairCode() requires an active connection. Without this wait, we
        // race the WebSocket handshake and WhatsApp kicks us with 401.
        try {
          await entry.client.waitForConnection(15000); // 15s max wait
        } catch (e) {
          console.warn(`[WA ${userId}/${deviceId}] waitForConnection failed:`, e.message);
          // If it timed out but might still connect, proceed cautiously;
          // pairCode will fail cleanly if not.
        }

        // Step 3: request the code.
        let code;
        try {
          code = await entry.client.pairCode(cleanPhone);
        } catch (e) {
          const m = String(e?.message || e);
          if (m.includes('conflict') || m.includes('already') || m.includes('401')) {
            throw Object.assign(new Error('This number is already linked elsewhere. Unlink it first.'), {
              code: 'PHONE_ALREADY_LINKED',
            });
          }
          throw e;
        }

        return { ok: true, code };
      })();

      waPairingInFlight.set(userId, pairingPromise);

      try {
        const result = await pairingPromise;
        if (result.alreadyPaired) {
          res.end(JSON.stringify(result));
        } else {
          res.end(JSON.stringify(result));
        }
      } catch (e) {
        const code = e.code || null;
        const status = code === 'ALREADY_PAIRED_ELSEWHERE' || code === 'PHONE_ALREADY_LINKED' ? 409 : 500;
        res.writeHead(status).end(JSON.stringify({
          ok: false,
          error: e.message || String(e),
          code,
        }));
      } finally {
        waPairingInFlight.delete(userId);
      }
      return;
    }

    // ---- GET /status ----
    if (req.method === 'GET' && url.pathname === '/status') {
      const userId = url.searchParams.get('userId');
      const deviceId = url.searchParams.get('deviceId');
      if (!userId || !deviceId) {
        res.writeHead(400).end(JSON.stringify({ ok: false, error: 'userId and deviceId required' }));
        return;
      }
      const entry = waEntryForDevice(userId, deviceId);
      res.end(JSON.stringify({
        ok: true,
        connected: !!entry?.connected,
        jid: entry?.jid || null,
        loggedOut: !!entry?.loggedOut,
      }));
      return;
    }

    // ---- POST /unpair ----
    if (req.method === 'POST' && url.pathname === '/unpair') {
      const { userId, deviceId } = JSON.parse(raw || '{}');
      if (!userId || !deviceId) {
        res.writeHead(400).end(JSON.stringify({ ok: false, error: 'userId and deviceId required' }));
        return;
      }
      await waDropClient(userId, deviceId);
      await waDestroyStore(userId, deviceId);
      if (waPairedDevice.get(userId) === deviceId) {
        waPairedDevice.delete(userId);
      }
      res.end(JSON.stringify({ ok: true, message: 'Unpaired.' }));
      return;
    }

    // ---- POST /send ---- (WhatsApp voice note)
    if (req.method === 'POST' && url.pathname === '/send') {
      const { userId, deviceId, phone, resultId } = JSON.parse(raw || '{}');
      if (!userId || !deviceId || !phone || !resultId) {
        res.writeHead(400).end(JSON.stringify({
          ok: false,
          error: 'userId, deviceId, phone, resultId required',
        }));
        return;
      }

      const entry = waEntryForDevice(userId, deviceId);
      if (!entry?.connected) {
        res.writeHead(409).end(JSON.stringify({
          ok: false,
          error: 'Not paired on this device',
        }));
        return;
      }

      const to = String(phone).replace(/[^0-9]/g, '') + '@s.whatsapp.net';

      const audioRes = await fetch(`${APP_BASE_URL}/api/result/${encodeURIComponent(resultId)}`);
      if (!audioRes.ok) {
        res.writeHead(502).end(JSON.stringify({
          ok: false,
          error: `Audio fetch failed: ${audioRes.status}`,
        }));
        return;
      }
      const inputBuf = Buffer.from(await audioRes.arrayBuffer());

      const tmpDir = os.tmpdir();
      const stamp = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const inPath  = path.join(tmpDir, `wa-in-${stamp}.wav`);
      const outPath = path.join(tmpDir, `wa-out-${stamp}.ogg`);

      await fs.writeFile(inPath, inputBuf);

      try {
        await execAsync(
          `ffmpeg -y -i "${inPath}" ` +
          `-vn -c:a libopus -b:a 32k -ar 16000 -ac 1 ` +
          `-application voip -avoid_negative_ts make_zero -map_metadata -1 ` +
          `"${outPath}"`,
          { timeout: 30000 }
        );

        const oggBuf = await fs.readFile(outPath);
        const media = await entry.client.uploadMedia(outPath, 'audio');
        await entry.client.sendRawMessage(to, {
          audioMessage: {
            URL: media.URL,
            directPath: media.directPath,
            mediaKey: media.mediaKey,
            fileEncSHA256: media.fileEncSHA256,
            fileSHA256: media.fileSHA256,
            fileLength: String(media.fileLength),
            mimetype: 'audio/ogg; codecs=opus',
            PTT: true,
          }
        });

        res.end(JSON.stringify({ ok: true, to, bytes: oggBuf.length }));
      } finally {
        await fs.unlink(inPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});
      }
      return;
    }

    /* ═══════════════ TELEGRAM ROUTES (unchanged) ═══════════════ */

    // ---- POST /tg/start-login ----
    if (req.method === 'POST' && url.pathname === '/tg/start-login') {
      const { userId, phone } = JSON.parse(raw || '{}');
      if (!userId || !phone) {
        res.writeHead(400).end(JSON.stringify({
          ok: false,
          error: 'userId and phone required',
        }));
        return;
      }

      const cleanPhone = String(phone).replace(/[^0-9+]/g, '');

      const old = tgPendingLogins.get(userId);
      if (old?.client) {
        try { await old.client.disconnect(); } catch (e) {}
      }

      const session = new StringSession('');
      const client = new TelegramClient(session, TG_API_ID, TG_API_HASH, {
        connectionRetries: 5,
        useWSS: false,
      });

      await client.connect();

      const result = await client.invoke(new Api.auth.SendCode({
        phoneNumber: cleanPhone,
        apiId: TG_API_ID,
        apiHash: TG_API_HASH,
        settings: new Api.CodeSettings({}),
      }));

      tgPendingLogins.set(userId, {
        client,
        phone: cleanPhone,
        phoneCodeHash: result.phoneCodeHash,
      });

      res.end(JSON.stringify({
        ok: true,
        message: 'Code sent. Check your Telegram app.',
      }));
      return;
    }

    // ---- POST /tg/verify ----
    if (req.method === 'POST' && url.pathname === '/tg/verify') {
      const { userId, code } = JSON.parse(raw || '{}');
      if (!userId || !code) {
        res.writeHead(400).end(JSON.stringify({
          ok: false,
          error: 'userId and code required',
        }));
        return;
      }

      const pending = tgPendingLogins.get(userId);
      if (!pending) {
        res.writeHead(400).end(JSON.stringify({
          ok: false,
          error: 'No pending login. Start over.',
        }));
        return;
      }

      try {
        await pending.client.invoke(new Api.auth.SignIn({
          phoneNumber: pending.phone,
          phoneCodeHash: pending.phoneCodeHash,
          phoneCode: String(code),
        }));

        const sessionString = pending.client.session.save();
        await tgSaveUserSession(userId, sessionString);

        tgClients.set(userId, { client: pending.client, connected: true });
        tgPendingLogins.delete(userId);

        res.end(JSON.stringify({ ok: true, message: 'Logged in successfully.' }));
      } catch (err) {
        const m = err?.errorMessage || err?.message || String(err);
        if (m.includes('SESSION_PASSWORD_NEEDED')) {
          res.end(JSON.stringify({
            ok: true,
            passwordNeeded: true,
            message: '2FA password required.',
          }));
        } else if (m.includes('PHONE_CODE_INVALID')) {
          res.writeHead(400).end(JSON.stringify({
            ok: false,
            error: 'Invalid code. Try again.',
          }));
        } else if (m.includes('PHONE_CODE_EXPIRED')) {
          res.writeHead(400).end(JSON.stringify({
            ok: false,
            error: 'Code expired. Start over.',
          }));
        } else {
          res.writeHead(500).end(JSON.stringify({ ok: false, error: m }));
        }
      }
      return;
    }

    // ---- POST /tg/verify-password ----
    if (req.method === 'POST' && url.pathname === '/tg/verify-password') {
      const { userId, password } = JSON.parse(raw || '{}');
      if (!userId || !password) {
        res.writeHead(400).end(JSON.stringify({
          ok: false,
          error: 'userId and password required',
        }));
        return;
      }

      const pending = tgPendingLogins.get(userId);
      if (!pending) {
        res.writeHead(400).end(JSON.stringify({
          ok: false,
          error: 'No pending login. Start over.',
        }));
        return;
      }

      try {
        const pwd = await pending.client.invoke(new Api.account.GetPassword());
        const check = await computeCheck(pwd, password);
        await pending.client.invoke(new Api.auth.CheckPassword({ password: check }));

        const sessionString = pending.client.session.save();
        await tgSaveUserSession(userId, sessionString);
        tgClients.set(userId, { client: pending.client, connected: true });
        tgPendingLogins.delete(userId);

        res.end(JSON.stringify({ ok: true, message: 'Logged in successfully.' }));
      } catch (err) {
        const m = err?.errorMessage || err?.message || String(err);
        if (m.includes('PASSWORD_HASH_INVALID')) {
          res.writeHead(400).end(JSON.stringify({
            ok: false,
            error: 'Wrong password.',
          }));
        } else {
          res.writeHead(400).end(JSON.stringify({ ok: false, error: m }));
        }
      }
      return;
    }

    // ---- GET /tg/status ----
    if (req.method === 'GET' && url.pathname === '/tg/status') {
      const userId = url.searchParams.get('userId');
      if (!userId) {
        res.writeHead(400).end(JSON.stringify({
          ok: false,
          error: 'userId required',
        }));
        return;
      }
      const entry = tgClients.get(userId);
      let hasSaved = false;
      try { hasSaved = !!(await tgGetUserSession(userId)); } catch (e) {}
      res.end(JSON.stringify({
        ok: true,
        connected: !!entry?.connected,
        hasSavedSession: hasSaved,
      }));
      return;
    }

    // ---- POST /tg/send ---- (Telegram voice note from resultId)
    if (req.method === 'POST' && url.pathname === '/tg/send') {
      const { userId, to, resultId, mode } = JSON.parse(raw || '{}');
      if (!userId || !to || !resultId) {
        res.writeHead(400).end(JSON.stringify({
          ok: false,
          error: 'userId, to, resultId required',
        }));
        return;
      }

      const entry = tgClients.get(userId);
      if (!entry?.connected) {
        res.writeHead(409).end(JSON.stringify({
          ok: false,
          error: 'Not logged in. Please log in first.',
        }));
        return;
      }

      const audioRes = await fetch(`${APP_BASE_URL}/api/result/${encodeURIComponent(resultId)}`);
      if (!audioRes.ok) {
        res.writeHead(502).end(JSON.stringify({
          ok: false,
          error: `Audio fetch failed: ${audioRes.status}`,
        }));
        return;
      }
      const inputBuf = Buffer.from(await audioRes.arrayBuffer());

      const tmpDir = os.tmpdir();
      const stamp = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const inPath  = path.join(tmpDir, `tg-in-${stamp}.wav`);
      const outPath = path.join(tmpDir, `tg-out-${stamp}.ogg`);

      await fs.writeFile(inPath, inputBuf);

      try {
        await execAsync(
          `ffmpeg -y -i "${inPath}" ` +
          `-vn -c:a libopus -b:a 32k -ar 16000 -ac 1 ` +
          `-application voip -avoid_negative_ts make_zero -map_metadata -1 ` +
          `"${outPath}"`,
          { timeout: 30000 }
        );

        const entity = await entry.client.getEntity(to);

        const stat = await fs.stat(outPath);
        const file = new CustomFile(path.basename(outPath), stat.size, outPath);
        const media = await entry.client.uploadFile({ file, workers: 1 });

        await entry.client.sendFile(entity, {
          file: media,
          voiceNote: mode !== 'video',
          videoNote: mode === 'video',
        });

        res.end(JSON.stringify({ ok: true, to, mode: mode || 'voice' }));
      } finally {
        await fs.unlink(inPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});
      }
      return;
    }

    // ---- POST /tg/send-video ---- (multipart)
    if (req.method === 'POST' && url.pathname === '/tg/send-video') {
      const contentType = req.headers['content-type'] || '';
      const boundaryMatch = contentType.match(/boundary=(.+)$/);
      if (!boundaryMatch) {
        res.writeHead(400).end(JSON.stringify({
          ok: false,
          error: 'multipart/form-data required',
        }));
        return;
      }

      const boundary = '--' + boundaryMatch[1];
      const parts = splitMultipart(rawBodyBuffer, boundary);

      let videoBuf = null, videoName = 'video.mp4';
      let formUserId = null, formTo = null;

      for (const part of parts) {
        const headerEnd = part.indexOf('\r\n\r\n');
        if (headerEnd === -1) continue;
        const headers = part.slice(0, headerEnd).toString();
        const body = part.slice(headerEnd + 4);
        const nameMatch = headers.match(/name="([^"]+)"/);
        const fileMatch = headers.match(/filename="([^"]+)"/);
        if (!nameMatch) continue;
        const fieldName = nameMatch[1];
        if (fieldName === 'video' && fileMatch) {
          videoBuf = body;
          videoName = fileMatch[1];
        } else if (fieldName === 'userId') {
          formUserId = body.toString().trim();
        } else if (fieldName === 'to') {
          formTo = body.toString().trim();
        }
      }

      if (!videoBuf || !formUserId || !formTo) {
        res.writeHead(400).end(JSON.stringify({
          ok: false,
          error: 'video, userId, to required',
        }));
        return;
      }

      const entry = tgClients.get(formUserId);
      if (!entry?.connected) {
        res.writeHead(409).end(JSON.stringify({
          ok: false,
          error: 'Not logged in.',
        }));
        return;
      }

      const tmpDir = os.tmpdir();
      const stamp = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const ext = path.extname(videoName) || '.mp4';
      const inPath  = path.join(tmpDir, `tgvid-in-${stamp}${ext}`);
      const outPath = path.join(tmpDir, `tgvid-out-${stamp}.mp4`);

      await fs.writeFile(inPath, videoBuf);

      try {
        await execAsync(
          `ffmpeg -y -i "${inPath}" ` +
          `-t 60 ` +
          `-vf "scale=480:480:force_original_aspect_ratio=increase,crop=480:480" ` +
          `-c:v libx264 -preset ultrafast -crf 28 -pix_fmt yuv420p ` +
          `-c:a aac -b:a 64k ` +
          `-movflags +faststart ` +
          `"${outPath}"`,
          { timeout: 180000 }
        );

        const entity = await entry.client.getEntity(formTo);

        const stat = await fs.stat(outPath);
        const file = new CustomFile(path.basename(outPath), stat.size, outPath);
        const media = await entry.client.uploadFile({ file, workers: 1 });

        await entry.client.sendFile(entity, { file: media, videoNote: true });

        res.end(JSON.stringify({ ok: true, to: formTo, bytes: stat.size }));
      } finally {
        await fs.unlink(inPath).catch(() => {});
        await fs.unlink(outPath).catch(() => {});
      }
      return;
    }

    res.writeHead(404).end(JSON.stringify({ ok: false, error: 'not found' }));
  } catch (err) {
    console.error('request failed:', err);
    res.writeHead(500).end(JSON.stringify({
      ok: false,
      error: err?.message || String(err),
    }));
  }
});

server.listen(PORT, () => console.log(`Combined bridge on :${PORT}`));
