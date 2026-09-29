import { createClient } from '@whatsmeow-node/whatsmeow-node';
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

if (!SUPABASE_DB_URL || !APP_BASE_URL || !BRIDGE_SECRET) {
  console.error('Missing required environment variables: SUPABASE_DB_URL, APP_BASE_URL, BRIDGE_SECRET');
  process.exit(1);
}

// A single pool for creating per-user schemas.
const adminPool = new Pool({ connectionString: SUPABASE_DB_URL });

// One whatsmeow client per user, keyed by userId.
const clients = new Map();

async function storeUrlForUser(userId) {
  // Sanitize userId into a valid Postgres schema name.
  const safe = String(userId).toLowerCase().replace(/[^a-z0-9_]/g, '_').slice(0, 40) || 'default';
  const schema = `wa_${safe}`;
  // Whitelisted characters, so no SQL injection path here.
  await adminPool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
  const sep = SUPABASE_DB_URL.includes('?') ? '&' : '?';
  return `${SUPABASE_DB_URL}${sep}options=-csearch_path%3D${schema}`;
}

async function getOrCreateClient(userId) {
  if (clients.has(userId)) return clients.get(userId);

  const storeUrl = await storeUrlForUser(userId);
  const client = createClient({ 
  store: storeUrl,
  commandTimeout: 120000  // 120 seconds (2 minutes)
});

  const entry = { client, jid: null, connected: false };
  clients.set(userId, entry);

  client.on('connected', ({ jid }) => {
    entry.jid = jid;
    entry.connected = true;
    console.log(`[${userId}] connected as ${jid}`);
  });

  client.on('disconnected', () => {
    entry.connected = false;
    console.log(`[${userId}] disconnected`);
  });

  client.on('error', (err) => {
    console.error(`[${userId}] error:`, err?.message || err);
  });

  await client.init();
  return entry;
}

// ---- the server ----

const server = http.createServer(async (req, res) => {
  // CORS — without these, browsers discard responses and report
  // "Failed to fetch" even when the server answered correctly.
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Bridge-Secret');

  // Browsers send an OPTIONS preflight before any POST that carries a custom
  // header like X-Bridge-Secret. Answer it here and return early. No body to
  // read, no secret to check on a preflight.
  if (req.method === 'OPTIONS') {
    res.writeHead(204).end();
    return;
  }

  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString('utf8');

  res.setHeader('Content-Type', 'application/json');

  const url = new URL(req.url, `http://localhost:${PORT}`);

  // Shared-secret gate. Every real request must carry it.
  const providedSecret = req.headers['x-bridge-secret'];
  if (providedSecret !== BRIDGE_SECRET) {
    res.writeHead(401).end(JSON.stringify({ ok: false, error: 'Unauthorized' }));
    return;
  }

  try {
    // ---- POST /pair ----
    if (req.method === 'POST' && url.pathname === '/pair') {
      const { userId, phone } = JSON.parse(raw || '{}');
      if (!userId || !phone) {
        res.writeHead(400).end(JSON.stringify({ ok: false, error: 'userId and phone required' }));
        return;
      }

      const cleanPhone = String(phone).replace(/[^0-9]/g, '');
      const entry = await getOrCreateClient(userId);

      if (entry.connected) {
        res.end(JSON.stringify({ ok: true, alreadyPaired: true, jid: entry.jid }));
        return;
      }

      // The WebSocket may already be up from a previous attempt. If so,
      // connect() throws "already connected", which is harmless here.
      try {
        await entry.client.connect();
      } catch (e) {
        const m = String(e?.message || e);
        if (!/already connected/i.test(m)) throw e;
      }

      const code = await entry.client.pairCode(cleanPhone);
      res.end(JSON.stringify({ ok: true, code }));
      return;
    }

    // ---- GET /status ----
    if (req.method === 'GET' && url.pathname === '/status') {
      const userId = url.searchParams.get('userId');
      const entry = clients.get(userId);
      res.end(JSON.stringify({
        ok: true,
        connected: !!entry?.connected,
        jid: entry?.jid || null
      }));
      return;
    }

    // ---- POST /send ----
    if (req.method === 'POST' && url.pathname === '/send') {
      const { userId, phone, resultId } = JSON.parse(raw || '{}');
      if (!userId || !phone || !resultId) {
        res.writeHead(400).end(JSON.stringify({ ok: false, error: 'userId, phone, resultId required' }));
        return;
      }

      const entry = clients.get(userId);
      if (!entry?.connected) {
        res.writeHead(409).end(JSON.stringify({ ok: false, error: 'User not paired' }));
        return;
      }

      const to = String(phone).replace(/[^0-9]/g, '') + '@s.whatsapp.net';

      // 1. Fetch the converted audio from app.py
      const audioRes = await fetch(`${APP_BASE_URL}/api/result/${encodeURIComponent(resultId)}`);
      if (!audioRes.ok) {
        res.writeHead(502).end(JSON.stringify({ ok: false, error: `Audio fetch failed: ${audioRes.status}` }));
        return;
      }
      const inputBuf = Buffer.from(await audioRes.arrayBuffer());

      // 2. Temp files — live on disk only for the length of this request.
      const tmpDir = os.tmpdir();
      const stamp = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const inPath  = path.join(tmpDir, `wa-${stamp}.wav`);
      const outPath = path.join(tmpDir, `wa-${stamp}.ogg`);

      await fs.writeFile(inPath, inputBuf);

      try {
        // 3. ffmpeg → OGG / Opus / 16kHz / mono. Required for iOS playback.
        await execAsync(
          `ffmpeg -y -i "${inPath}" ` +
          `-vn -c:a libopus -b:a 32k -ar 16000 -ac 1 ` +
          `-application voip -avoid_negative_ts make_zero -map_metadata -1 ` +
          `"${outPath}"`,
          { timeout: 30000 }
        );

        const oggBuf = await fs.readFile(outPath);

        // 4. Upload then send with ptt: true so it renders as a voice note.
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

    res.writeHead(404).end(JSON.stringify({ ok: false, error: 'not found' }));
  } catch (err) {
    console.error('request failed:', err);
    res.writeHead(500).end(JSON.stringify({ ok: false, error: err?.message || String(err) }));
  }
});

server.listen(PORT, () => console.log(`WA bridge on :${PORT}`));
