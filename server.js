'use strict';
/* Omoogle — static host + real worldwide matchmaking/signaling server. */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const ROOT = path.join(__dirname, 'public');
const REPORTS = path.join(__dirname, 'reports.log');

const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml', '.png': 'image/png', '.jpg': 'image/jpeg',
  '.webp': 'image/webp', '.ico': 'image/x-icon', '.woff2': 'font/woff2',
  '.txt': 'text/plain; charset=utf-8', '.webmanifest': 'application/manifest+json'
};

let chatsToday = 0;
let dayStamp = new Date().toDateString();
function bumpChats() {
  const d = new Date().toDateString();
  if (d !== dayStamp) { dayStamp = d; chatsToday = 0; }
  chatsToday++;
}

function json(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*'
  });
  res.end(body);
}

function iceConfig() {
  const ice = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
  if (process.env.TURN_URL) {
    ice.push({
      urls: process.env.TURN_URL.split(',').map(s => s.trim()),
      username: process.env.TURN_USER || '',
      credential: process.env.TURN_PASS || ''
    });
  }
  return ice;
}

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  const p = u.pathname;

  if (p === '/api/health') return json(res, 200, { ok: true, uptime: Math.round(process.uptime()) });
  if (p === '/api/ice') return json(res, 200, { iceServers: iceConfig(), turn: !!process.env.TURN_URL });
  if (p === '/api/stats') return json(res, 200, stats());

  if (p === '/api/report' && req.method === 'POST') {
    let body = '';
    req.on('data', c => { body += c; if (body.length > 8192) req.destroy(); });
    req.on('end', () => {
      let r = {};
      try { r = JSON.parse(body || '{}'); } catch (e) { return json(res, 400, { ok: false }); }
      const line = JSON.stringify({
        ts: new Date().toISOString(),
        room: String(r.room || '').slice(0, 40),
        who: String(r.who || '').slice(0, 60),
        reason: String(r.reason || '').slice(0, 120),
        note: String(r.note || '').slice(0, 600)
      }) + '\n';
      fs.appendFile(REPORTS, line, () => {});
      json(res, 200, { ok: true });
    });
    return;
  }
  if (p.startsWith('/api/')) return json(res, 404, { ok: false });

  let rel;
  try { rel = decodeURIComponent(p); } catch (e) { rel = '/'; }
  if (rel === '/') rel = '/index.html';
  const file = path.normalize(path.join(ROOT, rel));
  if (!file.startsWith(ROOT)) { res.writeHead(403); return res.end('Forbidden'); }

  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) {
      const idx = path.join(ROOT, 'index.html');
      return fs.readFile(idx, (e2, buf) => {
        if (e2) { res.writeHead(404); return res.end('Not found'); }
        res.writeHead(200, { 'Content-Type': MIME['.html'], 'Cache-Control': 'no-cache' });
        res.end(buf);
      });
    }
    const ext = path.extname(file).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600',
      'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'no-referrer'
    });
    fs.createReadStream(file).pipe(res);
  });
});

/* ---------------- matchmaking ---------------- */
const clients = new Map();   // id -> ws
const queue = new Set();     // ws waiting
const rooms = new Map();     // roomId -> {a,b}

function stats() {
  const places = new Set();
  clients.forEach(c => { if (c.info && c.info.region) places.add(c.info.region); });
  return { online: clients.size, waiting: queue.size, chats: chatsToday, places: places.size, live: true };
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) { try { ws.send(JSON.stringify(obj)); } catch (e) {} }
}

function compatible(a, b) {
  if (a === b) return false;
  if (!a.info || !b.info) return false;
  if (a.info.mode !== b.info.mode) return false;
  const ra = a.info.regionKey || 'world', rb = b.info.regionKey || 'world';
  return ra === 'world' || rb === 'world' || ra === rb;
}

function interestScore(a, b) {
  const A = (a.info.interests || []), B = (b.info.interests || []);
  let s = 0;
  for (const x of A) for (const y of B) if (String(x).toLowerCase() === String(y).toLowerCase()) s++;
  return s;
}

function tryMatch(ws) {
  if (!queue.has(ws)) return;
  let best = null, bestScore = -1;
  for (const other of queue) {
    if (!compatible(ws, other)) continue;
    const s = interestScore(ws, other);
    if (s > bestScore) { bestScore = s; best = other; }
  }
  if (!best) return;
  queue.delete(ws); queue.delete(best);
  const id = crypto.randomBytes(6).toString('hex');
  rooms.set(id, { a: ws, b: best });
  ws.room = id; best.room = id;
  bumpChats();
  send(ws, { t: 'paired', room: id, role: 'offerer', peer: best.info });
  send(best, { t: 'paired', room: id, role: 'answerer', peer: ws.info });
  broadcastStats();
}

function leaveQueue(ws) { queue.delete(ws); }

function endRoom(ws, notify) {
  const id = ws.room;
  if (!id) return;
  const r = rooms.get(id);
  ws.room = null;
  if (!r) return;
  const other = r.a === ws ? r.b : r.a;
  rooms.delete(id);
  if (other) {
    other.room = null;
    if (notify) send(other, { t: 'peer-left' });
  }
  broadcastStats();
}

function broadcastStats() {
  const s = stats();
  clients.forEach(c => send(c, { t: 'stats', ...s }));
}

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 262144 });

wss.on('connection', (ws, req) => {
  if (clients.size >= 5000) { try { ws.close(1013, 'full'); } catch (e) {} return; }
  ws.id = crypto.randomBytes(6).toString('hex');
  ws.alive = true;
  ws.room = null;
  ws.info = null;
  ws.rate = 0;
  clients.set(ws.id, ws);
  send(ws, { t: 'welcome', id: ws.id, ...stats() });
  broadcastStats();

  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', raw => {
    if (++ws.rate > 120) { try { ws.close(1008, 'rate'); } catch (e) {} return; }
    let m;
    try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'hello') {
      ws.info = {
        alias: String(m.alias || 'Anonymous').slice(0, 24),
        flag: String(m.flag || '🌍').slice(0, 8),
        region: String(m.region || 'Earth').slice(0, 24),
        regionKey: String(m.regionKey || 'world').slice(0, 16),
        mode: m.mode === 'text' ? 'text' : 'video',
        interests: Array.isArray(m.interests) ? m.interests.slice(0, 5).map(x => String(x).slice(0, 18)) : [],
        tz: String(m.tz || '').slice(0, 48),
        lang: String(m.lang || 'en').slice(0, 16)
      };
      broadcastStats();
      return;
    }
    if (m.t === 'find') {
      if (!ws.info) return;
      endRoom(ws, true);
      queue.add(ws);
      send(ws, { t: 'queued', waiting: queue.size });
      tryMatch(ws);
      return;
    }
    if (m.t === 'cancel') { leaveQueue(ws); broadcastStats(); return; }
    if (m.t === 'signal') {
      const r = rooms.get(ws.room);
      if (!r) return;
      const other = r.a === ws ? r.b : r.a;
      send(other, { t: 'signal', data: m.data });
      return;
    }
    if (m.t === 'bye') { leaveQueue(ws); endRoom(ws, true); return; }
    if (m.t === 'ping') { send(ws, { t: 'pong' }); return; }
  });

  ws.on('close', () => {
    leaveQueue(ws);
    endRoom(ws, true);
    clients.delete(ws.id);
    broadcastStats();
  });
  ws.on('error', () => {});
});

setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.alive) { try { ws.terminate(); } catch (e) {} return; }
    ws.alive = false;
    try { ws.ping(); } catch (e) {}
    ws.rate = 0;
  });
  broadcastStats();
}, 25000);

setInterval(() => { for (const ws of queue) tryMatch(ws); }, 1500);

server.listen(PORT, () => {
  console.log('Omoogle live on http://localhost:' + PORT);
  console.log('TURN relay: ' + (process.env.TURN_URL ? 'configured' : 'not set (STUN only)'));
});