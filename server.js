// Servidor del overlay de seguidores para TikTok Live
// Escucha tu live con TikTok-Live-Connector y avisa al overlay cuando alguien te sigue.

const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');
const lib = require('tiktok-live-connector');

// Compatible con la versión 1.x (WebcastPushConnection) y 2.x (TikTokLiveConnection)
const Connection = lib.TikTokLiveConnection || lib.WebcastPushConnection;

const TIKTOK_USER = (process.env.TIKTOK_USER || 'jaadennnnn').replace('@', '');
const PORT = process.env.PORT || 3000;
const RETRY_MS = 20000; // cada cuánto reintenta conectarse si no estás en directo

// ---------- Servidor web ----------
const app = express();
app.get('/', (req, res) => res.redirect('/overlay'));
app.get('/overlay', (req, res) => res.sendFile(path.join(__dirname, 'overlay.html')));
app.get('/health', (req, res) => res.json({ ok: true, user: TIKTOK_USER, status }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

let status = 'offline';
const seen = new Set(); // evita contar dos veces al mismo usuario durante la sesión

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const client of wss.clients) {
    if (client.readyState === 1) client.send(msg);
  }
}

function setStatus(s) {
  if (status === s) return;
  status = s;
  console.log('Estado:', s);
  broadcast({ type: 'status', status: s });
}

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'status', status }));
  ws.on('message', (raw) => {
    // El overlay envía "ping" cada poco: mantiene despierto el servicio gratuito
    if (String(raw) === 'ping') ws.send('pong');
  });
});

// ---------- Conexión con TikTok ----------
let conn = null;
let retryTimer = null;

function scheduleRetry() {
  if (retryTimer) return;
  retryTimer = setTimeout(() => {
    retryTimer = null;
    start();
  }, RETRY_MS);
}

async function start() {
  try {
    if (conn) {
      try { conn.disconnect(); } catch (_) {}
    }
    conn = new Connection(TIKTOK_USER);

    conn.on('follow', (data) => {
      const id =
        data.userId ||
        (data.user && data.user.userId) ||
        data.uniqueId ||
        (data.user && data.user.uniqueId);
      if (id && seen.has(id)) return;
      if (id) seen.add(id);
      const name = data.uniqueId || (data.user && data.user.uniqueId) || data.nickname || '';
      console.log('Nuevo seguidor:', name);
      broadcast({ type: 'follow', user: name });
    });

    conn.on('streamEnd', () => {
      setStatus('offline');
      scheduleRetry();
    });

    conn.on('disconnected', () => {
      setStatus('offline');
      scheduleRetry();
    });

    conn.on('error', (err) => {
      console.log('Error de conexión:', err && err.message ? err.message : err);
    });

    await conn.connect();
    setStatus('live');
  } catch (err) {
    console.log('No se pudo conectar (¿no estás en directo todavía?):', err && err.message ? err.message : err);
    setStatus('offline');
    scheduleRetry();
  }
}

process.on('uncaughtException', (e) => console.log('Excepción:', e && e.message ? e.message : e));
process.on('unhandledRejection', (e) => console.log('Promesa rechazada:', e && e.message ? e.message : e));

server.listen(PORT, () => {
  console.log('Servidor en el puerto', PORT, '- usuario:', TIKTOK_USER);
  start();
});
