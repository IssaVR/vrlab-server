/**
 * server.js  —  VR Lab Agent Server
 * ─────────────────────────────────────────────────────────────────────────────
 * Deployed on Railway (https://railway.app)
 *
 * WHAT THIS DOES:
 *   • Accepts secure WebSocket (WSS) connections from Unity agents on headsets
 *   • Authenticates each headset with a shared secret (set in Railway env vars)
 *   • Maintains live device state: battery, status, model, serial, FPS, RAM
 *   • Exposes a REST API so the dashboard can query device state
 *   • Relays commands from the dashboard to individual headsets in real time
 *   • Broadcasts telemetry updates to all connected dashboard browser tabs
 *
 * ENVIRONMENT VARIABLES (set these in Railway → Variables):
 *   PORT             — automatically injected by Railway (do NOT set manually)
 *   AGENT_SECRET     — shared secret key that Unity agents must send on connect
 *   DASHBOARD_ORIGIN — (optional) restrict CORS to your dashboard domain
 *
 * DEPLOYMENT:
 *   Push this folder to GitHub → connect repo in Railway → set AGENT_SECRET.
 *   Railway handles HTTPS, WSS, SSL certificates, restarts, and scaling.
 *
 * YOUR PUBLIC ENDPOINTS (replace with your Railway domain after deploy):
 *   WSS  (headset agents):  wss://your-app.up.railway.app
 *   WSS  (dashboard tabs):  wss://your-app.up.railway.app?type=dashboard
 *   REST (device list):     https://your-app.up.railway.app/api/devices
 * ─────────────────────────────────────────────────────────────────────────────
 */

'use strict';

const WebSocket = require('ws');
const http      = require('http');
const url       = require('url');
const fs        = require('fs');
const path      = require('path');
const os        = require('os');
const crypto    = require('crypto');

// ── Environment Variables ─────────────────────────────────────────────────────
// PORT is injected automatically by Railway — never hardcode it.
const PORT         = process.env.PORT;
const AGENT_SECRET = process.env.AGENT_SECRET;
const CORS_ORIGIN  = process.env.DASHBOARD_ORIGIN || '*';

// Fail fast if required env vars are missing
if (!PORT) {
  console.error('❌  PORT is not set. Railway injects this automatically — do not set it manually.');
  process.exit(1);
}
if (!AGENT_SECRET) {
  console.error('❌  AGENT_SECRET is not set.');
  console.error('    Go to Railway → your project → Variables → add AGENT_SECRET=<your-secret>');
  process.exit(1);
}

// ── Device Registry ───────────────────────────────────────────────────────────
// Map<deviceId, { ws, info, telemetry, lastSeen }>
const devices = new Map();

// Commands are queued even when also sent over WebSocket. The Unity agent
// deduplicates by id, so polling is a safe fallback for a broken receive path.
const pendingCommands = new Map();
const COMMAND_TTL_MS = 10 * 60 * 1000;
const MAX_PENDING_COMMANDS_PER_DEVICE = 50;

// Set of connected dashboard browser tabs
const dashboardClients = new Set();

// Session log: sessionKey (deviceId+ts) → session object
const sessions = new Map();

// Per-device current user name
const deviceUsers = new Map();

// ── HTTP Server ───────────────────────────────────────────────────────────────
// Railway terminates TLS at its edge — this server speaks plain HTTP/WS
// internally. Clients connect over HTTPS/WSS via Railway's reverse proxy.
const httpServer = http.createServer((req, res) => {
  const { pathname, query } = url.parse(req.url, true);

  // CORS headers for the REST API
  res.setHeader('Access-Control-Allow-Origin',  CORS_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Agent-Secret, X-Filename, X-Upload-Id, X-Chunk-Index, X-Total-Chunks');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Disposition');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // Unity fallback: atomically return and clear this device's queued commands.
  if (pathname === '/api/agent/commands' && req.method === 'GET') {
    if (req.headers['x-agent-secret'] !== AGENT_SECRET) {
      res.writeHead(401); res.end('Unauthorized'); return;
    }

    const deviceId = typeof query.deviceId === 'string' ? query.deviceId.trim() : '';
    if (!deviceId) { res.writeHead(400); res.end('Missing deviceId'); return; }

    const commands = takePendingCommands(deviceId);
    if (commands.length > 0)
      console.log(`[CMD Poll] ${deviceId} received ${commands.length}: ${commands.map(c => c.command).join(', ')}`);
    jsonResponse(res, { commands });
    return;
  }


  // ── POST /api/upload/chunk — chunked APK upload (avoids Railway timeout) ─
  // Headers: X-Upload-Id, X-Chunk-Index, X-Total-Chunks, X-Filename
  if (pathname === '/api/upload/chunk' && req.method === 'POST') {
    const uploadId    = (req.headers['x-upload-id']    || '').replace(/[^a-zA-Z0-9_-]/g,'_');
    const chunkIndex  = parseInt(req.headers['x-chunk-index']  || '0');
    const totalChunks = parseInt(req.headers['x-total-chunks'] || '1');
    const rawName     = req.headers['x-filename'] || 'upload.apk';
    const safeName    = path.basename(rawName).replace(/[^a-zA-Z0-9._-]/g, '_');

    if (!uploadId) { res.writeHead(400); res.end('Missing X-Upload-Id'); return; }

    const chunkPath = path.join(os.tmpdir(), `chunk_${uploadId}_${chunkIndex}`);
    const cws = fs.createWriteStream(chunkPath);
    req.pipe(cws);

    cws.on('finish', () => {
      // Count how many chunks we have so far
      let received = 0;
      for (let i = 0; i < totalChunks; i++) {
        if (fs.existsSync(path.join(os.tmpdir(), `chunk_${uploadId}_${i}`))) received++;
      }

      if (received < totalChunks) {
        jsonResponse(res, { ok: true, received, totalChunks, done: false });
        return;
      }

      // All chunks here — assemble into final file
      const finalName = `apk_${uploadId}_${safeName}`;
      const finalPath = path.join(os.tmpdir(), finalName);
      const out       = fs.createWriteStream(finalPath);

      let i = 0;
      function writeChunk() {
        if (i >= totalChunks) { out.end(); return; }
        const cp = path.join(os.tmpdir(), `chunk_${uploadId}_${i}`);
        const data = fs.readFileSync(cp);
        out.write(data);
        try { fs.unlinkSync(cp); } catch {}
        i++;
        writeChunk();
      }
      writeChunk();

      out.on('finish', () => {
        const size = fs.statSync(finalPath).size;
        console.log(`[Upload] Assembled ${finalName} (${Math.round(size/1024)}KB)`);
        setTimeout(() => fs.unlink(finalPath, () => {}), 2 * 60 * 60 * 1000);

        const host     = req.headers['x-forwarded-host'] || req.headers['host'] || 'localhost';
        const protocol = req.headers['x-forwarded-proto'] || 'https';
        const url      = `${protocol}://${host}/api/download/${finalName}`;
        jsonResponse(res, { ok: true, done: true, url, filename: safeName, size });
      });

      out.on('error', (err) => { res.writeHead(500); res.end(`Assembly failed: ${err.message}`); });
    });

    cws.on('error', (err) => { res.writeHead(500); res.end(`Chunk write failed: ${err.message}`); });
    return;
  }

  // ── GET /api/download/:filename — serve uploaded APK ─────────────────────
  const dlMatch = pathname.match(/^\/api\/download\/([a-zA-Z0-9._-]+)$/);
  if (dlMatch && req.method === 'GET') {
    const filename = dlMatch[1];
    const filepath = path.join(os.tmpdir(), filename);

    if (!fs.existsSync(filepath)) { res.writeHead(404); res.end('File not found or expired'); return; }

    const stat = fs.statSync(filepath);
    res.setHeader('Content-Type', 'application/vnd.android.package-archive');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Length', stat.size);
    res.setHeader('Access-Control-Allow-Origin', CORS_ORIGIN);
    res.writeHead(200);
    fs.createReadStream(filepath).pipe(res);
    return;
  }

  // ── GET /api/health ────────────────────────────────────────────────────────
  if (pathname === '/api/health' && req.method === 'GET') {
    jsonResponse(res, {
      status:     'ok',
      devices:    devices.size,
      dashboards: dashboardClients.size,
      uptime:     Math.floor(process.uptime()),
    });
    return;
  }

  // ── GET /api/sessions ─────────────────────────────────────────────────────
  if (pathname === '/api/sessions' && req.method === 'GET') {
    jsonResponse(res, [...sessions.values()]);
    return;
  }

  // ── GET /api/users ─────────────────────────────────────────────────────────
  // Aggregated stats per user name
  if (pathname === '/api/users' && req.method === 'GET') {
    const userMap = new Map();
    for (const s of sessions.values()) {
      const u = s.userName;
      if (!userMap.has(u)) {
        userMap.set(u, { userName: u, totalSeconds: 0, sessionCount: 0,
          lastSeen: 0, lastDevice: '', lastHeadset: '' });
      }
      const entry = userMap.get(u);
      entry.totalSeconds  += s.durationSeconds || 0;
      entry.sessionCount  += 1;
      if ((s.startTime || 0) > entry.lastSeen) {
        entry.lastSeen    = s.startTime;
        entry.lastDevice  = s.deviceName;
        entry.lastHeadset = s.headsetModel;
      }
    }
    jsonResponse(res, [...userMap.values()]
      .sort((a, b) => b.totalSeconds - a.totalSeconds));
    return;
  }

  // ── GET /api/devices ───────────────────────────────────────────────────────
  if (pathname === '/api/devices' && req.method === 'GET') {
    jsonResponse(res, [...devices.values()].map(sanitizeDevice));
    return;
  }

  // ── GET /api/device/:id ────────────────────────────────────────────────────
  const singleMatch = pathname.match(/^\/api\/device\/(.+)$/);
  if (singleMatch && req.method === 'GET') {
    const device = devices.get(singleMatch[1]);
    if (!device) { res.writeHead(404); res.end('Device not found'); return; }
    jsonResponse(res, sanitizeDevice(device));
    return;
  }

  // ── POST /api/command ──────────────────────────────────────────────────────
  // Body: { "deviceId": "...", "command": "restart", "payload": null }
  if (pathname === '/api/command' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => (body += chunk));
    req.on('end', () => {
      try {
        const { deviceId, command, payload } = JSON.parse(body);
        if (!deviceId || !command) { res.writeHead(400); res.end('Missing deviceId or command'); return; }

        const queued = enqueueCommand(deviceId, command, payload);
        const device = devices.get(deviceId);
        let sent = false;
        if (device && device.ws.readyState === WebSocket.OPEN) {
          try { device.ws.send(JSON.stringify(queued)); sent = true; }
          catch (err) { console.warn(`[CMD] WebSocket send failed for ${deviceId}: ${err.message}`); }
        }
        console.log(`[CMD] QUEUED${sent ? '+SENT' : ''} -> ${deviceId} : ${command} (${queued.id})`);
        jsonResponse(res, { ok: true, queued: true, sent, id: queued.id });
      } catch {
        res.writeHead(400); res.end('Invalid JSON body');
      }
    });
    return;
  }

  res.writeHead(404); res.end('Not found');
});

// ── Connection Janitor — purge dead sockets every 15s ─────────────────────────
setInterval(() => {
  for (const [id, dev] of devices) {
    // readyState: 0=CONNECTING 1=OPEN 2=CLOSING 3=CLOSED
    if (!dev.ws || dev.ws.readyState === 2 || dev.ws.readyState === 3) {
      console.log(`[Janitor] Purging dead connection: ${id} (state=${dev.ws?.readyState})`);
      devices.delete(id);
      broadcastToDashboard({ type: 'device_removed', deviceId: id });
    }
  }
}, 15_000);

// ── WebSocket Server ──────────────────────────────────────────────────────────
const wss = new WebSocket.Server({
  server: httpServer,
  maxPayload: 64 * 1024 * 1024,  // 64 MB — cast frames can be large; default 1MB was killing the connection
});

wss.on('connection', (ws, req) => {
  const { query } = url.parse(req.url, true);
  // ?type=dashboard → browser tab; anything else → Unity agent (headset)
  if (query.type === 'dashboard') {
    handleDashboardConnection(ws);
  } else {
    handleAgentConnection(ws);
  }
});

// ── Agent Connection Handler (Unity / Headset) ────────────────────────────────
function handleAgentConnection(ws) {
  let deviceId    = null;
  let authed      = false;
  let pingInterval;

  // Mark alive on any pong (response to our ws.ping())
  ws.on('pong', () => {
    if (deviceId && devices.has(deviceId)) devices.get(deviceId).lastSeen = Date.now();
  });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); }
    catch { return; }  // ignore malformed/partial frames, do NOT close the connection

    // ── Handshake — must be the very first message ─────────────────────────
    if (msg.type === 'handshake') {
      if (msg.secret !== AGENT_SECRET) {
        console.warn(`[AUTH] Rejected ${msg.deviceId} — wrong AGENT_SECRET`);
        ws.close(1008, 'Unauthorized');
        return;
      }

      deviceId = msg.deviceId;
      authed   = true;

      // If this device already has an old connection, close it cleanly first
      const existing = devices.get(deviceId);
      if (existing && existing.ws && existing.ws !== ws) {
        console.log(`[~] Replacing stale connection for ${deviceId}`);
        try { existing.ws.terminate(); } catch (e) {}
        if (existing.pingInterval) clearInterval(existing.pingInterval);
      }

      devices.set(deviceId, {
        ws,
        info: {
          deviceId:     msg.deviceId,
          deviceName:   msg.deviceName   || 'Unknown',
          serialNumber: msg.serialNumber || 'N/A',
          headsetModel: msg.headsetModel || 'Unknown',
          platform:     msg.platform     || 'Android',
          osVersion:    msg.osVersion    || '',
          appVersion:   msg.appVersion   || '',
          method:       'Wi-Fi',
          group:        null,
        },
        telemetry: {
          battery:    100,
          isCharging: false,
          status:     'online',
          fpsAvg:     0,
          memUsedMB:  0,
        },
        lastSeen: Date.now(),
        pingInterval: null,
      });

      console.log(`[+] Headset connected: "${msg.deviceName}" (${deviceId}) — ${msg.headsetModel}`);
      broadcastToDashboard({ type: 'device_connected', device: sanitizeDevice(devices.get(deviceId)) });

      // Keep-alive: WebSocket protocol-level ping every 20s
      // This prevents Railway's proxy from closing the connection as idle.
      pingInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          try { ws.ping(); } catch (e) {}  // protocol-level ping frame
        }
      }, 20_000);
      // Track interval on device so a replacement connection can clear it
      const devEntry = devices.get(deviceId);
      if (devEntry) devEntry.pingInterval = pingInterval;
      return;
    }

    if (!authed) { ws.close(1008, 'Not authenticated'); return; }

    const device = devices.get(deviceId);
    if (!device) return;
    device.lastSeen = Date.now();

    // ── User Session tracking ─────────────────────────────────────────────
    if (msg.type === 'session_start') {
      const session = {
        sessionId:   `${deviceId}-${msg.ts}`,
        deviceId,
        deviceName:  msg.deviceName  || devices.get(deviceId)?.info?.deviceName || 'Unknown',
        headsetModel:msg.headsetModel || devices.get(deviceId)?.info?.headsetModel || 'Unknown',
        userName:    msg.userName || 'Unknown',
        startTime:   Date.now(),
        endTime:     null,
        durationSeconds: 0,
      };
      sessions.set(session.sessionId, session);
      deviceUsers.set(deviceId, msg.userName);

      // Update device info with current user
      if (devices.has(deviceId)) devices.get(deviceId).info.currentUser = msg.userName;

      broadcastToDashboard({ type: 'session_start', session });
      console.log(`[Session] ${msg.userName} started on ${session.deviceName}`);
      return;
    }

    if (msg.type === 'session_end') {
      // Find open session for this device
      let found = null;
      for (const [k, s] of sessions) {
        if (s.deviceId === deviceId && !s.endTime) { found = s; break; }
      }
      if (found) {
        found.endTime        = Date.now();
        found.durationSeconds = msg.durationSeconds || Math.round((found.endTime - found.startTime) / 1000);
        broadcastToDashboard({ type: 'session_end', session: found });
        console.log(`[Session] ${found.userName} ended — ${found.durationSeconds}s`);
      }
      deviceUsers.delete(deviceId);
      return;
    }

    // ── Install progress/result — relay to dashboard ─────────────────────
    if (msg.type === 'install_progress' || msg.type === 'install_result') {
      const out = JSON.stringify(msg);
      for (const c of dashboardClients)
        if (c.readyState === WebSocket.OPEN) c.send(out);
      return;
    }

    // ── Cast Frame — relay to dashboard only ─────────────────────────────
    if (msg.type === 'frame') {
      const out = JSON.stringify({
        type: 'frame', deviceId,
        data: msg.data, w: msg.w, h: msg.h, ts: msg.ts
      });
      for (const c of dashboardClients)
        if (c.readyState === WebSocket.OPEN) c.send(out);
      return;
    }

    // ── Telemetry ──────────────────────────────────────────────────────────
    if (msg.type === 'telemetry') {
      device.telemetry = {
        battery:    msg.battery    ?? device.telemetry.battery,
        isCharging: msg.isCharging ?? false,
        status:     msg.status     || 'online',
        fpsAvg:     msg.fpsAvg     || 0,
        memUsedMB:  msg.memUsedMB  || 0,
      };
      broadcastToDashboard({ type: 'telemetry_update', deviceId, telemetry: device.telemetry });
      return;
    }

    // ── Status ─────────────────────────────────────────────────────────────
    if (msg.type === 'status') {
      device.telemetry.status = msg.status;
      broadcastToDashboard({ type: 'status_update', deviceId, status: msg.status });
      return;
    }

    // pong — lastSeen already updated above, nothing else needed
  });

  ws.on('close', () => {
    clearInterval(pingInterval);
    if (!deviceId || !devices.has(deviceId)) return;

    // Only act if THIS ws is still the registered one.
    // If a newer connection replaced it, leave the new one alone.
    if (devices.get(deviceId).ws !== ws) {
      console.log(`[-] Old connection closed for ${deviceId} (already replaced — ignoring)`);
      return;
    }

    devices.get(deviceId).telemetry.status = 'offline';
    broadcastToDashboard({ type: 'device_disconnected', deviceId });
    console.log(`[-] Headset disconnected: ${deviceId}`);

    setTimeout(() => {
      // Purge only if still offline AND still the same dead ws
      const d = devices.get(deviceId);
      if (d && d.ws === ws && d.telemetry?.status === 'offline') {
        devices.delete(deviceId);
        broadcastToDashboard({ type: 'device_removed', deviceId });
      }
    }, 5 * 60 * 1_000);
  });

  ws.on('error', (err) => console.error(`[Agent WS Error] ${err.message}`));
}

// ── Dashboard Connection Handler (Browser Tab) ────────────────────────────────
function handleDashboardConnection(ws) {
  dashboardClients.add(ws);
  console.log(`[Dashboard] Tab connected (total: ${dashboardClients.size})`);

  // Immediately push current state of all devices to the new tab
  ws.send(JSON.stringify({ type: 'snapshot', devices: [...devices.values()].map(sanitizeDevice) }));

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // ── Command relay: dashboard → headset ─────────────────────────────────
    if (msg.type === 'command_relay') {
      const target = devices.get(msg.deviceId);
      if (!msg.deviceId || !msg.command) {
        ws.send(JSON.stringify({ type: 'command_ack', ok: false, error: 'Missing deviceId or command' }));
        return;
      }

      const queued = enqueueCommand(msg.deviceId, msg.command, msg.payload);
      let sent = false;
      console.log(`[CMD Relay] command=${msg.command} deviceId=${msg.deviceId}`);
      console.log(`[CMD Relay] devices in registry: ${[...devices.keys()].join(', ') || 'NONE'}`);
      if (target && target.ws.readyState === WebSocket.OPEN) {
        try { target.ws.send(JSON.stringify(queued)); sent = true; }
        catch (err) { console.warn(`[CMD Relay] WebSocket send failed: ${err.message}`); }
      }

      console.log(`[CMD Relay] SUCCESS QUEUED${sent ? '+SENT' : ''} -> ${msg.deviceId} : ${msg.command} (${queued.id})`);
      ws.send(JSON.stringify({
        type: 'command_ack', ok: true, queued: true, sent,
        delivery: sent ? 'websocket_and_polling' : 'polling',
        id: queued.id, deviceId: msg.deviceId, command: msg.command,
      }));
    }
  });

  ws.on('close', () => {
    dashboardClients.delete(ws);
    console.log(`[Dashboard] Tab disconnected (total: ${dashboardClients.size})`);
  });

  ws.on('error', (err) => console.error(`[Dashboard WS Error] ${err.message}`));
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function enqueueCommand(deviceId, command, payload) {
  const queued = {
    id: crypto.randomUUID(),
    deviceId,
    command,
    payload: payload ?? null,
    createdAt: Date.now(),
  };

  const queue = pendingCommands.get(deviceId) || [];
  queue.push(queued);
  while (queue.length > MAX_PENDING_COMMANDS_PER_DEVICE) queue.shift();
  pendingCommands.set(deviceId, queue);
  return queued;
}

function takePendingCommands(deviceId) {
  const now = Date.now();
  const commands = (pendingCommands.get(deviceId) || [])
    .filter(command => now - command.createdAt <= COMMAND_TTL_MS);
  pendingCommands.delete(deviceId);
  return commands;
}

function broadcastToDashboard(payload) {
  const msg = JSON.stringify(payload);
  for (const client of dashboardClients) {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  }
}

function sanitizeDevice(d) {
  // Strip the ws object — safe to send over the network
  return {
    id:           d.info.deviceId,
    name:         d.info.deviceName,
    sn:           d.info.serialNumber,
    headset:      d.info.headsetModel,
    deviceId:     d.info.deviceId,
    deviceName:   d.info.deviceName,
    serialNumber: d.info.serialNumber,
    headsetModel: d.info.headsetModel,
    platform:     d.info.platform,
    osVersion:    d.info.osVersion,
    appVersion:   d.info.appVersion,
    method:       d.info.method,
    group:        d.info.group,
    ...d.telemetry,
    lastSeen:     d.lastSeen,
  };
}

function jsonResponse(res, data) {
  res.setHeader('Content-Type', 'application/json');
  res.writeHead(200);
  res.end(JSON.stringify(data));
}

// ── Start ─────────────────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`\n✅  VR Lab Server running`);
  console.log(`   PORT          : ${PORT}  (Railway-injected)`);
  console.log(`   AGENT_SECRET  : set ✓`);
  console.log(`   CORS origin   : ${CORS_ORIGIN}`);
  console.log(`\n   After Railway deploys, your live endpoints will be:`);
  console.log(`   wss://your-app.up.railway.app              ← Unity agents`);
  console.log(`   wss://your-app.up.railway.app?type=dashboard ← Dashboard`);
  console.log(`   https://your-app.up.railway.app/api/health ← Health check\n`);
});
