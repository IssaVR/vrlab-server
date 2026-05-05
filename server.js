/**
 * server.js  —  VR Lab Agent Cloud Server
 * ─────────────────────────────────────────────────────────────────────────────
 * WHAT THIS DOES:
 *   • Accepts WebSocket connections from Unity agents running on headsets
 *   • Authenticates each headset with a shared secret key
 *   • Maintains live device state (battery, status, model, etc.)
 *   • Exposes a REST API so the dashboard can query device state
 *   • Sends commands FROM the dashboard TO individual headsets
 *   • Broadcasts updates to all connected dashboard browser tabs
 *
 * DEPLOY ON:
 *   Any Linux cloud VM: DigitalOcean, Linode, AWS EC2, Render, Railway, etc.
 *
 * INSTALL & RUN:
 *   npm install
 *   AGENT_SECRET=your_secret_here node server.js
 *
 * ENVIRONMENT VARIABLES:
 *   PORT          — port to listen on (default: 3000)
 *   AGENT_SECRET  — shared secret key (must match Unity script)
 *   DASHBOARD_ORIGIN — allowed CORS origin for dashboard (default: *)
 * ─────────────────────────────────────────────────────────────────────────────
 */

const WebSocket = require('ws');
const http      = require('http');
const url       = require('url');

// ── Config ────────────────────────────────────────────────────────────────────
const PORT          = process.env.PORT          || 3000;
const AGENT_SECRET  = process.env.AGENT_SECRET  || 'CHANGE_THIS_SECRET';
const CORS_ORIGIN   = process.env.DASHBOARD_ORIGIN || '*';

if (AGENT_SECRET === 'CHANGE_THIS_SECRET') {
  console.warn('⚠️  WARNING: Using default AGENT_SECRET. Set the AGENT_SECRET environment variable!');
}

// ── Device Registry ───────────────────────────────────────────────────────────
// deviceId → { ws, info, telemetry, lastSeen }
const devices = new Map();

// Connected dashboard browser tabs (WebSocket connections from the dashboard)
const dashboardClients = new Set();

// ── HTTP Server (serves REST API + WebSocket upgrade) ─────────────────────────
const httpServer = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const path      = parsedUrl.pathname;

  // CORS headers
  res.setHeader('Access-Control-Allow-Origin',  CORS_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // ── GET /api/devices — full device list ───────────────────────────────────
  if (path === '/api/devices' && req.method === 'GET') {
    const list = [...devices.values()].map(d => sanitizeDevice(d));
    json(res, list);
    return;
  }

  // ── GET /api/device/:id — single device ───────────────────────────────────
  const match = path.match(/^\/api\/device\/(.+)$/);
  if (match && req.method === 'GET') {
    const d = devices.get(match[1]);
    if (!d) { res.writeHead(404); res.end('Not found'); return; }
    json(res, sanitizeDevice(d));
    return;
  }

  // ── POST /api/command — send command to a device ──────────────────────────
  if (path === '/api/command' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const { deviceId, command, payload } = JSON.parse(body);
        if (!deviceId || !command) { res.writeHead(400); res.end('Bad request'); return; }

        const d = devices.get(deviceId);
        if (!d || d.ws.readyState !== WebSocket.OPEN) {
          res.writeHead(503); res.end('Device not connected'); return;
        }

        d.ws.send(JSON.stringify({ command, payload: payload || null }));
        console.log(`[CMD] → ${deviceId} : ${command}`);
        json(res, { ok: true });
      } catch (e) {
        res.writeHead(400); res.end('Invalid JSON');
      }
    });
    return;
  }

  // ── GET /api/health — server health check ─────────────────────────────────
  if (path === '/api/health') {
    json(res, { status: 'ok', devices: devices.size, dashboards: dashboardClients.size });
    return;
  }

  res.writeHead(404); res.end('Not found');
});

// ── WebSocket Server ──────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server: httpServer });

wss.on('connection', (ws, req) => {
  const parsedUrl = url.parse(req.url, true);
  const clientType = parsedUrl.query.type; // "agent" | "dashboard"

  if (clientType === 'dashboard') {
    handleDashboardConnection(ws);
  } else {
    handleAgentConnection(ws);
  }
});

// ── Handle Unity Agent Connection (Headset) ───────────────────────────────────
function handleAgentConnection(ws) {
  let deviceId    = null;
  let authed      = false;
  let pingInterval;

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); }
    catch { ws.close(1003, 'Invalid JSON'); return; }

    // ── Handshake (first message must be this) ───────────────────────────
    if (msg.type === 'handshake') {
      if (msg.secret !== AGENT_SECRET) {
        console.warn(`[AUTH] Rejected device ${msg.deviceId} — wrong secret`);
        ws.close(1008, 'Unauthorized');
        return;
      }

      deviceId = msg.deviceId;
      authed   = true;

      const device = {
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
          battery:   100,
          isCharging: false,
          status:    'online',
          fpsAvg:    0,
          memUsedMB: 0,
        },
        lastSeen: Date.now(),
      };

      devices.set(deviceId, device);
      console.log(`[+] Device connected: ${msg.deviceName} (${deviceId}) — ${msg.headsetModel}`);

      // Notify all dashboard tabs
      broadcastToDashboard({ type: 'device_connected', device: sanitizeDevice(device) });

      // Start ping to keep connection alive
      pingInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ command: 'ping' }));
        }
      }, 30000);
      return;
    }

    // ── All other messages require auth ──────────────────────────────────
    if (!authed) { ws.close(1008, 'Not authenticated'); return; }

    const d = devices.get(deviceId);
    if (!d) return;
    d.lastSeen = Date.now();

    // ── Telemetry Update ──────────────────────────────────────────────────
    if (msg.type === 'telemetry') {
      d.telemetry = {
        battery:    msg.battery    ?? d.telemetry.battery,
        isCharging: msg.isCharging ?? false,
        status:     msg.status     || 'online',
        fpsAvg:     msg.fpsAvg    || 0,
        memUsedMB:  msg.memUsedMB || 0,
      };

      broadcastToDashboard({ type: 'telemetry_update', deviceId, telemetry: d.telemetry });
      return;
    }

    // ── Status Update ─────────────────────────────────────────────────────
    if (msg.type === 'status') {
      if (d.telemetry) d.telemetry.status = msg.status;
      broadcastToDashboard({ type: 'status_update', deviceId, status: msg.status });
      return;
    }

    // ── Pong ──────────────────────────────────────────────────────────────
    if (msg.type === 'pong') {
      // Just updates lastSeen (already done above)
      return;
    }
  });

  ws.on('close', () => {
    clearInterval(pingInterval);
    if (deviceId && devices.has(deviceId)) {
      const d = devices.get(deviceId);
      if (d.telemetry) d.telemetry.status = 'offline';
      broadcastToDashboard({ type: 'device_disconnected', deviceId });
      // Keep device record for 5 mins (shows as offline), then remove
      setTimeout(() => {
        if (devices.has(deviceId)) {
          const current = devices.get(deviceId);
          if (current.telemetry?.status === 'offline') {
            devices.delete(deviceId);
            broadcastToDashboard({ type: 'device_removed', deviceId });
          }
        }
      }, 300000);
      console.log(`[-] Device disconnected: ${deviceId}`);
    }
  });

  ws.on('error', (err) => console.error(`[WS Error] ${err.message}`));
}

// ── Handle Dashboard Browser Connection ──────────────────────────────────────
function handleDashboardConnection(ws) {
  dashboardClients.add(ws);
  console.log(`[Dashboard] Connected (total: ${dashboardClients.size})`);

  // Send current device list on connect
  const snapshot = [...devices.values()].map(d => sanitizeDevice(d));
  ws.send(JSON.stringify({ type: 'snapshot', devices: snapshot }));

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }

    // Dashboard can relay commands to devices via WebSocket
    if (msg.type === 'command_relay') {
      const target = devices.get(msg.deviceId);
      if (target && target.ws.readyState === WebSocket.OPEN) {
        target.ws.send(JSON.stringify({ command: msg.command, payload: msg.payload || null }));
        console.log(`[CMD Relay] dashboard → ${msg.deviceId} : ${msg.command}`);
        ws.send(JSON.stringify({ type: 'command_ack', ok: true, deviceId: msg.deviceId, command: msg.command }));
      } else {
        ws.send(JSON.stringify({ type: 'command_ack', ok: false, error: 'Device not connected' }));
      }
    }
  });

  ws.on('close', () => {
    dashboardClients.delete(ws);
    console.log(`[Dashboard] Disconnected (total: ${dashboardClients.size})`);
  });

  ws.on('error', (err) => console.error(`[Dashboard WS Error] ${err.message}`));
}

// ── Broadcast to all dashboard tabs ──────────────────────────────────────────
function broadcastToDashboard(payload) {
  const msg = JSON.stringify(payload);
  for (const client of dashboardClients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  }
}

// ── Strip WebSocket object before sending to clients ─────────────────────────
function sanitizeDevice(d) {
  return {
    ...d.info,
    ...d.telemetry,
    lastSeen: d.lastSeen,
    id: d.info.deviceId,   // alias for dashboard compatibility
    sn: d.info.serialNumber,
    name: d.info.deviceName,
    headset: d.info.headsetModel,
  };
}

// ── JSON helper ───────────────────────────────────────────────────────────────
function json(res, data) {
  res.setHeader('Content-Type', 'application/json');
  res.writeHead(200);
  res.end(JSON.stringify(data));
}

// ── Start ─────────────────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`\n🚀  VR Lab Server running on port ${PORT}`);
  console.log(`   WebSocket (agents):    ws://YOUR_SERVER:${PORT}            (type not set)`);
  console.log(`   WebSocket (dashboard): ws://YOUR_SERVER:${PORT}?type=dashboard`);
  console.log(`   REST API:              http://YOUR_SERVER:${PORT}/api/devices\n`);
});
