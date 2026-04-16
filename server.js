'use strict';

const express  = require('express');
const http     = require('http');
const os       = require('os');
const { WebSocketServer } = require('ws');
const mqtt     = require('mqtt');
const protobuf = require('protobufjs');
const path     = require('path');

function getLocalIp() {
  for (const iface of Object.values(os.networkInterfaces())) {
    for (const addr of iface) {
      if (addr.family === 'IPv4' && !addr.internal) return addr.address;
    }
  }
  return '127.0.0.1';
}

// ── Config ───────────────────────────────────────────────────────────────────
const PORT        = process.env.PORT     || 3000;
const DEFAULT_URL = process.env.MQTT_URL || 'mqtt://127.0.0.1:1883';
const PROTO_DIR   = path.resolve(__dirname, '../../app/roadeazy/infra/protobuf');

// ── Topic → Protobuf type map ─────────────────────────────────────────────────
// Each entry: { re: RegExp, type: 'pkg.MsgName' | null, cat: 'ipc'|'event'|'set' }
const SCHEMA_MAP = [
  // ── Vehicle events  (device → cloud) ────────────────────────────────────
  { re: /^vehicle\/gps\/event\/state\//,                  type: 'vehiclepackage.GpsEventState',                  cat: 'event' },
  { re: /^vehicle\/telemetry\/event\/state\//,            type: 'vehiclepackage.TelemetryEventState',             cat: 'event' },
  { re: /^vehicle\/system\/event\/health\//,              type: 'vehiclepackage.SystemEventHealth',               cat: 'event' },
  { re: /^vehicle\/system\/event\/powerstate\//,          type: 'vehiclepackage.SystemEventPowerState',           cat: 'event' },
  { re: /^vehicle\/system\/event\/log\//,                 type: 'vehiclepackage.EventLog',                        cat: 'event' },
  { re: /^vehicle\/safety\/event\/stream\//,              type: 'vehiclepackage.SafetyEventStream',               cat: 'event' },
  { re: /^vehicle\/firmware\/event\/state\//,             type: 'vehiclepackage.FirmwareEventStatus',             cat: 'event' },
  { re: /^vehicle\/pack\/event\/state\//,                 type: 'vehiclepackage.PackEventState',                  cat: 'event' },
  { re: /^vehicle\/video\/event\/qrcode\//,               type: 'vehiclepackage.QrCodeEventText',                 cat: 'event' },
  { re: /^vehicle\/system\/event\/disconnected\//,        type: null,                                             cat: 'event' },

  // ── Vehicle set commands  (cloud → device) ───────────────────────────────
  { re: /^vehicle\/gps\/set\/action\//,                   type: 'vehiclepackage.GpsSetAction',                    cat: 'set'   },
  { re: /^vehicle\/telemetry\/set\/engineHoursVirtual\//, type: 'vehiclepackage.TelemetrySetEngineHoursVirtual',  cat: 'set'   },
  { re: /^vehicle\/safety\/set\/live\//,                  type: 'vehiclepackage.SafetySetLive',                   cat: 'set'   },
  { re: /^vehicle\/audio\/set\/playtext\//,               type: 'vehiclepackage.AudioSetPlaytext',                cat: 'set'   },
  { re: /^vehicle\/system\/set\/fileput\//,               type: 'vehiclepackage.SystemSetFilePut',                cat: 'set'   },
  { re: /^vehicle\/firmware\/set\/swupgrade\//,           type: 'vehiclepackage.FirmwareSetVersion',              cat: 'set'   },
  { re: /^vehicle\/system\/set\/utcoffset\//,             type: 'vehiclepackage.SystemUtcOffsetAction',           cat: 'set'   },
  { re: /^vehicle\/system\/set\/cfg\//,                   type: 'vehiclepackage.SystemSetAllCfg',                 cat: 'set'   },
  { re: /^vehicle\/recording\/set\//,                     type: 'vehiclepackage.RecordingSetFilePut',             cat: 'set'   },
  { re: /^vehicle\/video\/set\//,                         type: 'vehiclepackage.VideoSetFilePut',                 cat: 'set'   },
  { re: /^vehicle\/power\/set\//,                         type: null,                                             cat: 'set'   },
  { re: /^vehicle\/accel\/set\//,                         type: null,                                             cat: 'set'   },
  { re: /^vehicle\/health\/set\//,                        type: null,                                             cat: 'set'   },
  { re: /^vehicle\/environment\/set\//,                   type: null,                                             cat: 'set'   },

  // ── IPC internal topics  (device-local bus) ──────────────────────────────
  { re: /^ipc\/mcu\/event\/health$/,                      type: 'ipcpackage.McuEventHealth',                     cat: 'ipc'   },
  { re: /^ipc\/mcu\/event\/gpioignition/,                 type: 'ipcpackage.McuEventButtonIgnition',             cat: 'ipc'   },
  { re: /^ipc\/modem\/event\/health$/,                    type: 'ipcpackage.ModemEventHealth',                   cat: 'ipc'   },
  { re: /^ipc\/gps\/event\/health$/,                      type: 'ipcpackage.GpsEventHealth',                     cat: 'ipc'   },
  { re: /^ipc\/safety\/event\/gps1Sec$/,                  type: 'ipcpackage.GpsEventHealth',                     cat: 'ipc'   },
  { re: /^ipc\/(inward|outward)\/event\/health$/,         type: 'ipcpackage.TriggerEventHealth',                 cat: 'ipc'   },
  { re: /^ipc\/auxcam\/event\/health$/,                   type: 'ipcpackage.AuxCaemraEventHealth',               cat: 'ipc'   },
  { re: /^ipc\/system\/event\/swupgrade$/,                type: 'ipcpackage.SystemSwUpgrade',                    cat: 'ipc'   },
  { re: /^ipc\/system\/event\/accelmove$/,                type: 'ipcpackage.AccelMove',                          cat: 'ipc'   },
  { re: /^ipc\/system\/event\/mqttpubconnect$/,           type: 'ipcpackage.MqttPublicConnect',                  cat: 'ipc'   },
  { re: /^ipc\/telemetry\/event\/ecuSpeed1Sec$/,          type: 'ipcpackage.EcuEventSpeed',                      cat: 'ipc'   },
  { re: /^ipc\/telemetry\/event\/ecuIgnition1Sec$/,       type: 'ipcpackage.EcuEventIgnition',                   cat: 'ipc'   },
  { re: /^ipc\/aplay\/event\/text$/,                      type: 'ipcpackage.AplayEventText',                     cat: 'ipc'   },
  { re: /^ipc\/stream\/event\/videocreate$/,              type: 'ipcpackage.StreamEventVideoCreate',             cat: 'ipc'   },
  { re: /^ipc\/ai\/event\/ttcms$/,                        type: 'ipcpackage.AiEventTtcMs',                       cat: 'ipc'   },
  { re: /^ipc\/ai\/event\/trigger$/,                      type: 'ipcpackage.AiEventTrigger',                     cat: 'ipc'   },
  { re: /^ipc\/accel\/event\/trigger$/,                   type: 'ipcpackage.AiEventTrigger',                     cat: 'ipc'   },
  { re: /^ipc\/rz41\/event\/camera$/,                     type: 'ipcpackage.Rz41EventCamera',                    cat: 'ipc'   },
  { re: /^ipc\/power\/event\//,                           type: null,                                             cat: 'ipc'   },
  { re: /^ipc\/button\/event\//,                          type: null,                                             cat: 'ipc'   },
  { re: /^ipc\/system\/event\/cfgchange$/,                type: null,                                             cat: 'ipc'   },
];

// PackEventState inner-pack type → protobuf type name
const PACK_TYPE_TO_PROTO = {
  1: 'vehiclepackage.GpsEventState',
  2: 'vehiclepackage.SystemEventHealth',
  3: 'vehiclepackage.TelemetryEventState',
  4: 'vehiclepackage.FirmwareEventStatus',
  5: 'vehiclepackage.SafetyEventStream',
  6: 'vehiclepackage.SystemEventPowerState',
  7: 'vehiclepackage.EventLog',
  8: 'vehiclepackage.QrCodeEventText',
};

// ── Schema lookup ─────────────────────────────────────────────────────────────
function findSchema(topic) {
  for (const e of SCHEMA_MAP) {
    if (e.re.test(topic)) return e;
  }
  return null;
}

// ── Protobuf decode ───────────────────────────────────────────────────────────
function decode(root, topic, payload) {
  const entry = findSchema(topic);
  const cat   = entry?.cat
    ?? (topic.startsWith('ipc/') ? 'ipc' : topic.includes('/set/') ? 'set' : 'event');

  if (!entry || !entry.type) {
    return { cat, schema: null, decoded: null, raw: payload.toString('hex') };
  }

  try {
    const MsgType = root.lookupType(entry.type);
    const msg     = MsgType.decode(payload);
    const obj     = MsgType.toObject(msg, {
      longs:    String,
      enums:    String,
      bytes:    'base64',
      defaults: false,
    });

    // Special: decode each inner packData in PackEventState
    if (entry.type === 'vehiclepackage.PackEventState' && Array.isArray(obj.packEvent)) {
      for (const pack of obj.packEvent) {
        const packTypeNum = typeof pack.packType === 'string'
          ? parseInt(pack.packType, 10)
          : pack.packType;
        const innerType = PACK_TYPE_TO_PROTO[packTypeNum];
        if (innerType && pack.packData) {
          try {
            const Inner  = root.lookupType(innerType);
            const rawBuf = Buffer.from(pack.packData, 'base64');
            pack._decoded = Inner.toObject(Inner.decode(rawBuf), {
              longs: String, enums: String, defaults: false,
            });
            pack._schema = innerType.split('.').pop();
          } catch { /* keep raw base64 */ }
        }
      }
    }

    return { cat, schema: entry.type.split('.').pop(), decoded: obj };
  } catch (err) {
    return {
      cat,
      schema:  entry.type.split('.').pop(),
      decoded: null,
      error:   err.message,
      raw:     payload.toString('hex').slice(0, 256),
    };
  }
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  // Load all proto schemas
  let root;
  try {
    root = await protobuf.load([
      path.join(PROTO_DIR, 'vehicle.proto'),
      path.join(PROTO_DIR, 'ipc.proto'),
      path.join(PROTO_DIR, 'vehicleAdmin.proto'),
      path.join(PROTO_DIR, 'vehicleCfg.proto'),
    ]);
    console.log('✓ Proto schemas loaded from', PROTO_DIR);
  } catch (err) {
    console.error('✗ Proto load failed:', err.message);
    console.error('  Expected dir:', PROTO_DIR);
    process.exit(1);
  }

  // HTTP + WebSocket
  const app    = express();
  const server = http.createServer(app);
  const wss    = new WebSocketServer({ server });

  app.use(express.static(path.join(__dirname, 'public')));

  // Serve schema map to the browser for display
  app.get('/api/schemas', (_req, res) => {
    res.json(SCHEMA_MAP.map(e => ({ pattern: e.re.source, type: e.type, cat: e.cat })));
  });

  let mqttClient = null;
  let currentUrl = DEFAULT_URL;
  let msgSeq     = 0;

  function broadcast(data) {
    const str = JSON.stringify(data);
    for (const ws of wss.clients) {
      if (ws.readyState === 1) ws.send(str);
    }
  }

  function connectMqtt(url) {
    if (mqttClient) { mqttClient.end(true); mqttClient = null; }
    currentUrl = url;

    const client = mqtt.connect(url, {
      clientId:        `rz-sniffer-${Date.now()}`,
      clean:           true,
      reconnectPeriod: 3000,
      connectTimeout:  10_000,
    });

    client.on('connect', () => {
      console.log('MQTT connected →', url);
      client.subscribe('#', { qos: 1 });
      broadcast({ type: 'status', connected: true, url });
    });

    client.on('reconnect', () =>
      broadcast({ type: 'status', connected: false, url, reconnecting: true }));
    client.on('offline', () =>
      broadcast({ type: 'status', connected: false, url }));
    client.on('error', err =>
      broadcast({ type: 'error', message: err.message }));

    client.on('message', (topic, payload) => {
      const result = decode(root, topic, payload);
      broadcast({
        type:  'msg',
        id:    ++msgSeq,
        ts:    Date.now(),
        topic,
        size:  payload.length,
        ...result,
      });
    });

    mqttClient = client;
  }

  // Handle browser commands over WebSocket
  wss.on('connection', ws => {
    // Send current connection state to new browser tab
    ws.send(JSON.stringify({
      type:      'status',
      connected: mqttClient?.connected ?? false,
      url:       currentUrl,
    }));

    ws.on('message', raw => {
      try {
        const cmd = JSON.parse(raw);
        if (cmd.action === 'connect')    connectMqtt(cmd.url || DEFAULT_URL);
        if (cmd.action === 'disconnect') {
          mqttClient?.end(true);
          broadcast({ type: 'status', connected: false, url: currentUrl });
        }
      } catch { /* ignore malformed JSON */ }
    });
  });

  connectMqtt(DEFAULT_URL);

  server.listen(PORT, '0.0.0.0', () => {
    const localIp = getLocalIp();
    console.log(`\n🔍 MQTT Sniffer  →  http://localhost:${PORT}`);
    console.log(`                    http://${localIp}:${PORT}`);
    console.log(`   Broker        →  ${DEFAULT_URL}`);
    console.log(`   Proto dir     →  ${PROTO_DIR}`);
    console.log(`\nPress Ctrl+C to stop.\n`);
  });
}

main().catch(err => { console.error(err); process.exit(1); });
