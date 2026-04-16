#!/usr/bin/env python3
"""
MQTT Sniffer — Roadeazy
Real-time MQTT message sniffer with protobuf decoding.

Usage:
  python3 server.py [MQTT_URL] [PORT]

Defaults:
  MQTT_URL = mqtt://127.0.0.1:1883
  PORT     = 3000
"""

import asyncio
import json
import os
import socket
import sys
import threading
import time
import struct
import http.server
import mimetypes
from pathlib import Path
from urllib.parse import urlparse

# ── Third-party ───────────────────────────────────────────────────────────────
import paho.mqtt.client as mqtt_client
import websockets
import websockets.server

# ── Generated protobuf modules ────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent / 'proto_gen'))
import ipc_pb2
import vehicle_pb2
import vehicleAdmin_pb2
import vehicleCfg_pb2

# ── Config ────────────────────────────────────────────────────────────────────
MQTT_URL     = os.environ.get('MQTT_URL', 'mqtt://127.0.0.1:1883')
PORT         = int(os.environ.get('PORT', 3000))
WS_PORT      = PORT + 1       # WebSocket on PORT+1, HTTP on PORT
PUBLIC_DIR   = Path(__file__).parent / 'public'

# ── Topic → Protobuf message class map ───────────────────────────────────────
# Each entry: (regex_prefix, MessageClass | None, category)
import re as _re

SCHEMA_MAP = [
    # Vehicle events  (device → cloud)
    (_re.compile(r'^vehicle/gps/event/state/'),                  vehicle_pb2.GpsEventState,                  'event'),
    (_re.compile(r'^vehicle/telemetry/event/state/'),            vehicle_pb2.TelemetryEventState,            'event'),
    (_re.compile(r'^vehicle/system/event/health/'),              vehicle_pb2.SystemEventHealth,              'event'),
    (_re.compile(r'^vehicle/system/event/powerstate/'),          vehicle_pb2.SystemEventPowerState,          'event'),
    (_re.compile(r'^vehicle/system/event/log/'),                 vehicleAdmin_pb2.EventLog,                  'event'),
    (_re.compile(r'^vehicle/safety/event/stream/'),              vehicle_pb2.SafetyEventStream,              'event'),
    (_re.compile(r'^vehicle/firmware/event/state/'),             vehicleAdmin_pb2.FirmwareEventStatus,       'event'),
    (_re.compile(r'^vehicle/pack/event/state/'),                 vehicleAdmin_pb2.PackEventState,            'event'),
    (_re.compile(r'^vehicle/video/event/qrcode/'),               vehicle_pb2.QrCodeEventText,                'event'),
    (_re.compile(r'^vehicle/system/event/disconnected/'),        None,                                       'event'),
    # Vehicle set commands  (cloud → device)
    (_re.compile(r'^vehicle/gps/set/action/'),                   vehicleAdmin_pb2.GpsSetAction,              'set'),
    (_re.compile(r'^vehicle/telemetry/set/engineHoursVirtual/'), vehicleAdmin_pb2.TelemetrySetEngineHoursVirtual, 'set'),
    (_re.compile(r'^vehicle/safety/set/live/'),                  vehicleAdmin_pb2.SafetySetLive,             'set'),
    (_re.compile(r'^vehicle/audio/set/playtext/'),               vehicleAdmin_pb2.AudioSetPlaytext,          'set'),
    (_re.compile(r'^vehicle/system/set/fileput/'),               vehicleAdmin_pb2.SystemSetFilePut,          'set'),
    (_re.compile(r'^vehicle/firmware/set/swupgrade/'),           vehicleAdmin_pb2.FirmwareSetVersion,        'set'),
    (_re.compile(r'^vehicle/system/set/utcoffset/'),             vehicleAdmin_pb2.SystemUtcOffsetAction,     'set'),
    (_re.compile(r'^vehicle/system/set/cfg/'),                   vehicleCfg_pb2.SystemSetAllCfg,             'set'),
    (_re.compile(r'^vehicle/recording/set/'),                    vehicleAdmin_pb2.RecordingSetFilePut,       'set'),
    (_re.compile(r'^vehicle/video/set/'),                        vehicleAdmin_pb2.VideoSetFilePut,           'set'),
    (_re.compile(r'^vehicle/power/set/'),                        None,                                       'set'),
    (_re.compile(r'^vehicle/accel/set/'),                        None,                                       'set'),
    (_re.compile(r'^vehicle/health/set/'),                       None,                                       'set'),
    (_re.compile(r'^vehicle/environment/set/'),                  None,                                       'set'),
    # IPC internal topics
    (_re.compile(r'^ipc/mcu/event/health$'),                     ipc_pb2.McuEventHealth,                     'ipc'),
    (_re.compile(r'^ipc/mcu/event/gpioignition'),                ipc_pb2.McuEventButtonIgnition,             'ipc'),
    (_re.compile(r'^ipc/modem/event/health$'),                   ipc_pb2.ModemEventHealth,                   'ipc'),
    (_re.compile(r'^ipc/gps/event/health$'),                     ipc_pb2.GpsEventHealth,                     'ipc'),
    (_re.compile(r'^ipc/safety/event/gps1Sec$'),                 ipc_pb2.GpsEventHealth,                     'ipc'),
    (_re.compile(r'^ipc/(inward|outward)/event/health$'),        ipc_pb2.TriggerEventHealth,                 'ipc'),
    (_re.compile(r'^ipc/auxcam/event/health$'),                  ipc_pb2.AuxCaemraEventHealth,               'ipc'),
    (_re.compile(r'^ipc/system/event/swupgrade$'),               ipc_pb2.SystemSwUpgrade,                    'ipc'),
    (_re.compile(r'^ipc/system/event/accelmove$'),               ipc_pb2.AccelMove,                          'ipc'),
    (_re.compile(r'^ipc/system/event/mqttpubconnect$'),          ipc_pb2.MqttPublicConnect,                  'ipc'),
    (_re.compile(r'^ipc/telemetry/event/ecuSpeed1Sec$'),         ipc_pb2.EcuEventSpeed,                      'ipc'),
    (_re.compile(r'^ipc/telemetry/event/ecuIgnition1Sec$'),      ipc_pb2.EcuEventIgnition,                   'ipc'),
    (_re.compile(r'^ipc/aplay/event/text$'),                     ipc_pb2.AplayEventText,                     'ipc'),
    (_re.compile(r'^ipc/stream/event/videocreate$'),             ipc_pb2.StreamEventVideoCreate,             'ipc'),
    (_re.compile(r'^ipc/ai/event/ttcms$'),                       ipc_pb2.AiEventTtcMs,                       'ipc'),
    (_re.compile(r'^ipc/ai/event/trigger$'),                     ipc_pb2.AiEventTrigger,                     'ipc'),
    (_re.compile(r'^ipc/accel/event/trigger$'),                  ipc_pb2.AiEventTrigger,                     'ipc'),
    (_re.compile(r'^ipc/rz41/event/camera$'),                    ipc_pb2.Rz41EventCamera,                    'ipc'),
    (_re.compile(r'^ipc/power/event/'),                          None,                                       'ipc'),
    (_re.compile(r'^ipc/button/event/'),                         None,                                       'ipc'),
    (_re.compile(r'^ipc/system/event/cfgchange$'),               None,                                       'ipc'),
]

# PackType enum → message class (for inner PackEventState decoding)
PACK_TYPE_MAP = {
    1: vehicle_pb2.GpsEventState,
    2: vehicle_pb2.SystemEventHealth,
    3: vehicle_pb2.TelemetryEventState,
    4: vehicleAdmin_pb2.FirmwareEventStatus,
    5: vehicle_pb2.SafetyEventStream,
    6: vehicle_pb2.SystemEventPowerState,
    7: vehicleAdmin_pb2.EventLog,
    8: vehicle_pb2.QrCodeEventText,
}

# ── Protobuf → dict helper ────────────────────────────────────────────────────
def proto_to_dict(msg):
    """Convert a protobuf message to a JSON-serialisable dict."""
    from google.protobuf import descriptor
    from google.protobuf.descriptor import FieldDescriptor as FD
    result = {}
    for field, value in msg.ListFields():
        if field.type == FD.TYPE_MESSAGE:
            if field.label == FD.LABEL_REPEATED:
                result[field.name] = [proto_to_dict(v) for v in value]
            else:
                result[field.name] = proto_to_dict(value)
        elif field.type == FD.TYPE_BYTES:
            result[field.name] = value.hex()
        elif field.type == FD.TYPE_ENUM:
            enum_type = field.enum_type
            enum_val  = enum_type.values_by_number.get(value)
            result[field.name] = enum_val.name if enum_val else value
        else:
            result[field.name] = value
    return result


# ── Decode one MQTT message ───────────────────────────────────────────────────
def decode(topic: str, payload: bytes) -> dict:
    # Find matching schema entry
    schema_entry = None
    for pattern, cls, cat in SCHEMA_MAP:
        if pattern.search(topic):
            schema_entry = (cls, cat)
            break

    if schema_entry is None:
        cat = ('ipc' if topic.startswith('ipc/')
               else 'set' if '/set/' in topic
               else 'event')
        return {'cat': cat, 'schema': None, 'decoded': None, 'raw': payload.hex()}

    cls, cat = schema_entry

    if cls is None:
        return {'cat': cat, 'schema': None, 'decoded': None, 'raw': payload.hex()}

    schema_name = cls.DESCRIPTOR.name

    try:
        msg = cls()
        msg.ParseFromString(payload)
        decoded = proto_to_dict(msg)

        # Extra: decode inner packs for PackEventState
        if cls is vehicleAdmin_pb2.PackEventState:
            for pack in msg.packEvent:
                inner_cls = PACK_TYPE_MAP.get(pack.packType)
                if inner_cls and pack.packData:
                    try:
                        inner = inner_cls()
                        inner.ParseFromString(pack.packData)
                        pack_idx = list(msg.packEvent).index(pack)
                        decoded['packEvent'][pack_idx]['_decoded'] = proto_to_dict(inner)
                        decoded['packEvent'][pack_idx]['_schema']  = inner_cls.DESCRIPTOR.name
                    except Exception:
                        pass

        return {'cat': cat, 'schema': schema_name, 'decoded': decoded}

    except Exception as e:
        return {
            'cat':     cat,
            'schema':  schema_name,
            'decoded': None,
            'error':   str(e),
            'raw':     payload.hex()[:256],
        }


# ── Shared state ──────────────────────────────────────────────────────────────
connected_ws:   set  = set()
mqtt_connected: bool = False
mqtt_url_current: str = MQTT_URL
msg_seq: int = 0
loop: asyncio.AbstractEventLoop = None


def broadcast_sync(data: dict):
    """Thread-safe broadcast to all WebSocket clients."""
    if not loop or not connected_ws:
        return
    msg = json.dumps(data)
    asyncio.run_coroutine_threadsafe(_broadcast(msg), loop)


async def _broadcast(msg: str):
    dead = set()
    for ws in list(connected_ws):
        try:
            await ws.send(msg)
        except Exception:
            dead.add(ws)
    connected_ws.difference_update(dead)


# ── MQTT client ───────────────────────────────────────────────────────────────
mqtt: mqtt_client.Client = None


def mqtt_connect(url: str):
    global mqtt, mqtt_connected, mqtt_url_current, msg_seq

    if mqtt:
        try:
            mqtt.disconnect()
            mqtt.loop_stop()
        except Exception:
            pass

    mqtt_url_current = url
    parsed = urlparse(url)
    host   = parsed.hostname or '127.0.0.1'
    port   = parsed.port or 1883

    client = mqtt_client.Client(
        callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
        client_id=f'rz-sniffer-{int(time.time())}',
        clean_session=True,
        protocol=mqtt_client.MQTTv311,
    )

    def on_connect(c, _ud, _flags, reason_code, _props):
        global mqtt_connected
        if reason_code.is_failure:
            print(f'MQTT connect failed: {reason_code}')
            broadcast_sync({'type': 'status', 'connected': False, 'url': url})
        else:
            mqtt_connected = True
            c.subscribe('#', qos=1)
            print(f'MQTT connected → {url}')
            broadcast_sync({'type': 'status', 'connected': True, 'url': url})

    def on_disconnect(c, _ud, _flags, reason_code, _props):
        global mqtt_connected
        mqtt_connected = False
        reconnecting = reason_code != 0
        print(f'MQTT disconnected ({reason_code}){" — reconnecting" if reconnecting else ""}')
        broadcast_sync({'type': 'status', 'connected': False, 'url': url,
                        'reconnecting': reconnecting})

    def on_message(c, _ud, message):
        global msg_seq
        msg_seq += 1
        topic   = message.topic
        payload = bytes(message.payload)
        result  = decode(topic, payload)
        broadcast_sync({
            'type':  'msg',
            'id':    msg_seq,
            'ts':    int(time.time() * 1000),
            'topic': topic,
            'size':  len(payload),
            **result,
        })

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message
    client.reconnect_delay_set(min_delay=2, max_delay=10)

    try:
        client.connect(host, port, keepalive=60)
        client.loop_start()
    except Exception as e:
        print(f'MQTT connect error: {e}')
        broadcast_sync({'type': 'error', 'message': str(e)})

    mqtt = client


# ── WebSocket handler ─────────────────────────────────────────────────────────
async def ws_handler(ws):
    connected_ws.add(ws)
    # Send current state immediately
    await ws.send(json.dumps({
        'type':      'status',
        'connected': mqtt_connected,
        'url':       mqtt_url_current,
    }))

    try:
        async for raw in ws:
            try:
                cmd = json.loads(raw)
                if cmd.get('action') == 'connect':
                    url = cmd.get('url', MQTT_URL)
                    threading.Thread(target=mqtt_connect, args=(url,), daemon=True).start()
                elif cmd.get('action') == 'disconnect':
                    if mqtt:
                        mqtt.disconnect()
                        mqtt.loop_stop()
                    broadcast_sync({'type': 'status', 'connected': False, 'url': mqtt_url_current})
            except json.JSONDecodeError:
                pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_ws.discard(ws)


# ── HTTP server for static files ──────────────────────────────────────────────
class StaticHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(PUBLIC_DIR), **kwargs)

    def log_message(self, fmt, *args):
        pass  # silence access log


class ReuseAddrHTTPServer(http.server.HTTPServer):
    allow_reuse_address = True


def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'


def run_http(port: int):
    server = ReuseAddrHTTPServer(('0.0.0.0', port), StaticHandler)
    server.serve_forever()


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    global loop
    loop = asyncio.get_event_loop()

    local_ip = get_local_ip()
    print(f'\n🔍 MQTT Sniffer  →  Roadeazy')
    print(f'   Broker        →  {MQTT_URL}')
    print(f'   Web UI        →  http://localhost:{PORT}')
    print(f'                    http://{local_ip}:{PORT}')
    print(f'   WebSocket     →  ws://localhost:{WS_PORT}')
    print(f'                    ws://{local_ip}:{WS_PORT}')
    print(f'\nPress Ctrl+C to stop.\n')

    # Start HTTP server in background thread
    http_thread = threading.Thread(target=run_http, args=(PORT,), daemon=True)
    http_thread.start()

    # Connect to MQTT in background thread
    threading.Thread(target=mqtt_connect, args=(MQTT_URL,), daemon=True).start()

    # Run WebSocket server
    async with websockets.server.serve(ws_handler, '0.0.0.0', WS_PORT):
        await asyncio.Future()  # run forever


if __name__ == '__main__':
    if len(sys.argv) > 1:
        MQTT_URL = sys.argv[1]
    if len(sys.argv) > 2:
        PORT   = int(sys.argv[2])
        WS_PORT = PORT + 1

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\nStopped.')
