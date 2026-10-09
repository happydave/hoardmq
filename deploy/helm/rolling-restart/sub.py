# sub.py — the chart runbook's durable subscriber, writing one JSON line per
# connection event. It resubscribes only when the broker reports no session,
# so a reconnect that keeps the session is measured, not papered over.
import json, os, time
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

TOTAL, DEADLINE, seen = int(os.environ.get("TOTAL", "600")), int(os.environ.get("DEADLINE", "420")), set()


def emit(**kv):
    print(json.dumps(kv), flush=True)


def peer(c):
    try:
        return c.socket().getpeername()[0]
    except Exception:
        return None


def on_connect(c, u, flags, rc, props=None):
    emit(ev="connect", t=time.time(), code=getattr(rc, "value", rc), session_present=flags.session_present, peer=peer(c))
    if not flags.session_present:
        c.subscribe("t/x", qos=1)


def on_disconnect(c, u, flags, rc, props=None):
    ref = getattr(props, "ServerReference", None) if props is not None else None
    emit(ev="disconnect", t=time.time(), code=getattr(rc, "value", rc), server_reference=ref)


props = Properties(PacketTypes.CONNECT)
props.SessionExpiryInterval = 3600
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="sub", protocol=mqtt.MQTTv5)
c.on_connect, c.on_disconnect = on_connect, on_disconnect
c.on_message = lambda cl, u, m: seen.add(int(m.payload))
c.connect("hoardmq", 1883, keepalive=30, clean_start=False, properties=props)
c.loop_start()
start = time.time()
while len(seen) < TOTAL and time.time() - start < DEADLINE:
    time.sleep(1)
missing = sorted(set(range(1, TOTAL + 1)) - seen)
emit(ev="summary", received=len(seen), missing=missing)
print("received", len(seen), "missing", missing, flush=True)
