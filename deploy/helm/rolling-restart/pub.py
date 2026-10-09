# pub.py — the chart runbook's publisher, writing one JSON line per event so
# each refusal can be attributed afterwards. It publishes only while
# connected and reads the PUBACK reason code from on_publish: a PUBACK
# carrying 0x83 satisfies wait_for_publish(), which reports only Paho's own
# local result.
import json, os, threading, time
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

TOTAL = int(os.environ.get("TOTAL", "600"))
connected, cv, codes = threading.Event(), threading.Condition(), {}
ok, refused = set(), {}


def emit(**kv):
    print(json.dumps(kv), flush=True)


def peer(c):
    try:
        return c.socket().getpeername()[0]
    except Exception:
        return None


def on_connect(c, u, flags, rc, props=None):
    emit(ev="connect", t=time.time(), code=getattr(rc, "value", rc), session_present=flags.session_present, peer=peer(c))
    connected.set()


def on_disconnect(c, u, flags, rc, props=None):
    ref = getattr(props, "ServerReference", None) if props is not None else None
    emit(ev="disconnect", t=time.time(), code=getattr(rc, "value", rc), server_reference=ref)
    connected.clear()


def on_publish(c, u, mid, rc=None, props=None):
    with cv:
        codes[mid] = getattr(rc, "value", rc)
        cv.notify_all()


props = Properties(PacketTypes.CONNECT)
props.SessionExpiryInterval = 3600
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="pub", protocol=mqtt.MQTTv5)
c.on_connect, c.on_disconnect, c.on_publish = on_connect, on_disconnect, on_publish
c.connect("hoardmq", 1883, keepalive=30, clean_start=False, properties=props)
c.loop_start()
time.sleep(5)
for i in range(1, TOTAL + 1):
    while not connected.wait(timeout=180):
        pass
    sent = time.time()
    info = c.publish("t/x", str(i), qos=1)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        emit(ev="not-sent", i=i, t=sent, rc=info.rc)
        continue
    info.wait_for_publish(timeout=180)
    with cv:
        deadline = time.time() + 10
        while info.mid not in codes and time.time() < deadline:
            cv.wait(timeout=1)
        code = codes.pop(info.mid, None)
    emit(ev="pub", i=i, t=sent, answered=time.time(), code=code)
    (ok.add(i) if code in (0, 16) else refused.setdefault(i, code))
    time.sleep(0.1)
emit(ev="summary", acked=len(ok), refused=len(refused))
print("acked", len(ok), "refused", len(refused), sorted(refused.items())[:20], flush=True)
