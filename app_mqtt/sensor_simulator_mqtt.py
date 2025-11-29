#!/usr/bin/env python3
"""
sensor_simulator.py

Simula sensores de estacionamento e envia leituras para:
 - HTTP: POST /readings (default antigo)
 - MQTT: publica em parking/{slot}/reading  (default atual)

Uso:
  python sensor_simulator.py --transport mqtt
  python sensor_simulator.py --transport http --api http://localhost:8000/readings
  python sensor_simulator.py --slots 6 --interval 0.5 --mode sequential
"""

import time
import random
import argparse
import json
from datetime import datetime
from threading import Event

# HTTP client
try:
    import requests
except Exception:
    requests = None

# MQTT client
try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

API_URL_DEFAULT = "http://localhost:8000/readings"
MQTT_BROKER_DEFAULT = "localhost"
MQTT_PORT_DEFAULT = 1883
MQTT_QOS_DEFAULT = 1
MQTT_TOPIC_TEMPLATE = "parking/{slot}/reading"


def iso_now():
    return datetime.utcnow().isoformat() + "Z"


class SlotSimulator:
    def __init__(self, slot_id, base_empty=120.0, base_occupied=20.0, jitter=5.0):
        """base_empty/base_occupied em cm; jitter é ruído adicionado"""
        self.slot_id = slot_id
        self.base_empty = base_empty
        self.base_occupied = base_occupied
        self.jitter = jitter
        self.occupied = random.random() < 0.3

    def sample_distance(self):
        if self.occupied:
            d = random.gauss(self.base_occupied, self.jitter)
            if random.random() < 0.02:
                d += random.uniform(5, 30)
        else:
            d = random.gauss(self.base_empty, self.jitter * 3)
            if random.random() < 0.03:
                d = random.gauss(self.base_occupied + 5, self.jitter)
        return max(1.0, float(d))

    def maybe_flip_state(self, flip_prob=0.01):
        if random.random() < flip_prob:
            self.occupied = not self.occupied


def send_http_reading(api_url, slot_id, distance_cm, timeout=3):
    if requests is None:
        return None, "requests library not installed"
    payload = {
        "device_id": slot_id,
        "timestamp": iso_now(),
        "distance_cm": float(distance_cm)
    }
    try:
        r = requests.post(api_url, json=payload, timeout=timeout)
        return r.status_code, r.text
    except requests.RequestException as e:
        return None, str(e)


class MQTTProducer:
    def __init__(self, host="localhost", port=1883, client_id=None, username=None, password=None, keepalive=60):
        if mqtt is None:
            raise RuntimeError("paho-mqtt not installed. pip install paho-mqtt")
        self.host = host
        self.port = port
        self.client = mqtt.Client(client_id=client_id)
        if username:
            self.client.username_pw_set(username, password)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.connected = False
        self._stop = Event()
        # enable auto-reconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=60)

    def _on_connect(self, client, userdata, flags, rc):
        self.connected = True
        print(f"[MQTT] Connected to {self.host}:{self.port} (rc={rc})")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        print(f"[MQTT] Disconnected (rc={rc})")

    def start(self):
        try:
            self.client.connect(self.host, self.port, keepalive=60)
            self.client.loop_start()
        except Exception as e:
            print("[MQTT] Connect error:", e)

    def stop(self):
        try:
            self._stop.set()
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass

    def publish(self, topic, payload, qos=1):
        if not self.connected:
            # attempt a non-blocking reconnect
            try:
                self.client.reconnect()
            except Exception:
                pass
            return False, "not connected"
        try:
            info = self.client.publish(topic, payload, qos=qos)
            info.wait_for_publish()
            return True, f"mid={info.mid}"
        except Exception as e:
            return False, str(e)


def run_simulator(api_url=None, transport="mqtt", broker="localhost", port=1883, slots=10, interval=1.0, mode="random", qos=1):
    sims = {f"slot-{i+1}": SlotSimulator(f"slot-{i+1}") for i in range(slots)}
    slot_keys = list(sims.keys())

    print(f"Simulator: transport={transport} api={api_url} broker={broker}:{port} slots={slots} interval={interval}s mode={mode}\nPress Ctrl+C to stop\n")

    mqtt_prod = None
    if transport == "mqtt":
        if mqtt is None:
            raise RuntimeError("paho-mqtt not installed. pip install paho-mqtt")
        mqtt_prod = MQTTProducer(host=broker, port=port)
        mqtt_prod.start()
        # small wait for connect
        time.sleep(0.5)

    idx = 0
    try:
        while True:
            if mode == "random":
                slot = random.choice(slot_keys)
            else:
                slot = slot_keys[idx % len(slot_keys)]
                idx += 1

            sim = sims[slot]
            sim.maybe_flip_state(flip_prob=0.02)
            dist = sim.sample_distance()

            now = datetime.now().strftime("%H:%M:%S")
            if transport == "http":
                status_code, resp = send_http_reading(api_url, slot, dist)
                if status_code is None:
                    print(f"[{now}] -> {slot} distance={dist:.1f}cm  -> ERROR: {resp}")
                else:
                    print(f"[{now}] -> {slot} distance={dist:.1f}cm  -> {status_code} {resp}")
            else:  # mqtt
                topic = MQTT_TOPIC_TEMPLATE.format(slot=slot)
                payload = json.dumps({
                    "device_id": slot,
                    "timestamp": iso_now(),
                    "distance_cm": float(dist)
                })
                ok, info = mqtt_prod.publish(topic, payload, qos=qos)
                if ok:
                    print(f"[{now}] PUB {topic} -> distance={dist:.1f}cm  -> OK {info}")
                else:
                    print(f"[{now}] PUB {topic} -> distance={dist:.1f}cm  -> ERROR {info}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nSimulator stopped by user.")
    finally:
        if mqtt_prod:
            mqtt_prod.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--transport", choices=["mqtt", "http"], default="mqtt", help="Transport: mqtt or http (default mqtt)")
    p.add_argument("--api", default=API_URL_DEFAULT, help="URL do endpoint /readings (for http transport)")
    p.add_argument("--broker", default=MQTT_BROKER_DEFAULT, help="MQTT broker host")
    p.add_argument("--port", type=int, default=MQTT_PORT_DEFAULT, help="MQTT broker port")
    p.add_argument("--slots", type=int, default=10, help="Número de vagas/sensores")
    p.add_argument("--interval", type=float, default=1.0, help="Intervalo entre envios (segundos)")
    p.add_argument("--mode", choices=["random", "sequential"], default="random", help="Escolha envio random ou sequencial")
    p.add_argument("--qos", type=int, choices=[0,1,2], default=MQTT_QOS_DEFAULT, help="QoS para MQTT")
    args = p.parse_args()

    # use global template and defaults
    MQTT_TOPIC_TEMPLATE = MQTT_TOPIC_TEMPLATE  # keep default unless user changes code
    run_simulator(api_url=args.api, transport=args.transport, broker=args.broker, port=args.port, slots=args.slots, interval=args.interval, mode=args.mode, qos=args.qos)